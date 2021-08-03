import htcondor
import os
import logging

from typing import List, Dict, Any

from . import translate

log = logging.getLogger(__name__)


class job:
    """ Submits and holds a single job and all relevant information

    Currently planning to keep single job per clusterID,
    since group submittion would significantly complicate resubmitting.
    HTCondor does not seem to allow for re-submittion of single ProcId,
    so one would have to first connect specific arguments to specific ProcIds
    and then resubmit individual jobs anyway.
    """

    # Define the job by its name and add a schedd
    def __init__(self, name: str, schedd: htcondor.Schedd) -> None:

        # first, define schedd
        self.schedd = schedd

        self.name = name        # name of the job for easy identification

        # since we will be resubmitting, job IDs are kept as a list
        self.clusterIDs: List[str] = []

        # add a decoration to the job to hold dependencies
        self.dependencies: List["job"] = []

    # set up a simple job with only executable and a path to log files
    def set_simple(self, exe: str, logPath: str):

        # htcondor defines job as a dict
        cfg = {
            "executable":   exe,
            "output":       logPath + "/" + self.name + "/$(ClusterId).out",
            "error":        logPath + "/" + self.name + "/$(ClusterId).err",
            "log":          logPath + "/" + self.name + "/$(ClusterId).log"
        }
        self.config = cfg

        # create the directory for the log
        logDir = os.getcwd() + "/" + logPath + "/" + self.name + "/"
        if not os.path.exists(logDir):
            os.makedirs(logDir)

        # setup flags:
        self.reset()

    # define dict containing all relevant job information
    def save(self) -> Dict[str, Any]:
        # first rewrite dependencies using names
        depNames = [j.name for j in self.dependencies]
        jobDict = {
            "clusterIDs": self.clusterIDs,
            "config": self.config,
            "depNames": depNames,
            "done": "false"
        }
        # to test if job is done takes long time
        # because log file needs to be checked
        # so its best to save this status
        if self.done:
            jobDict["done"] = "true"
        return jobDict

    # define job from dictionary created using the save function
    # TODO: define proper "jobDict checker"
    def load(self, jobDict: Dict[str, Any]) -> None:
        if "clusterIDs" not in jobDict.keys() and "config" not in jobDict.keys():
            log.error("Job dictionary in a wrong form")
            raise SystemError

        # the htcondor version of the configuration
        self.config = jobDict["config"]

        # setup flags:
        self.reset()
        # tmp backwards compatibility
        if "done" in jobDict and jobDict["done"] == "true":
            log.debug("Job is already done")
            self.done = True

        # set cluster IDs
        self.clusterIDs = jobDict["clusterIDs"]
        # if not empty, the job has been already submitted at least once
        if len(self.clusterIDs):
            self.htjob = htcondor.Submit(self.config)
            self.clusterID = self.clusterIDs[-1]
            self.logFile = self.config["log"].replace("$(ClusterId)", str(self.clusterIDs[-1]))
            self.submitted = True

    # reset job flags
    def reset(self) -> None:
        self.submitted = False
        self.skipped = False
        self.failed = False
        self.done = False

    # extend dependen
    # old def add_job_dependency(self, dps: List["job"]) -> None:
    def add_job_dependency(self, *args: "job") -> None:
        self.dependencies.extend(list(args))

    # submit the job
    # TODO: raise error if problem
    def submit(self, force: bool = False) -> None:
        # force: for cases when the job status was checked
        # e.g. when retrying
        # this is can save a lot of time because
        # failed job required the log file to be read.
        # Should not be used by users, only internally.

        # first check if job was not submitted before:
        if self.clusterIDs != []:
            status = self.get_status()
            if status == 12 or status < 0 or status == 10:
                log.info("Job %s failed and will be resubmitted.", self.name)
            else:
                log.info("The job is %s, not submitting", translate.statusMessage[status])
                return
        else:
            # the htcondor version of the configuration
            self.htjob = htcondor.Submit(self.config)

        with self.schedd.transaction() as txn:
            self.clusterID = self.htjob.queue(txn)
            self.clusterIDs.append(self.clusterID)
            log.info("Submitting job %s with id %s", self.name, self.clusterID)
            log.debug(self.config)
            self.logFile = self.config["log"].replace("$(ClusterId)", str(self.clusterIDs[-1]))

        # reset job properties
        self.reset()
        self.submitted = True

    # simple implementations of release, remove, ...
    def release(self) -> bool:
        if self.clusterIDs == []:
            return False
        self.schedd.act(htcondor.JobAction.Release, "ClusterId == "+str(self.clusterID))
        log.info("Releasing job %s with id %s", self.name, self.clusterID)
        return True

    def remove(self) -> bool:
        if self.clusterIDs == []:
            return False
        self.schedd.act(htcondor.JobAction.Remove, "ClusterId == "+str(self.clusterID))
        log.info("Removing job %s with id %s", self.name, self.clusterID)
        return True

    # get information about the job
    def get_info(self) -> Dict[str, Any]:
        # check if job has an ID
        if self.clusterIDs == []:
            log.error("Trying to list info for a job which was not submitted")
            raise SystemError

        constr = "ClusterId == " + str(self.clusterID)
        # get all job info of running job
        ads = self.schedd.query(
            constraint=constr
        )

        # if the job finished, query will be empty and we have to use history
        # because condor is stupid, it returns and iterator (?),
        # so just returning first element
        # TODO: add more to projection
        if ads == []:
            for ad in self.schedd.history(
                requirements=constr,
                projection=["JobStatus"]
            ):
                return ad

        # check if only one job was returned
        if len(ads) != 1:
            # empty job is probably job finished a long ago
            # return specific code -999 and let get_status function
            # sort the rest from the log files
            if ads == []:
                return {"JobStatus": -999}
            else:
                log.error("HTCondor returned more than one jobs for given ID, this should not happen!")
                log.error("Job %s with id %u", self.name, self.clusterID)
                print(ads)
                raise SystemError

        return ads[0]  # we take only single job, so return onl the first eleement

    # get condor status of the job
    def get_condor_status(self) -> int:
        return self.get_info()["JobStatus"]

    # get status of the job, as defined in translate.py
    def get_status(self) -> int:

        # First check if the job is skipped or not even submitted
        if self.skipped:
            return 8
        elif self.done:
            return 4
        elif self.clusterIDs == []:  # job was not even submitted
            return 9
        elif not os.path.isfile(self.logFile):
            return 10

        cndr_status = self.get_condor_status()
        # If job complete, check if with error:
        if cndr_status == 4 or cndr_status == -999:
            with open(self.logFile) as fl:
                search = fl.read()
                if "Job terminated" in search:
                    searchSplit = search.split("\n")
                    for line in searchSplit:
                        line = line.rstrip()  # remove '\n' at end of line
                        if "Normal termination (return value" in line:
                            status = int(line.split("value")[1].strip()[:-1])

                            if status == 0:
                                self.done = True
                                return 4  # success

                            log.debug(f"Job failed {status}")
                            self.failed = True
                            return -status
                    return 11  # no "Normal termination for Job terminated"
                elif "Job was aborted by the user" in search:
                    # I think this is the same as 3 but need to check
                    return 12
                else:
                    log.error("Uknown output of job %s!", self.name)
        return cndr_status

    def set_custom(self, dict: Dict[str, str]) -> None:
        for key, item in dict.items():
            self.config[key] = item

    def set_time(self, runTime: int) -> None:
        self.config["+MaxRuntime"] = str(runTime)
        # TODO: this does not work on UI
        # self.config["+RequestRuntime"] = str(runTime)

    def set_arguments(self, args: str) -> None:
        self.config["arguments"] = args
