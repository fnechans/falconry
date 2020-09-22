import htcondor  # for submitting jobs, querying HTCondor daemons, etc.
import os
import logging

from typing import List, Dict,Any

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
        log.debug(cfg)

        # create the directory for the log
        logDir = os.getcwd() + "/" + logPath + "/" + name + "/"
        if not os.path.exists(logDir):
            os.makedirs(logDir)

        # the htcondor version of the configuration
        self.htjob = htcondor.Submit(self.config)

        # since we will be resubmitting, job IDs are kept as a list
        self.clusterIDs: List[str] = []

        # add a decoration to the job to hold dependencies
        self.dependencies: List["job"] = []

        # setup flags:
        self.reset()

    # define dict containing all relevant job information
    def save(self) -> Dict[str,Any]:
        jobDict = {
            "clusterIDs": self.clusterIDs,
            "dependencies": self.dependencies,
            "config": self.config
        }
        return jobDict

    # define job from dictionary created using the save function
    # TODO: define proper "jobDict checker"
    def load(self, jobDict: Dict[str,Any]) -> None:
        if "clusterIDs" not in jobDict.keys() and "config" not in jobDict.keys():
            log.error("Job dictionary in a wrong form")
            raise SystemError

        # the htcondor version of the configuration
        self.config = jobDict["config"]
        self.htjob = htcondor.Submit(self.config)]

        # set cluster IDs and dependencies
        self.clusterIDs = jobDict["clusterIDs"]
        self.clusterID = self.clusterIDs[-1]
        self.dependencies = jobDict["dependencies"]

        # setup flags:
        self.reset()

    # reset job flags
    def reset(self) -> None:
        self.submitted = False
        self.skipped = False
        self.failed = False
        self.done = False

    # extend dependencies
    def add_job_dependency(self, dps: List["job"]) -> None:
        self.dependencies.extend(dps)

    # submit the job
    # TODO: raise error if problem
    def submit(self) -> None:
        # first check if job was not submitted before:
        if self.clusterIDs != []:
            status = self.get_condor_status()
            if status == 12 or status < 0 or status == 10:
                log.info("Job %s failed and will be resubmitted.", self.name)
            else:
                log.info("The job is %s, not submitting", translate.status[status])
                return

        with self.schedd.transaction() as txn:
            self.clusterID = self.htjob.queue(txn)
            self.clusterIDs.append(self.clusterID)
            log.debug("Submitting job with id %s", self.clusterID)
            self.logFile = self.config["log"].replace("$(ClusterId)", str(self.clusterIDs[-1]))

        # reset job properties
        self.reset()
        self.submitted = True

    # simple implementations of release, remove, ...
    def release(self) -> bool:
        if self.clusterIDs == []:
            return False
        self.schedd.act(htcondor.JobAction.Release, "ClusterId == "+str(self.clusterID))
        return True

    def remove(self) -> bool:
        if self.clusterIDs == []:
            return False
        self.schedd.act(htcondor.JobAction.Remove, "ClusterId == "+str(self.clusterID))
        return True

    # get information about the job
    def get_info(self) -> None:
        # check if job has an ID
        if self.clusterIDs == []:
            log.error("Trying to list info for a job which was not submitted")
            raise SystemError

        # get all job info
        ads = self.schedd.query(
            constraint="ClusterId==" + str(self.clusterID)
        )

        # check if only one job was returned
        if len(ads) != 1:
            log.error("HTCondor returned more than one job for given ID, this should not happen!")
            raise SystemError

        return ads[0] # we take only single job, so return onl the first eleement

    # get condor status of the job
    def get_condor_status(self) -> None:
        return self.get_info()["JobStatus"]

    # get status of the job, as defined in translate.py
    def get_status(self) -> int:
        # First check if the job is skipped or not even submitted
        if self.skipped:
            return 8
        elif self.clusterIDs == []:  # job was not even submitted
            return 9
        elif not os.path.isfile(self.logFile):
            return 10

        # Positive numbers are reserved for condor
        cndr_status = self.get_condor_status()
        # If job complete, check if with error:
        if cndr_status == 4:
            with open(self.config["log"]) as search:
                if "Job terminated" in search.read():
                    for line in search:
                        line = line.rstrip()  # remove '\n' at end of line
                        if "Normal termination (return value" in line:
                            status = int(line.split("value")[1].strip()[:-1])

                            if status == 0:
                                self.done = True
                                return 4  # success

                            self.failed = True
                            return -status
                    return 11  # no "Normal termination for Job terminated"
                elif "Job was aborted by the user" in search.read():
                    # I think this is the same as 3 but need to check
                    return 12
                else:
                    log.error("Uknown output of job %s!", self.name)
        return 0

    def set_custom(self, dict: Dict[str, str]) -> None:
        for key, item in dict.items():
            self.config[key] = item

    def set_time(self, runTime: int) -> None:
        self.config["+RequestRuntime"] = str(runTime)
        self.config["+MaxRuntime"] = str(runTime)

    def set_arguments(self, args: str) -> None:
        self.config["arguments"] = args
