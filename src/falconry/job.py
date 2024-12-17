import htcondor
import os
import logging

from typing import List, Dict, Any

from . import translate
from .schedd_wrapper import ScheddWrapper

log = logging.getLogger('falconry')


class job:
    """Submits and holds a single job and all relevant information

    The schedd can be imported as:

    .. code-block:: python

        from falconry import ScheddWrapper
        schedd = ScheddWrapper()

    or from the manager:

    .. code-block:: python

        from falconry import manager
        mgr = manager.Manager(mgrDir, mgrMsg)
        schedd = mgr.schedd

    Currently planning to keep single job per clusterID,
    since group submittion would significantly complicate resubmitting.
    HTCondor does not seem to allow for re-submittion of single ProcId,
    so one would have to first connect specific arguments to specific ProcIds
    and then resubmit individual jobs anyway.


    Arguments:
        name (str): name of the job for easy identification
        schedd (ScheddWrapper): HTCondor schedd wrapper
    """

    def __init__(self, name: str, schedd: ScheddWrapper) -> None:

        # first, define HTCondor schedd wrapper
        self.schedd = schedd

        # name of the job for easy identification
        self.name = name

        # since we will be resubmitting, job IDs are kept as a list
        self.clusterIDs: List[str] = []

        # add a decoration to the job to hold dependencies
        self.dependencies: List["job"] = []

        # configuration of the jobs
        self.config: Dict[str, str] = {}

        # to setup initial state (done/submitted and so on)
        self.reset()

    def set_simple(self, exe: str, logPath: str):
        """Sets up a simple job with only executable and a path to log files

        Arguments:
            exe (str): path to the executable
            logPath (str): path to the log files
        """

        # htcondor defines job as a dict
        cfg = {
            "executable": exe,
            "output": logPath + "/" + self.name + "/$(ClusterId).out",
            "error": logPath + "/" + self.name + "/$(ClusterId).err",
            "log": logPath + "/" + self.name + "/$(ClusterId).log",
        }
        self.config = cfg

        # create the directory for the log
        logDir = os.getcwd() + "/" + logPath + "/" + self.name + "/"
        if not os.path.exists(logDir):
            os.makedirs(logDir)

        # setup flags:
        self.reset()

    def save(self) -> Dict[str, Any]:
        """Returns a dictionary containing all relevant job information
        to be saved to a file.

        Returns:
            Dict[str, Any]: dictionary containing job information
        """
        # first rewrite dependencies using names
        depNames = [j.name for j in self.dependencies]
        jobDict = {
            "clusterIDs": self.clusterIDs,
            "config": self.config,
            "depNames": depNames,
            "done": "false",
        }
        # to test if job is done takes long time
        # because log file needs to be checked
        # so its best to save this status
        if self.done:
            jobDict["done"] = "true"
        return jobDict

    def load(self, jobDict: Dict[str, Any]) -> None:
        """Loads a job from a dictionary created using the save function.

        Arguments:
            jobDict (Dict[str, Any]): dictionary containing job information
        """

        # TODO: define proper "jobDict checker"
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
            self.logFile = self.config["log"].replace(
                "$(ClusterId)", str(self.clusterID)
            )
            self.outFile = self.config["output"].replace(
                "$(ClusterId)", str(self.clusterID)
            )
            self.errFile = self.config["error"].replace(
                "$(ClusterId)", str(self.clusterID)
            )
            self.submitted = True

    def reset(self) -> None:
        """Resets job flags"""
        self.submitted = False
        self.skipped = False
        self.failed = False
        self.done = False

    def add_job_dependency(self, *args: "job") -> None:
        """Add dependencies to the job.

        Arguments:
            *args (List["job"]): list of jobs
        """
        self.dependencies.extend(list(args))

    # submit the job
    def submit(self, force: bool = False) -> None:
        """Submits the job to HTCondor if either the job is not submitted,
        the force flag is set or the job failed.

        Arguments:
            force (bool, optional): force submission. Defaults to False.
        """
        # TODO: raise error if problem

        # force: for cases when the job status was checked
        # e.g. when retrying
        # this is can save a lot of time because
        # failed job required the log file to be read.
        # Should not be used by users, only internally.

        # first check if job was not submitted before:
        if not force and self.clusterIDs != []:
            status = self.get_status()
            if status == 12 or status < 0 or status == 10:
                log.info("Job %s failed and will be resubmitted.", self.name)
            else:
                log.info(
                    "The job is %s, not submitting", translate.statusMessage[status]
                )
                return
        else:
            # the htcondor version of the configuration
            self.htjob = htcondor.Submit(self.config)

        # Of course the submit has different capitalization here ...
        self.submit_result = self.schedd.submit(self.htjob)
        self.clusterID = self.submit_result.cluster()

        self.clusterIDs.append(self.clusterID)
        log.info("Submitting job %s with id %s", self.name, self.clusterID)
        log.debug(self.config)
        self.logFile = self.config["log"].replace("$(ClusterId)", str(self.clusterID))
        self.outFile = self.config["output"].replace(
            "$(ClusterId)", str(self.clusterID)
        )
        self.errFile = self.config["error"].replace("$(ClusterId)", str(self.clusterID))

        # reset job properties
        self.reset()
        self.submitted = True

    def release(self) -> bool:
        """Releases held job"""
        if self.clusterIDs == []:
            return False
        self.schedd.act(
            htcondor.JobAction.Release, "ClusterId == " + str(self.clusterID)
        )
        log.info("Releasing job %s with id %s", self.name, self.clusterID)
        return True

    def remove(self) -> bool:
        """Removes the job from HTCondor"""
        if self.clusterIDs == []:
            return False
        self.schedd.act(
            htcondor.JobAction.Remove, "ClusterId == " + str(self.clusterID)
        )
        log.info("Removing job %s with id %s", self.name, self.clusterID)
        return True

    def get_info(self) -> Dict[str, Any]:
        """Returns information about the job

        Returns:
            Dict[str, Any]: dictionary containing job information
        """
        # check if job has an ID
        if self.clusterIDs == []:
            log.error("Trying to list info for a job which was not submitted")
            raise SystemError

        constr = "ClusterId == " + str(self.clusterID)
        # get all job info of running job
        ads = self.schedd.query(constraint=constr)

        # if the job finished, query will be empty and we have to use history
        # because condor is stupid, it returns and iterator (?),
        # so just returning first element
        # TODO: add more to projection
        if ads == []:
            for ad in self.schedd.history(constraint=constr, projection=["JobStatus"]):
                return ad

        # check if only one job was returned
        if len(ads) != 1:
            # empty job is probably job finished a long ago
            # return specific code -999 and let get_status function
            # sort the rest from the log files
            if ads == []:
                return {"JobStatus": -999}
            else:
                log.error(
                    "HTCondor returned more than one jobs for given ID, this should not happen!"
                )
                log.error("Job %s with id %u", self.name, self.clusterID)
                print(ads)
                raise SystemError

        return ads[0]  # we take only single job, so return onl the first eleement

    def get_status(self) -> int:
        """Returns status of the job, as defined in translate.py

        Returns:
            int: status of the job
        """

        # First check if the job is skipped or not even submitted
        if self.skipped:
            return 8
        elif self.done:
            return 4
        elif self.clusterIDs == []:  # job was not even submitted
            return 9
        elif not os.path.isfile(self.logFile):
            return 10

        status_log = self._get_status_log()
        if status_log != 0:  # 0 for unknown so try from condor
            return status_log

        cndr_status = self._get_status_condor()
        # If job is incomplete, simply return the status:
        if cndr_status != 4 and cndr_status != -999:
            return cndr_status

        log.error("Unknown output of job %s!", self.name)
        return 0

    def _get_status_condor(self) -> int:
        """Returns status of the job, as defined in condor

        Returns:
            int: status of the job
        """
        return self.get_info()["JobStatus"]

    def _get_status_log(self) -> int:
        """Gets status from the log file

        Returns:
            int: status of the job
        """
        # Check log file to determine if job finished with an error
        with open(self.logFile, 'r') as fl:
            search = fl.read()

        # User abortion is special case
        if "Job was aborted by the user" in search:
            # I think this is the same as 3 but need to check
            return 12

        # Sometimes `removed` is not properly saved
        # (probably when continuing after long time?)
        # so here alternative way
        if "SYSTEM_PERIODIC_REMOVE" in search or "Job was aborted" in search:
            return 3

        # Otherwise check `"Job terminated"`. If the log does not contain it
        # its unknown state
        if "Job terminated" not in search:
            return 0

        # Evaluate `"Job terminated"`
        searchSplit = search.split("\n")
        for line in searchSplit:
            if "Normal termination (return value" in line:
                line = line.rstrip()  # remove '\n' at end of line
                status = int(line.split("value")[1].strip()[:-1])

                if status == 0:
                    self.done = True
                    return 4  # success

                log.debug(f"Job failed {status}")
                self.failed = True
                # Positive  values reserved for falconry states,
                # so return as negative
                return -status
        return 11  # no "Normal termination for Job terminated"

    def set_custom(self, dict: Dict[str, str]) -> None:
        """Sets custom configuration for the job from a dictionary

        Arguments:
            dict (Dict[str, str]): dictionary containing job configuration
        """
        for key, item in dict.items():
            self.config[key] = item

    def set_time(self, runTime: int, useRequestRuntime: bool = False) -> None:
        """Sets time limit for the job.

        For some clusters (DESY), RequestRuntime is used instead of MaxRuntime,
        to use it set useRequestRuntime to `True`.

        Arguments:
            runTime (int): time limit in seconds
            useRequestRuntime (bool, optional): use RequestRuntime option. Defaults to False.
        """
        self.config["+MaxRuntime"] = str(runTime)
        # RequestRuntime seems to be DESY specific option and does not work
        # e.g. in Prague (jobs get held), so this option is false by default
        if useRequestRuntime:
            self.config["+RequestRuntime"] = str(runTime)

    def set_arguments(self, args: str) -> None:
        """Sets arguments for the job

        Arguments:
            args (str): arguments for the job
        """
        self.config["arguments"] = args
