import htcondor2 as htcondor
import os
import logging
import glob
from copy import copy
import time

from typing import List, Dict, Any, Optional

from .status import FalconryStatus
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
        self.jobIDs: List[str] = []
        self.jobID: Optional[str] = None
        self.jobDir: Optional[str] = None
        # For sanity check that new ID is indeed newer
        # (in case we are deducing new ID from log files for example,
        #  and the jobIDs got reset for some reason)
        self.jobTimeStamp: Optional[int] = None

        # add a decoration to the job to hold dependencies
        self.dependencies: List["job"] = []

        # configuration of the jobs
        self.config: Dict[str, str] = {}

        # to setup initial state (done/submitted and so on)
        self.reset()

        # keep track of last status to avoid calling get_status too often
        self.lastStatus: FalconryStatus = FalconryStatus.UNKNOWN

    def set_simple(self, exe: str, logPath: str) -> None:
        """Sets up a simple job with only executable and a path to log files

        Arguments:
            exe (str): path to the executable
            logPath (str): path to the log files
        """
        # Extend the log path
        logPathBase = os.path.abspath(logPath)
        logDir = os.path.join(logPathBase, "logs")
        self.jobDir = os.path.join(logPathBase, self.name)

        # htcondor defines job as a dict
        cfg = {
            "executable": exe,
            "log": os.path.join(logDir, "$(JobId).log"),
            "output": os.path.join(self.jobDir, "$(JobId).out"),
            "error": os.path.join(self.jobDir, "$(JobId).err"),
        }
        self.config = cfg

        # create the directory for the log
        if not os.path.exists(logDir):
            os.makedirs(logDir, exist_ok=True)

        if not os.path.exists(self.jobDir):
            os.makedirs(self.jobDir, exist_ok=True)

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
            "jobIDs": self.jobIDs,
            "jobDir": self.jobDir,
            "jobTimeStamp": self.jobTimeStamp,
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
        if "jobIDs" not in jobDict.keys() and "config" not in jobDict.keys():
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
        self.jobIDs = jobDict["jobIDs"]
        self.jobDir = jobDict["jobDir"]
        self.jobTimeStamp = jobDict["jobTimeStamp"]

        # if not empty, the job has been already submitted at least once
        if len(self.jobIDs) > 0:
            self.jobID = self.jobIDs[-1]

            self.expand_files()
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
    def submit(self, force: bool = False, doNotSubmit: bool = False) -> None:
        """Submits the job to HTCondor if either the job is not submitted,
        the force flag is set or the job failed.

        Arguments:
            force (bool, optional): force submission. Defaults to False.
            doNotSubmit (bool, optional): does not submit, just updates htjob. Defaults to False.
        """
        # TODO: raise error if problem

        # force: for cases when the job status was checked
        # e.g. when retrying
        # this is can save a lot of time because
        # failed job required the log file to be read.
        # Should not be used by users, only internally.

        # first check if job was not submitted before:
        if not force and self.jobIDs != []:
            status = self.get_status()
            if status in [
                FalconryStatus.ABORTED_BY_USER,
                FalconryStatus.FAILED,
                FalconryStatus.LOG_FILE_MISSING,
            ]:
                log.info("Job %s failed and will be resubmitted.", self.name)
            else:
                log.info("The job is %s, not submitting", status.name)
                return

        # the htcondor version of the configuration
        htjob = htcondor.Submit(self.config)  # type: ignore

        if doNotSubmit:
            return

        # Of course the submit has different capitalization here ...
        submit_result = self.schedd.submit(htjob)
        self.submit_done(f"{submit_result.cluster()}.0")

    def find_id(self) -> None:
        """Finds the job ID based on the log file names
        and updates the job accordingly.

        Only used with remote mode to synchronize jobs
        between the local and remote client.
        """
        # Find all log files
        if self.jobDir is None:
            raise RuntimeError(f'Job directory is not set for job {self.name}')
        logFiles = glob.glob(os.path.join(self.jobDir, "*.log"))
        logFiles.sort(reverse=True)
        if len(logFiles) == 0:
            return
        for logFile in logFiles:
            jobid = os.path.basename(logFile).strip(".log")
            if jobid not in self.jobIDs:
                # If job id is smaller then the last one,
                # it can mean that the numbers got reset,
                # best to check timestamp
                if len(self.jobIDs) > 0 and jobid < self.jobIDs[-1]:
                    tmp_copy = copy(self)
                    tmp_copy.jobID = jobid
                    # This can be quite slow if job is old
                    # (history is much slower than query)
                    # TODO: take from log files?
                    timestamp = int(tmp_copy.get_info()["QDate"])
                    if self.jobTimeStamp is not None and (
                        timestamp == -999 or timestamp < self.jobTimeStamp
                    ):
                        continue
                self.submit_done(jobid)

    def submit_done(self, jobID: str) -> None:
        """Sets the job as submitted and updates job ID

        Arguments:
            jobID (str): job ID
        """
        if jobID in self.jobIDs:
            log.debug(f'Job {self.name} has already been submitted with id {jobID}')
            return
        self.jobID = jobID
        self.jobTimeStamp = int(time.time())
        # Following would be more precise but it can get really slow
        # for old jobs - can this create issues?
        # int(self.get_info()["QDate"])
        self.jobIDs.append(self.jobID)
        log.info(f"Job {self.name}: found id {self.jobID}")
        log.debug(self.config)
        self.expand_files()
        # reset job properties
        self.reset()
        self.submitted = True

    def expand_files(self) -> None:
        """Expands job files with cluster and job IDs"""

        def update_symlink(source: str, dest: str) -> None:
            if os.path.islink(dest):
                os.remove(dest)
            os.symlink(source, dest)

        if self.jobDir is None:
            raise RuntimeError('Job directory is not set for job %s' % self.name)
        if self.jobID is None:
            raise RuntimeError('Job ID is not set for job %s' % self.name)

        self.logFile = os.path.join(self.jobDir, f"{self.jobID}.log")
        logFile = self.config["log"].replace("$(JobId)", self.jobID)
        update_symlink(logFile, self.logFile)

        self.outFile = self.config["output"].replace("$(JobId)", self.jobID)

        self.errFile = self.config["error"].replace("$(JobId)", self.jobID)

    @property
    def clusterId(self) -> str:
        """Returns cluster ID"""
        assert self.jobID is not None, "Job ID is not set to determine cluster ID"
        return self.jobID.split(".")[0]

    @property
    def procId(self) -> str:
        """Returns proc ID"""
        assert self.jobID is not None, "Job ID is not set to determine proc ID"
        return self.jobID.split(".")[1]

    @property
    def act_constraints(self) -> str:
        """Returns HTCondor constraints for the job"""
        return f"(ClusterId == {self.clusterId}) && (ProcId == {self.procId})"

    def release(self) -> bool:
        """Releases held job"""
        if self.jobID is None:
            return False
        self.schedd.act(
            htcondor.JobAction.Release, self.act_constraints  # type: ignore
        )
        log.info("Releasing job %s with id %s", self.name, self.jobID)
        return True

    def remove(self) -> bool:
        """Removes the job from HTCondor"""
        if self.jobID is None:
            return False
        self.schedd.act(htcondor.JobAction.Remove, self.act_constraints)  # type: ignore
        log.info("Removing job %s with id %s", self.name, self.jobID)
        return True

    def get_info(self) -> Dict[str, Any]:
        """Returns information about the job

        Returns:
            Dict[str, Any]: dictionary containing job information
        """
        # check if job has an ID
        if self.jobID is None:
            log.error("Trying to list info for a job which was not submitted")
            raise SystemError

        # get all job info of running job
        ads = self.schedd.query(
            constraint=self.act_constraints, projection=["JobStatus", "QDate"]
        )

        # if the job finished, query will be empty and we have to use history
        # because condor is stupid, it returns and iterator (?),
        # so just returning first element
        # TODO: add more to projection
        if ads == []:
            for ad in self.schedd.history(
                constraint=self.act_constraints, projection=["JobStatus", "QDate"]
            ):
                return ad

        # check if only one job was returned
        if len(ads) != 1:
            # empty job is probably job finished a long ago
            # return specific code -999 and let get_status function
            # sort the rest from the log files
            if ads == []:
                return {"JobStatus": -999, "QDate": -999}
            else:
                log.error(
                    "HTCondor returned more than one jobs for given ID, this should not happen!"
                )
                log.error(f"Job {self.name} with id {self.jobID}")
                print(ads)
                raise SystemError

        return ads[0]  # we take only single job, so return onl the first eleement

    def get_status(self) -> FalconryStatus:
        """Updates status of the job and returns it.
        Status is defined in status.py

        Returns:
            int: status of the job
        """
        self.lastStatus = self._get_status()
        return self.lastStatus

    def _get_status(self) -> FalconryStatus:
        """Returns status of the job, as defined in status.py

        Returns:
            int: status of the job
        """

        # First check if the job is skipped or not even submitted
        if self.skipped:
            return FalconryStatus.SKIPPED
        elif self.done:
            return FalconryStatus.COMPLETE
        elif self.jobID is None:  # job was not even submitted
            return FalconryStatus.NOT_SUBMITTED
        # Using exists here is crucial, it returns false for broken symlinks
        elif not os.path.exists(self.logFile):
            return FalconryStatus.LOG_FILE_MISSING

        status_log = self._get_status_log()
        if status_log != 0:  # 0 for unknown so try from condor
            if status_log < 0:
                return FalconryStatus.FAILED
            return FalconryStatus(status_log)

        cndr_status = self._get_status_condor()
        # If job is incomplete, simply return the status:
        if cndr_status != 4 and cndr_status != -999:
            return FalconryStatus(cndr_status)

        log.error("Unknown output of job %s!", self.name)
        return FalconryStatus.UNKNOWN

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

    def set_custom(self, config: Dict[str, str]) -> None:
        """Sets custom configuration for the job from a dictionary

        Arguments:
            config (Dict[str, str]): dictionary containing job configuration
        """
        for key, item in config.items():
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
