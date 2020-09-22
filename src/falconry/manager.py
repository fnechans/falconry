import htcondor  # for submitting jobs, querying HTCondor daemons, etc.
import logging
import json
import os
import datetime         # so user knowns the time of last check
from time import sleep  # used for sleep time between checks

from typing import Dict, Any

from . import job
from . import translate

log = logging.getLogger(__name__)


class manager:
    """ Manager holds all jobs and periodically checks their status.

    It also take care of dependent jobs,
    submitting jobs when all dependencies are satisfied.
    These are handled as decorations of the job.
    """

    # Initialize the manager, maily getting the htcondor schedd
    def __init__(self, mgrDir):
        log.info("MONITOR: INIT")

        #  get the schedd
        self.schedd = htcondor.Schedd()

        # job collection
        self.jobs: Dict[str, job.job] = {}

        # flags
        self.retryFailed = False

        # now create a directory where the info about jobs will be save
        if not os.path.exists(mgrDir):
            os.makedirs(mgrDir)
        self.dir = mgrDir

    # add a job to the manager
    def add_job(self, j: job.job):
        # first check if the jobs already exists
        if j.name in self.jobs.keys():
            log.error("Job %s already exists! Exiting ...")
            raise SystemExit

        self.jobs[j.name] = j

    # save current status
    def save(self):
        log.info("Saving current status of jobs")
        output: Dict[str, Any] = {}
        for name, j in self.jobs.items():
            output[name] = j.save()

        with open(self.dir+"/data.json", "w") as f:
            json.dump(output, f)
            log.info("Success!")

    # load saved jobs
    def load(self):
        log.info("Loading past status of jobs")
        with open(self.dir+"/data.json", "r") as f:
            input = json.load(f)
            for name, jobDict in input.items():
                # create a job
                j = job.job(name, self.schedd)
                j.load(jobDict)
                # add it to the manager
                self.add_job(j)

                # decorate the list of names of the dependencies
                j.depNames = jobDict["depNames"]
            # now that jobs are defined, dependencies can be recreated:
            for j in self.jobs.values():
                dependencies = [self.jobs[name] for name in j.depNames]
                j.add_job_dependency(dependencies)

    # check all tasks in queue whether the jobs they depend on already finished.
    # If some of them failed, add this task to the skipped.
    def check_dependence(self):
        # TODO: consider if not submitted jobs in a special list

        for name, j in self.jobs.items():
            # only check jobs which are neither submitted nor skipped
            if j.submitted or j.skipped:
                continue

            # if ready submit, single not done dependency leads to isReady=False
            isReady = True
            for tarJob in j.dependencies:
                # if any job is not done, do not submit
                if tarJob.done:
                    continue

                isReady = False

                if tarJob.skipped or tarJob.failed:
                    log.error("Job %s depends on job %s which either failed or was skipped! Skipping ...", name, tarJob.name)
                    j.skipped = True
                break
            if isReady:
                j.submit()

    # check if some job should be resubmitted
    def check_resubmit(self, j: job.job):
        status = j.get_status()
        if status > 0:
            log.debug("Job %s has status %s", j.name, translate.statusMessage[status])
        if status == 12 or (self.retryFailed and status < 0):
            log.warning("Error! Job %s failed due to condor, rerunning", j.name)
            j.submit()

    # start the manager, iteratively checking status of jobs
    def start(self, sleepTime: int = 60):

        log.info("MONITOR: START")

        while True:
            log.info("Checking status of jobs [%s]", str(datetime.datetime.now()))

            waiting = 0
            notSub = 0
            idle = 0
            run = 0
            failed = 0
            done = 0
            skipped = 0

            # resubmit jobs and find out state of the jobs
            for j in self.jobs.values():

                # ignore jobs which are not submitted, skipped or done
                if j.skipped:
                    skipped += 1
                    continue
                elif not j.submitted:
                    waiting += 1
                    continue
                elif j.done:
                    done += 1
                    continue

                #  resubmit jobs which failed due to condor problems
                self.check_resubmit(j)

                # count jobs with different statuses
                status = j.get_status()
                if status == 9 or status == 10:
                    notSub += 1
                elif status == 1:
                    idle += 1
                elif status == 2:
                    run += 1
                elif status < 0:
                    failed += 1
                elif status == 4:
                    done += 1

            log.info(
                "Not sub.: %s | idle: %s | running: %s | failed: %s | done: %s | waiting: %s | skipped: %s",
                notSub, idle, run, failed, done, waiting, skipped
            )

            # if no job is waiting nor running, finish the manager
            if not (waiting or (notSub + idle + run > 0)):
                break

            # checking dependencies and submitting ready jobs
            self.check_dependence()

            sleep(sleepTime)

        log.info("MONITOR: FINISHED")
