import htcondor  # for submitting jobs, querying HTCondor daemons, etc.
import logging

from typing import List

from . import job

log = logging.getLogger(__name__)


class manager:
    """ Manager holds all the job and periodically checks their status.

    It also take care of dependent jobs,
    submitting jobs when all dependencies are satisfied.
    These are handled as decorations of the job.
    """

    # Initialize the manager, maily getting the htcondor schedd
    def __init__(self):
        log.info("MONITOR: INIT")

        #  get the schedd
        self.schedd = htcondor.Schedd()
        print(self.schedd)

        # job collection
        self.jobs: List[job.job] = []  # holds jobs
        self.jobNames: List[str] = []  # holds job names to ensure uniqueness

        # flags
        self.retryFailed = False

    # add a job to the manager
    def add_job(self, j: job.job):
        # first check if the jobs already exists
        if j.name in self.jobNames:
            log.error("Job %s already exists! Exiting ...")
            raise SystemExit

        self.jobs.append(j)
        self.jobNames.append(j.name)

    # check all tasks in queue whether the jobs they depend on already finished.
    # If some of them failed, add this task to the skipped.
    def check_dependence(self):
        # TODO: consider if not submitted jobs in a special list

        for j in self.jobs:
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
                    log.error("Job %s depends on job %s which either failed or was skipped! Skipping ...", j.name, tarJob.name)
                    j.skipped = True
                break
            if isReady:
                j.submit()

    # start the manager, iteratively checking status of jobs
    def start(self, sleepTime: int = 60):

        import datetime         # so user knowns the time of last check
        from time import sleep  # used for sleep time between checks
        log.info("MONITOR: START")

        while True:
            log.info("Checking status of jobs [%s]", str(datetime.datetime.now()))

            # checking dependencies and submitting ready jobs
            self.check_dependence()

            waiting = 0
            notSub = 0
            idle = 0
            run = 0
            failed = 0
            done = 0
            skipped = 0

            # resubmit jobs and find out state of the jobs
            for j in self.jobs:

                # ignore jobs which are not submitted
                if not j.submitted:
                    waiting += 1
                    continue

                # first resubmit jobs
                status = j.get_status()
                log.debug("Job %s has status %u", j.name, status)
                if status == 12 or (self.retryFailed and status < 0):
                    log.warning("Error! Job %s failed due to condor, rerunning", j.name)
                    j.submit()

                # count jobs with different statuses
                if status == 9 or status == 10:
                    notSub += 1
                elif status == 1:
                    idle += 1
                elif status == 2:
                    run += 1
                elif status == 12 or (self.retryFailed and status < 0):
                    failed += 1
                elif status == 4:
                    done += 1
                elif status == 8:
                    skipped += 1

            log.info(
                "Not sub.: %s | idle: %s | running: %s | failed: %s | done: %s | waiting: %s | skipped: %s",
                notSub, idle, run, failed, done, waiting, skipped
            )

            # if no job is waiting nor running, finish the manager
            if not (waiting or (notSub + idle + run > 0)):
                break

            sleep(sleepTime)

        log.info("MONITOR: FINISHED")
