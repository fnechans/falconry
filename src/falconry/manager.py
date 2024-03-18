import logging
import json
import os
import shutil
import sys
import traceback
import datetime         # so user knowns the time of last check

from typing import Dict, Any, Tuple, Optional

from .job import job
from . import translate
from . import cli
from .schedd_wrapper import ScheddWrapper

log = logging.getLogger(__name__)


class counter:
    # just holds few variables used in status print
    def __init__(self):
        self.waiting = 0
        self.notSub = 0
        self.idle = 0
        self.run = 0
        self.failed = 0
        self.done = 0
        self.skipped = 0
        self.removed = 0
        self.held = 0

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class manager:
    """ Manager holds all jobs and periodically checks their status.

    It also take care of dependent jobs,
    submitting jobs when all dependencies are satisfied.
    These are handled as decorations of the job.
    """
    reservedNames = ["Message", "Command"]

    # Initialize the manager, maily getting the htcondor schedd
    def __init__(self, mgrDir: str, mgrMsg: str = "", maxJobIdle: int = -1):
        log.info("MONITOR: INIT")

        #  get the schedd
        self.schedd = ScheddWrapper()

        # job collection
        self.jobs: Dict[str, job] = {}

        # now create a directory where the info about jobs will be save
        if not os.path.exists(mgrDir):
            os.makedirs(mgrDir)
        self.dir = mgrDir
        self.saveFileName = self.dir+"/data.json"
        self.mgrMsg = mgrMsg
        self.command = " ".join(sys.argv)

        self.maxJobIdle = maxJobIdle 
        self.curJobIdle = 0

    # check if save file already exists
    def check_savefile_status(self) -> Tuple[bool, Optional[str]]:
        if os.path.exists(self.saveFileName):
            log.warning(f"Manager directory {self.dir} already exists!")
            state, var = cli.input_checker({
                "l": "Load existing jobs",
                "n": "Start new jobs"})

            # Simplify the output for user interface
            # both unknown/timeout have the same result
            if state == cli.InputState.SUCCESS:
                return True, var
            return False, var
        return True, "n"  # automatically assume new

    # ask for custom meesage
    def ask_for_message(self):
        import select
        i, o, e = select.select([sys.stdin], [], [], 60)
        if i:
            self.mgrMsg = sys.stdin.readline().strip()

    # add a job to the manager
    def add_job(self, j: job, update: bool = False):
        # some reserved names, to simplify saving later
        if j.name in manager.reservedNames:
            log.error("Name %s  is reserved! Exiting ...", j.name)
            raise SystemExit

        # first check if the jobs already exists
        if j.name in self.jobs.keys():
            if not update:
                log.error("Job %s already exists! Exiting ...", j.name)
                raise SystemExit
            else:
                # TODO: update old style string to f-strings?
                log.info(f"Updating job {j.name}.")

        self.jobs[j.name] = j

    # save current status
    def save(self, quiet: bool = False):
        if not quiet:
            log.info("Saving current status of jobs")
        output: Dict[str, Any] = {
            "Message": self.mgrMsg,
            "Command": self.command,
        }
        for name, j in self.jobs.items():
            output[name] = j.save()

        # save with a timestamp as a suffix, create sym link
        current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M_%S")
        fileLatest = f"{self.saveFileName}.latest"
        fileSuf = f"{self.saveFileName}.{current_time}"  # only if not quiet

        with open(fileLatest, "w") as f:
            json.dump(output, f)
            if not quiet:
                log.info("Success! Making copy with time-stamp.")
                if not os.path.exists(fileSuf):
                    shutil.copyfile(fileLatest, fileSuf)
                else:
                    raise FileExistsError(f"Destination file {fileSuf} already exists. "
                                          "This should not be possible.")

        # not necessary to remove, but maybe better to be sure its not broken
        if os.path.exists(self.saveFileName):
            os.remove(self.saveFileName)
        os.symlink(fileLatest.split("/")[-1], self.saveFileName)

    # load saved jobs and retry those that failed
    # when `retryFailed` is true
    def load(self, retryFailed: bool = False):
        log.info("Loading past status of jobs")
        with open(self.dir+"/data.json", "r") as f:
            input = json.load(f)
            depNames = {}
            for name, jobDict in input.items():
                if name in manager.reservedNames:
                    continue
                log.debug("Loading job %s", name)

                # create a job
                j = job(name, self.schedd)
                j.load(jobDict)

                # add it to the manager
                self.add_job(j, update=True)

                # decorate the list of names of the dependencies
                depNames[j.name] = jobDict["depNames"]

        # Now that jobs are defined, dependencies can be recreated
        # also resubmit jobs which failed
        for j in self.jobs.values():
            dependencies = [self.jobs[name] for name in depNames[j.name]]
            j.add_job_dependency(*dependencies)

        # Retry failed jobs
        # Since this changes the status and submits
        # jobs, add safequard in case of crash
        # to save up-to-date state
        if retryFailed:
            try:
                for j in self.jobs.values():
                    self.check_resubmit(j, True)
            except KeyboardInterrupt:
                log.error("Manager interrupted with keyboard!")
                log.error("Saving and exitting ...")
                self.save()
                self.print_failed()
                sys.exit(0)
            except Exception:
                log.error("Error ocurred when running manager!")
                traceback.print_exc(file=sys.stdout)
                self.save()
                self.print_failed()
                sys.exit(1)

    # print names of all failed jobs
    def print_failed(self, printLogs : bool = False):
        log.info("Printing failed jobs:")
        for name, j in self.jobs.items():
            if j.get_status() < 0:
                log.info("%s (id %u)", name, j.clusterID)
                if printLogs:
                    log.info(f"log: {j.logFile}")
                    log.info(f"out: {j.config['output']}")
                    log.info(f"err: {j.config['error']}")
        # TODO: maybe separate failed and removed?
        log.info("Printing removed jobs:")
        for name, j in self.jobs.items():
            if j.get_status() == 3:
                log.info("%s (id %u)", name, j.clusterID)
                if printLogs:
                    log.info(f"log: {j.logFile}")
                    log.info(f"out: {j.config['output']}")
                    log.info(f"err: {j.config['error']}")

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

                status = tarJob.get_status()
                if status == 3:
                    log.error("Job %s depends on job %s which is %s! Skipping ...",
                              name, tarJob.name, translate.statusMessage[status])
                    j.skipped = True

                break

            if isReady:
                # Check if we did not reach maximum number of submitted jobs
                if self.maxJobIdle != -1 and self.curJobIdle > self.maxJobIdle:
                    break # break because it does not make sense to check any other jobs now
                j.submit()
                self.curJobIdle += 1 # Add the jobs as a idle for now

    # check if some job should be resubmitted
    def check_resubmit(self, j: job, retryFailed: bool = False):
        status = j.get_status()
        if status > 0:
            log.debug("Job %s has status %s", j.name, translate.statusMessage[status])
        if status == 12:
            log.warning("Error! Job %s (id %s) failed due to condor, rerunning", j.name, j.clusterID)
            j.submit(force=True)
        elif retryFailed and status < 0:
            log.warning("Error! Job %s (id %s) failed and will be retried, rerunning", j.name, j.clusterID)
            j.submit(force=True)
        elif retryFailed and status == 3:
            log.warning("Error! Job %s (id %s) was removed and will be retried, rerunning", j.name, j.clusterID)
            j.submit(force=True)
        elif retryFailed and j.submitted and (status == 9 or status == 10):
            log.warning("Error! Job %s was not submitted succesfully (probably...), rerunning", j.name)
            j.submit(force=True)

    # resubmit jobs and find out state of the jobs
    def count_jobs(self, c: counter):

        maxLength = 0
        for name, j in self.jobs.items():
            printStr = f"Checking {name}\t\t\t\t\t\t\r"
            if len(printStr) > maxLength:
                maxLength = len(printStr)
            print(printStr, end='')

            self.count_job(c, j)

            print(" "*maxLength+"\r", flush=True, end='')

    # resubmit jobs and find out state of a job
    def count_job(self, c: counter, j: job):

        # first check if job is not submitted, skipped or done
        if j.skipped:
            c.skipped += 1
            return
        if not j.submitted:
            c.waiting += 1
            return
        if j.done:
            c.done += 1
            return

        #  resubmit job which failed due to condor problems
        self.check_resubmit(j)

        # count job with different status
        status = j.get_status()
        if status == 9 or status == 10:
            c.notSub += 1
        elif status == 1:
            c.idle += 1
        elif status == 2:
            c.run += 1
        elif status < 0:
            c.failed += 1
        elif status == 4:
            c.done += 1
        elif status == 5:
            c.held += 1
        elif status == 3:
            c.removed += 1

    # start the manager, iteratively checking status of jobs
    def start_cli(self, sleepTime: int = 60):
        # TODO: maybe add flag to save for each check? or every n-th check?

        log.info("MONITOR: START")

        c = counter()
        while True:

            log.info("|-Checking status of jobs [%s]-----------|", str(datetime.datetime.now()))

            cOld = c
            c = counter()
            self.count_jobs(c)

            # only printout if something changed:
            if c != cOld:
                log.info(
                    "| nsub: {0:>4} | hold: {1:>4} | fail: {2:>4} | rem: {3:>5} | skip: {4:>4} |".format(
                        c.notSub, c.held, c.failed, c.removed, c.skipped
                    )
                )
                log.info(
                    "| wait: {0:>4} | idle: {1:>4} | RUN: {2:>5} | DONE: {3:>4} | TOT: {4:>5} |".format(
                        c.waiting, c.idle, c.run, c.done, len(self.jobs)
                    )
                )

                # if no job is waiting nor running, finish the manager
                if not (c.waiting + c.notSub + c.idle + c.run > 0):
                    break
                
                # Update current idle of jobs managed by manager.
                # All new jobs submitted jobs in `check_dependence`
                # will increase this number, that why we create different
                # variable than `c.idle`
                self.curJobIdle = c.idle

                # checking dependencies and submitting ready jobs
                self.check_dependence()
                self.save(quiet=True)

                # instead of sleeping wait for input
                log.info("|-Input 'f' to show failed jobs, 'ff' to also show log paths-------|")
                log.info("|-Input 'x' to exit----------------------------------------------|")
                log.info("|-Input 'retry all' to retry all failed jobs---------------------|")

            state, var = cli.input_checker({
                "f": "",
                "x": "",
                "ff": "",
                "retry all": ""}, silent=True,
                timeout=sleepTime)
            if state == cli.InputState.SUCCESS:
                if var == "f":
                    self.print_failed()
                elif var == "ff":
                    self.print_failed(True)
                elif var == "x":
                    log.info("MONITOR: EXITING")
                    return
                elif var == "retry all":
                    for j in self.jobs.values():
                        self.check_resubmit(j, True)

        log.info("MONITOR: FINISHED")

    # start the manager with gui, iteratively checking status of jobs
    def start_gui(self, sleepTime: int = 60):

        log.warning("GUI version is only experimental!")
        import tkinter as tk

        window = tk.Tk()
        window.title("Falconry monitor")
        frm_counter = tk.Frame()

        def quick_label(name: str, x: int, y: int = 0):
            lbl = tk.Label(master=frm_counter, width=10, text=name)
            lbl.grid(row=y, column=x)
            return lbl

        quick_label("Not sub.:", 0)
        quick_label("Idle:", 1)
        quick_label("Running:", 2)
        quick_label("Failed:", 3)
        quick_label("Done:", 4)
        quick_label("Waiting:", 5)
        quick_label("Skipped:", 6)
        quick_label("Removed:", 7)
        labels = {}
        labels["ns"] = quick_label("0", 0, 1)
        labels["i"] = quick_label("0", 1, 1)
        labels["r"] = quick_label("0", 2, 1)
        labels["f"] = quick_label("0", 3, 1)
        labels["d"] = quick_label("0", 4, 1)
        labels["w"] = quick_label("0", 5, 1)
        labels["s"] = quick_label("0", 6, 1)
        labels["rm"] = quick_label("0",  1)

        frm_counter.grid(row=0, column=0)

        def tk_count():
            c = counter()
            self.count_jobs(c)
            labels["ns"]["text"] = f"{c.notSub}"
            labels["i"]["text"] = f"{c.idle}"
            labels["r"]["text"] = f"{c.run}"
            labels["f"]["text"] = f"{c.failed}"
            labels["d"]["text"] = f"{c.done}"
            labels["w"]["text"] = f"{c.waiting}"
            labels["s"]["text"] = f"{c.skipped}"
            labels["rm"]["text"] = f"{c.removed}"

            # if no job is waiting nor running, finish the manager
            # TODO: add condition (close on finish)
            # if not (c.waiting + c.notSub + c.idle + c.run > 0):
            #    window.destroy()

            # checking dependencies and submitting ready jobs
            self.check_dependence()

            window.after(1000*sleepTime, tk_count)

        tk_count()
        log.info("MONITOR: START")
        window.mainloop()
        log.info("MONITOR: FINISHED")

    # if there is an error, especially interupt with keyboard,
    # save the current state of jobs
    def start(self, sleepTime: int = 60, gui: bool = False):
        try:
            if gui:
                self.start_gui(sleepTime)
            else:
                self.start_cli(sleepTime)  # argument is interval between checking of the jobs
        except KeyboardInterrupt:
            log.error("Manager interrupted with keyboard!")
            log.error("Saving and exitting ...")
            self.save()
            self.print_failed()
            sys.exit(0)
        except Exception as e:
            log.error("Error ocurred when running manager!")
            log.error(str(e))
            self.save()
            self.print_failed()
            sys.exit(1)

    # out-dated soon to be removed start_safe (now just start)
    def start_safe(self, sleepTime: int = 60, gui: bool = False):
        log.warning("IMPORTANT! `start_safe` is now renamed as `start`. "
                    "Change your scripts as `start_safe` will be removed "
                    "in next version!")
        self.start(sleepTime, gui)
