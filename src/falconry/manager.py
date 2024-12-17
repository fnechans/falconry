import logging
import json
import ijson
import os
import shutil
import sys
import traceback
import datetime
import select
from time import sleep

from typing import Dict, Any, Tuple, Optional

from .job import job
from . import translate
from . import cli
from .schedd_wrapper import ScheddWrapper

log = logging.getLogger('falconry')


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
    """Manager holds all jobs and periodically checks their status.

    It also take care of dependent jobs,
    submitting jobs when all dependencies are satisfied.
    These are handled as decorations of the job.

    Arguments:
        mgrDir (str): directory where the manager stores the jobs
        mgrMsg (str): message to be saved in the save file
        maxJobIdle (int): maximum number of idle jobs
    """

    reservedNames = ["Message", "Command"]

    def __init__(self, mgrDir: str, mgrMsg: str = "", maxJobIdle: int = -1):
        log.info("MONITOR: INIT")

        # Initialize the manager, maily getting the htcondor schedd
        self.schedd = ScheddWrapper()

        # job collection
        self.jobs: Dict[str, job] = {}

        # now create a directory where the info about jobs will be save
        if not os.path.exists(mgrDir):
            os.makedirs(mgrDir)
        self.dir = mgrDir
        self.saveFileName = self.dir + "/data.json"
        self.mgrMsg = mgrMsg
        self.command = " ".join(sys.argv)

        self.maxJobIdle = maxJobIdle
        self.curJobIdle = 0

    # check if save file already exists
    def check_savefile_status(self) -> Tuple[bool, Optional[str]]:
        """Checks if the save file already exists. If it does, asks the user
        whether to load existing jobs or start new ones.

        Returns:
            Tuple[bool, Optional[str]]: (True, 'l') if load, (True, 'n') if new,
            (False, None) if error
        """
        if os.path.exists(self.saveFileName):
            log.warning(f"Manager directory {self.dir} already exists!")
            state, var = cli.input_checker(
                {"l": "Load existing jobs", "n": "Start new jobs"}
            )

            # Simplify the output for user interface
            # both unknown/timeout have the same result
            if state == cli.InputState.SUCCESS:
                return True, var
            return False, var
        return True, "n"  # automatically assume new

    def ask_for_message(self):
        """Asks user for a message to be saved in the save file for bookkeeping."""

        log.info("Enter a message to be saved in the save file " "for bookkeeping.")
        i, o, e = select.select([sys.stdin], [], [], 60)
        if i:
            self.mgrMsg = sys.stdin.readline().strip()

    def add_job(self, j: job, update: bool = False):
        """Adds a job to the manager. If the job already exists and `update` is
        `True`, it will be updated.

        Arguments:
            j (job): job to be added
            update (bool, optional): whether to update the job if it already
            exists. Defaults to False.
        """
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
                log.info(f"Updating job {j.name}.")

        self.jobs[j.name] = j

    def save(self, quiet: bool = False):
        """Saves the current status of the jobs to a json file.

        If `quiet` is `True`, it will not print any messages and
        will not make a time-stamped copy of the save file.

        Arguments:
            quiet (bool, optional): whether to print messages. Defaults to False.
        """
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
            json.dump(output, f, indent=2)
            if not quiet:
                log.info("Success! Making copy with time-stamp.")
                if not os.path.exists(fileSuf):
                    shutil.copyfile(fileLatest, fileSuf)
                else:
                    raise FileExistsError(
                        f"Destination file {fileSuf} already exists. "
                        "This should not be possible."
                    )

        # not necessary to remove, but maybe better to be sure its not broken
        if os.path.exists(self.saveFileName):
            os.remove(self.saveFileName)
        os.symlink(fileLatest.split("/")[-1], self.saveFileName)

    def load(self, retryFailed: bool = False):
        """Loads the saved status of the jobs from a json file
        provided by the user.

        Arguments:
            retryFailed (bool, optional): whether to retry the failed jobs.
            Defaults to False.
        """
        log.info("Loading past status of jobs")
        with open(self.dir + "/data.json", "r") as f:
            depNames = {}
            for name, jobDict in ijson.kvitems(f, ""):
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
                    self._check_resubmit(j, True)
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
    def print_running(self, printLogs: bool = False):
        log.info("Printing running jobs:")
        for name, j in self.jobs.items():
            if j.get_status() == 2:
                log.info("%s (id %u)", name, j.clusterID)
                if printLogs:
                    log.info(f"log: {j.logFile}")
                    log.info(f"out: {j.outFile}")
                    log.info(f"err: {j.errFile}")

    # print names of all failed jobs
    def print_failed(self, printLogs: bool = False):
        """Prints names of all failed jobs.

        Arguments:
            printLogs (bool, optional): whether to print paths to logs.
                Defaults to False.
        """
        log.info("Printing failed jobs:")
        for name, j in self.jobs.items():
            if j.get_status() < 0:
                log.info("%s (id %u)", name, j.clusterID)
                if printLogs:
                    log.info(f"log: {j.logFile}")
                    log.info(f"out: {j.outFile}")
                    log.info(f"err: {j.errFile}")
        # TODO: maybe separate failed and removed?
        log.info("Printing removed jobs:")
        for name, j in self.jobs.items():
            if j.get_status() == 3:
                log.info("%s (id %u)", name, j.clusterID)
                if printLogs:
                    log.info(f"log: {j.logFile}")
                    log.info(f"out: {j.outFile}")
                    log.info(f"err: {j.errFile}")

    def _check_dependence(self):
        """Checks if all dependencies of a job are done. If so, it will submit
        the job. If any of the dependencies failed, it will add the job to the
        skipped list.
        """

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
                    log.error(
                        f"Job {name} depends on job {tarJob.name} which either failed or was skipped! Skipping ..."
                    )
                    j.skipped = True

                status = tarJob.get_status()
                if status == 3:
                    log.error(
                        f"Job {name} depends on job {tarJob.name} which is {translate.statusMessage[status]}! Skipping ..."
                    )
                    j.skipped = True

                break

            if isReady:
                # Check if we did not reach maximum number of submitted jobs
                if self.maxJobIdle != -1 and self.curJobIdle > self.maxJobIdle:
                    break  # break because it does not make sense to check any other jobs now
                j.submit()
                self.curJobIdle += 1  # Add the jobs as a idle for now

    def _check_resubmit(self, j: job, retryFailed: bool = False):
        """Checks if a job should be resubmitted due to some known problems.

        Arguments:
            j (job): job to check
            retryFailed (bool, optional): whether to also retry failed jobs.
                Defaults to False.
        """
        status = j.get_status()
        if status > 0:
            log.debug("Job %s has status %s", j.name, translate.statusMessage[status])
        if status == 12:
            log.warning(
                f"Error! Job {j.name} (id {j.clusterID}) failed due to condor, rerunning"
            )
            j.submit(force=True)
        elif retryFailed and status < 0:
            log.warning(
                f"Error! Job {j.name} (id {j.clusterID}) failed and will be retried, rerunning"
            )
            j.submit(force=True)
        elif retryFailed and status == 3:
            log.warning(
                f"Error! Job {j.name} (id {j.clusterID}) was removed and will be retried, rerunning"
            )
            j.submit(force=True)
        elif retryFailed and j.submitted and (status == 9 or status == 10):
            log.warning(
                f"Error! Job {j.name} was not submitted succesfully (probably...), rerunning"
            )
            j.submit(force=True)

        elif retryFailed and j.skipped:
            log.warning(
                f"Error! Job {j.name} was skipped and will be retried, rerunning"
            )
            j.skipped = False

    def _count_jobs(self, c: counter):
        """Counts the number of jobs with different status.
        Resubmits jobs which failed due to condor problems.

        Arguments:
            c (counter): counter object to count the jobs
        """

        maxLength = 0
        for name, j in self.jobs.items():
            printStr = f"Checking {name}\t\t\t\t\t\t\r"
            if len(printStr) > maxLength:
                maxLength = len(printStr)
            print(printStr, end='')

            self._count_job(c, j)

            print(" " * maxLength + "\r", flush=True, end='')

    def _count_job(self, c: counter, j: job):
        """Updates the counter object with the status of a single job.
        Also resubmits jobs which failed due to condor problems.

        Arguments:
            c (counter): counter object to update
            j (job): job to check
        """

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
        self._check_resubmit(j)

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

    def _start_cli(self, sleep_time: int = 60):
        """Starts the manager, iteratively checking status of jobs.

        Arguments:
            sleep_time (int, optional): time to sleep between checks.
                Defaults to 60.
        """
        # TODO: maybe add flag to save for each check? or every n-th check?

        log.info("MONITOR: START")

        c = counter()
        event_counter = 0
        while True:

            log.info(
                f"|-Checking status of jobs [{datetime.datetime.now()}]----------------|",
            )

            cOld = c
            c = counter()
            self._count_jobs(c)

            # if no job is waiting nor running, finish the manager
            if not (c.waiting + c.notSub + c.idle + c.run > 0):
                break

            # only printout if something changed:
            if c != cOld:
                sleep(0.2)  # the printing sometimes breaks here, adding delay helps...
                log.info(
                    "| nsub: {0:>4} | hold: {1:>5} | fail: {2:>6} | rem: {3:>6} | skip: {4:>5} |".format(
                        c.notSub, c.held, c.failed, c.removed, c.skipped
                    )
                )
                log.info(
                    "| wait: {0:>6} | idle: {1:>4} | RUN: {2:>5} | DONE: {3:>6} | TOT: {4:>6} |".format(
                        c.waiting, c.idle, c.run, c.done, len(self.jobs)
                    )
                )

                # Update current idle of jobs managed by manager.
                # All new jobs submitted jobs in `check_dependence`
                # will increase this number, that why we create different
                # variable than `c.idle`
                self.curJobIdle = c.idle

                # checking dependencies and submitting ready jobs
                self._check_dependence()
                self.save(quiet=True)

                # instead of sleeping wait for input
                log.info(
                    "|-Enter 'h' to show all commands, e.g. to resubmit or show failed jobs|"
                )

            # save with timestamp every 30 events
            # most important for first event when first
            # batch of jobs is defined
            if event_counter % 30 == 0:
                self.save()
            event_counter += 1

            self._cli_interface(sleep_time)

        log.info("MONITOR: FINISHED")

    def _cli_interface(self, sleep_time: int = 60):
        """CLI interface for the manager.

        Arguments:
            sleep_time (int, optional): time to sleep between checks.
                Defaults to 60.
        """
        print('>>>> ', end='', flush=True)
        state, var = cli.input_checker(
            {
                "h": "",
                "s": "",
                "f": "",
                "x": "",
                "ff": "",
                "retry all": "",
                "r": "",
                "rr": "",
            },
            silent=True,
            timeout=sleep_time,
        )
        if state == cli.InputState.TIMEOUT:
            print('\r    \r', end='', flush=True)
        elif state == cli.InputState.SUCCESS:
            if var == "f":
                self.print_failed()
            elif var == "s":
                self.save()
            elif var == "h":
                log.info(
                    "|-Enter 'f' to show failed jobs, 'ff' to also show log paths----------|"
                )
                log.info(
                    "|-Enter 'r' to show running jobs, 'rr' to also show log paths---------|"
                )
                log.info(
                    "|-Enter 'x' to exit, 's' to save or 'retry all' to retry all failed---|"
                )
                self._cli_interface(sleep_time)
            elif var == "ff":
                self.print_failed(True)
            elif var == "r":
                self.print_running()
            elif var == "rr":
                self.print_running(True)
            elif var == "x":
                log.info("MONITOR: EXITING")
                return
            elif var == "retry all":
                for j in self.jobs.values():
                    self._check_resubmit(j, True)

    def _start_gui(self, sleepTime: int = 60):
        """Starts the manager with GUI, iteratively checking status of jobs.

        This is only experimental!

        Arguments:
            sleepTime (int, optional): time to sleep between checks.
                Defaults to 60.
        """

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
        labels["rm"] = quick_label("0", 1)

        frm_counter.grid(row=0, column=0)

        def tk_count():
            c = counter()
            self._count_jobs(c)
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
            self._check_dependence()

            window.after(1000 * sleepTime, tk_count)

        tk_count()
        log.info("MONITOR: START")
        window.mainloop()
        log.info("MONITOR: FINISHED")

    def start(self, sleepTime: int = 60, gui: bool = False):
        """Starts the manager, iteratively checking status of jobs.

        Makes sure to save the current state of jobs
        in case of interupt or crash.

        Arguments:
            sleepTime (int, optional): time to sleep between checks.
                Defaults to 60.
            gui (bool, optional): whether to use GUI. Defaults to False.
                GUI is experimental!
        """
        try:
            if gui:
                self._start_gui(sleepTime)
            else:
                self._start_cli(sleepTime)
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

    def start_safe(self, sleepTime: int = 60, gui: bool = False):
        """Deprecated! Use `start` instead!"""
        log.warning(
            "IMPORTANT! `start_safe` is now renamed as `start`. "
            "Change your scripts as `start_safe` will be removed "
            "in next version!"
        )
        self.start(sleepTime, gui)
