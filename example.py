#!/usr/bin/env python3

import logging
from falconry import manager, job

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s (%(name)s): %(message)s"
)
log = logging.getLogger(__name__)


# using argparse to get command line arguments
def config():
    import argparse
    parser = argparse.ArgumentParser(description="Falconry. Read README!")
    parser.add_argument("--debug", action="store_true", help="Debug mode with more verbose printout")
    parser.add_argument("--dir", type=str, help="Path to output DIR. Output by default.", default="Output")
    return parser.parse_args()


def main():

    # getting command line arguments and creating the manager
    # the manager will periodically check the jobs and handle dependencies
    cfg = config()

    if cfg.debug:
        logging.getLogger("falconry").setLevel(logging.DEBUG)

    mgr = manager(cfg.dir)  # the argument specifies where the job is saved

    # Check if manager was already run in given dir and ask user
    # if they want to load previous instance
    # First return value if user typed reasonable input (quite if not)
    # Second is the actual value, possible are 'l' to load
    # and 'n' for new right now
    load = False
    status, var = mgr.check_savefile_status()
    if status:
        if var == "l":
            load = True
    else:
        return

    # Ask for message to be saved in the save file
    # Alwayas good to have some documentation ...
    mgr.ask_for_message()

    # short function to simplify the job setup
    def simple_job(name: str, exe: str) -> job:
        # define job and pass the HTCondor schedd to it
        j = job(name, mgr.schedd)

        # set the executable and the path to the log files
        j.set_simple(exe, cfg.dir+"/log/")

        # set the expected run time in seconds
        j.set_time(120)
        return j

    if load:
        mgr.load()
    else:
        # as a test, prepare one job which succeeds and one that fails
        j = simple_job("success", "util/echoS.sh")
        # use job::submit to submit the job. With manager, this is not necessary,
        # when job does not have and dependency it is submitted automatically!
        j.submit()
        mgr.add_job(j)
        depS = [j]

        j = simple_job("error", "util/echoE.sh")
        # here we let manager to handle the submition
        mgr.add_job(j)
        depE = [j]

        # add job depending on the two submitted job to demonstrate what happens if depending job fails or succeds
        j = simple_job("success_depS", "util/echoS.sh")
        j.add_job_dependency(*depS)
        mgr.add_job(j)

        j = simple_job("success_depE", "util/echoS.sh")
        j.add_job_dependency(*depE)
        mgr.add_job(j)

    # start the manager
    # if there is an error, especially interupt with keyboard,
    # saves the current state of jobs
    mgr.start(60, gui=False)  # argument is interval between checking of the jobs
    mgr.save()
    mgr.print_failed()


if __name__ == "__main__":
    main()
