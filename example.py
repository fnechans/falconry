#!/usr/bin/env python3

import logging
import sys
import traceback
from falconry import manager, job

logging.basicConfig(
    level=logging.INFO, format="--- %(levelname)s (%(name)s): %(message)s"
)
log = logging.getLogger(__name__)


def config():
    import argparse
    parser = argparse.ArgumentParser(description="Falconry. Read README!")
    parser.add_argument("--cont", action="store_true", help="Load jobs from previous iteration and continue")
    parser.add_argument("--dir", type=str, help="Path to output DIR. Output by default.", default="Output")
    return parser.parse_args()


def main():

    cfg = config()
    mgr = manager(cfg.dir)

    def simple_job(name: str, exe: str) -> job:
        j = job(name, mgr.schedd)
        j.set_simple(exe, cfg.dir+"/log/")
        j.set_time(120)
        return j

    if cfg.cont:
        mgr.load()
    else:
        j = simple_job("success", "util/echoS.sh")
        j.submit()
        mgr.add_job(j)
        depS = [j]

        j = simple_job("error", "util/echoE.sh")
        j.submit()
        mgr.add_job(j)
        depE = [j]

        j = simple_job("success_depS", "util/echoS.sh")
        j.add_job_dependency(depS)
        mgr.add_job(j)

        j = simple_job("success_depE", "util/echoS.sh")
        j.add_job_dependency(depE)
        mgr.add_job(j)

    try:
        mgr.start()
    except KeyboardInterrupt:
        log.error("Manager interrupted with keyboard!")
        log.error("Saving and exitting ...")
    except Exception:
        traceback.print_exc(file=sys.stdout)
    mgr.save()
    sys.exit(0)


if __name__ == "__main__":
    main()
