from .job import job
from .schedd_wrapper import ScheddWrapper
import os
import re
import logging

log = logging.getLogger('falconry')


def quick_job(
    name: str,
    command: str,
    schedd: ScheddWrapper,
    logDir: str,
    time: int,
    ncpu: int,
    custom_options: dict,
) -> job:
    """Create job for given command.

    Arguments:
        name (str): name of the job
        command (str): command to run
        schedd (ScheddWrapper): scheduller to use for the job
        logDir (str): directory to use for log files
        time (int): expected runtime
        ncpu (int): number of cores to request
        custom_options (dict) custom options for the job
    Returns:
        job: created job
    """
    # define job and pass the HTCondor schedd to it
    j = job(name, schedd)

    # set the executable and the path to the log files
    main_path = os.path.dirname(os.path.abspath(__file__))
    executable = os.path.join(main_path, 'run_simple.sh')
    if not os.path.isfile(executable):
        log.error(f'Failed to find executable {executable}')
        raise FileNotFoundError
    j.set_simple(
        executable,
        logDir,
    )

    is_desy = "desy.de" in schedd.location

    # expected runtime
    j.set_time(time, useRequestRuntime=is_desy)

    # set the command
    j.set_arguments(f'{name} {command}')

    # and environenment
    basedir = os.path.abspath('.')
    env = f'basedir={basedir};'

    condor_options = {'environment': env, 'getenv': 'True'}
    # Cluster-specific settings - these may need updating if cluster configurations change
    # Note: These settings are automatically applied based on the schedd location
    schedd_loc = str(schedd.location)
    if "cern.ch" in schedd_loc:
        condor_options["MY.SendCredential"] = "True"
    elif is_desy:
        condor_options["MY.SendCredential"] = "True"
        condor_options["Requirements"] = '(OpSysAndVer == "RedHat9")'
    if 'particle.cz' in schedd_loc:
        home = os.getenv("HOME")
        if home is not None:
            condor_options["x509userproxy"] = home + "/x509up_u{0}".format(os.geteuid())

        match = re.search(r"^/mnt/nfs(\d+)", os.getcwd())
        if match:
            nfs_num = match.group(1)
            condor_options[f"MY.nfs{nfs_num}"] = "True"

    if ncpu > 1:
        condor_options['RequestCpus'] = str(ncpu)
    condor_options.update(**custom_options)
    j.set_custom(condor_options)

    return j
