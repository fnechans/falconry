#!/usr/bin/env python
import logging
from .manager import manager
from .job import job
import os
import htcondor
import argparse

logging.basicConfig(level=logging.INFO, format="%(levelname)s (%(name)s): %(message)s")
log = logging.getLogger('falconry')


def config() -> argparse.ArgumentParser:
    """Get configuration from cli arguments"""

    parser = argparse.ArgumentParser(
        description="Falconry executable,"
        "which allows to run set of commands on HTCondor within the current "
        "environment."
    )
    parser.add_argument('--dry', action='store_true', help='Dry run')
    parser.add_argument(
        '--dir',
        type=str,
        default='condor_output',
        help='Output directory for falconry, `condor_output` by default.',
    )
    parser.add_argument(
        '-s',
        '--subdir',
        type=str,
        default='',
        help='Output sub-directory for falconry, empty by default ',
    )
    parser.add_argument(
        'commands',
        type=str,
        help='Commands to run. Can be '
        'either be specified directly in the cli one can specify link to '
        'file with multiple commands. In cli, commands separated by ;, in file '
        'by a new line. Commands grouped together are assumed '
        'to run in paralel, blocks separated by ll (cli) or empty line (file) '
        'are assumed to depend on previous block of commands.',
    )
    parser.add_argument(
        '--retry-failed',
        action='store_true',
        help='Retry failed jobs from previous run',
    )
    parser.add_argument(
        '--set-time',
        '-t',
        type=int,
        default=3 * 60 * 60,
        help='Set time limit for jobs',
    )
    parser.add_argument(
        '-v', '--verbose', help='Print extra info.', default=False, action='store_true'
    )
    parser.add_argument(
        '--ncpu',
        type=int,
        default=1,
        help='Number of cpus to request. Default is 1',
    )
    return parser


def get_name(command: str) -> str:
    """Get name from command by replacing various symbols with `_`.

    Arguments:
        command (str): command to get name from
    Returns:
        str: name of the job for given command
    """
    strings_to_replace = ['--', ' ', '.', '/', '-', '`', '(', ')', '$', '"', "'", "\\"]
    for string in strings_to_replace:
        command = command.replace(string, '_')

    # remove multiple _
    return '_'.join([x for x in command.split('_') if x != ''])


def create_job(name: str, command: str, mgr: manager, time: int, ncpu: int = 1) -> job:
    """Create job for given command.

    Arguments:
        name (str): name of the job
        command (str): command to run
        mgr (manager): HTCondor manager
        time (int): expected runtime
    Returns:
        job: created job
    """
    # define job and pass the HTCondor schedd to it
    j = job(name, mgr.schedd)

    # set the executable and the path to the log files
    main_path = os.path.dirname(os.path.abspath(__file__))
    executable = os.path.join(main_path, 'run_simple.sh')
    if not os.path.isfile(executable):
        log.error(f'Failed to find executable {executable}')
        raise FileNotFoundError
    j.set_simple(
        executable,
        mgr.dir + "/log/",
    )

    is_desy = "desy.de" in str(mgr.schedd.schedd.location)

    # expected runtime
    j.set_time(time, useRequestRuntime=is_desy)

    # set the command
    j.set_arguments(f'{name} {command}')

    # and environenment
    basedir = os.path.abspath('.')
    env = f'basedir={basedir};'

    condor_options = {'environment': env, 'getenv': 'True'}
    # Some cluster specific settings which might break submission on other clusters
    if "cern.ch" in str(mgr.schedd.schedd.location):
        condor_options["MY.SendCredential"] = "True"
    elif is_desy:
        condor_options["MY.SendCredential"] = "True"
        condor_options["Requirements"] = '(OpSysAndVer == "RedHat9")'

    if ncpu > 1:
        condor_options['RequestCpus'] = str(ncpu)
    j.set_custom(condor_options)

    return j


class Block:
    """Holds a block of commands and handles adding them to the manager.

    This is important to handle dependencies between blocks
    """

    def __init__(self) -> None:
        self.commands: dict[str, job] = {}
        self._lock: bool = False  # No more commands can be added

    def add_command(self, command: str, mgr: manager, time: int, ncpu: int = 1) -> None:
        """Add command to block and manager.

        Args:
            command (str): command to add
            mgr (manager): HTCondor manager
            time (int): expected runtime
        Raises:
            AttributeError: if block is locked
            AttributeError: if command is not valid
            AttributeError: if block already has the same command
        """
        if self._lock:
            log.error('Cannot add commands to locked block')
            raise AttributeError
        name = get_name(command)
        if name in self.commands:
            log.error(f'Block {name} already has command {self.commands[name]}')
            raise AttributeError
        self.commands[name] = create_job(name, command, mgr, time, ncpu)
        mgr.add_job(self.commands[name])
        log.info(f'Added command `{command}` to falconry')

    def lock(self) -> None:
        """Lock block, no more commands can be added"""
        self._lock = True

    def add_dependency(self, dependency: 'Block') -> None:
        """Add dependency between blocks, also locks current block.

        Args:
            dependency (Block): block to add as dependency
        """
        for j in self.commands.values():
            j.add_job_dependency(*dependency.commands.values())

    @property
    def empty(self) -> bool:
        """Check if block is empty"""
        return len(self.commands) == 0


def process_commands(commands: str, mgr: manager, time: int, ncpu: int = 1) -> None:
    """Process commands and add them to the manager"""

    # First check if we are dealing with file
    if os.path.isfile(commands):
        log.info(f'Processing commands from file {commands}')
        with open(commands) as f:
            lines = f.readlines()
    else:
        log.info(f'Processing commands string `{commands}`')
        lines = commands.split(';')
    log.debug(lines)

    previous_block = None
    current_block = Block()
    for line in lines:
        line = line.strip()
        if line.startswith('#'):
            continue
        if line == "":
            if current_block.empty:
                continue
            previous_block = current_block
            previous_block.lock()
            current_block = Block()
            # Automatically depends on the previous block
            current_block.add_dependency(previous_block)
            continue
        command = line.strip()
        # remove extra spaces
        command = ' '.join(command.split())

        current_block.add_command(command, mgr, time, ncpu)


def main() -> None:
    """Main function for `falconry`"""

    try:
        credd = htcondor.Credd()
        credd.add_user_cred(htcondor.CredTypes.Kerberos, None)
    except:
        log.warning(
            "Kerberos creds not available. This can cause problems on some clusters (like lxplus)."
        )

    log.info('Setting up `falconry` to run your commands')
    cfg = config().parse_args()
    tpcondor_dir = os.path.join(cfg.dir, cfg.subdir)
    mgr = manager(tpcondor_dir)  # the argument specifies where the job is saved

    if cfg.verbose:
        log.setLevel(logging.DEBUG)
        logging.getLogger('falconry').setLevel(logging.DEBUG)

    # Check if to run previous instance
    load = False
    status, var = mgr.check_savefile_status()

    if status == True:
        if var == "l":
            load = True
    else:
        return

    # Ask for message to be saved in the save file
    # Alwayas good to have some documentation ...
    mgr.ask_for_message()

    if load:
        mgr.load(cfg.retry_failed)
    else:
        process_commands(cfg.commands, mgr, cfg.set_time, cfg.ncpu)
    if cfg.dry:
        return
    # start the manager
    # if there is an error, especially interupt with keyboard,
    # saves the current state of jobs
    mgr.start(60, gui=False)  # argument is interval between checking of the jobs
    mgr.save()
    mgr.print_failed()


if __name__ == '__main__':
    main()
