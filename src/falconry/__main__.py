#!/usr/bin/env python
import logging
from .manager import manager, Mode
from .job import job
from .quick_job import quick_job
from .schedd_wrapper import kerberos_auth
import os
import argparse

logging.basicConfig(level=logging.INFO, format="%(levelname)s (%(name)s): %(message)s")
log = logging.getLogger('falconry')


def config() -> argparse.ArgumentParser:
    """Get configuration from cli arguments"""

    parser = argparse.ArgumentParser(
        description="Falconry executable,"
        "which allows to run set of commands on HTCondor within the current "
        "environment (using the htcondor `getenv` option, see:"
        "https://htcondor.readthedocs.io/en/latest/users-manual/env-of-job.html#environment-variables)."
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
    parser.add_argument(
        '--remote',
        action='store_true',
        help='Skips directly to loading and disables user interface, not generally recommended and mostly indended for internal use.',
    )
    parser.add_argument(
        '--tmux',
        action='store_true',
        help='Submits separate session in tmux which is responsible for submitting '
            'and monitoring jobs. Local instance only prints the status of jobs '
            'and processes user input.',
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


class Block:
    """Holds a block of commands and handles adding them to the manager.

    This is important to handle dependencies between blocks
    """

    def __init__(self) -> None:
        self.commands: dict[str, job] = {}
        self._lock: bool = False  # No more commands can be added
        self.dependencies: list[job] = []

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
        self.commands[name] = quick_job(name, command, mgr.schedd, mgr.dir + '/log', time, ncpu)
        mgr.add_job(self.commands[name])
        log.info(f'Added command `{command}` to falconry')

    def lock(self) -> None:
        """Lock block, no more commands can be added"""
        self._lock = True
        for j in self.commands.values():
            j.add_job_dependency(*self.dependencies)

    def add_dependency(self, dependency: 'Block') -> None:
        """Add dependency between blocks, also locks current block.

        Args:
            dependency (Block): block to add as dependency
        """
        self.dependencies.extend(dependency.commands.values())

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
    current_block.lock()

def main() -> None:
    """Main function for `falconry`"""

    kerberos_auth()
    log.info('Setting up `falconry` to run your commands')
    cfg = config().parse_args()
    condor_dir = os.path.join(cfg.dir, cfg.subdir)
    mode = Mode.NORMAL if not cfg.tmux else Mode.LOCAL
    if cfg.remote:
        mode = Mode.REMOTE
    mgr = manager(condor_dir, mode=mode)  # the argument specifies where the job is saved


    if cfg.verbose:
        log.setLevel(logging.DEBUG)
        logging.getLogger('falconry').setLevel(logging.DEBUG)

    if cfg.remote:
        if cfg.remote:
            log.addHandler(logging.FileHandler(os.path.join(condor_dir, 'falconry.remote.log')))
        else:
            log.addHandler(logging.FileHandler(os.path.join(condor_dir, 'falconry.log')))
        mgr.load()
    else:
        # Check if to run previous instance
        load = False
        status, var = mgr.check_savefile_status()
        if cfg.remote:
            log.addHandler(logging.FileHandler(os.path.join(condor_dir, 'falconry.remote.log')))
        else:
            log.addHandler(logging.FileHandler(os.path.join(condor_dir, 'falconry.log')))

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
