#!/usr/bin/env python
import logging
from .manager import manager
from .job import job
from .quick_job import quick_job
from .schedd_wrapper import kerberos_auth
import os
import argparse
import re

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
        'to run in paralel, blocks separated by ;; (cli) or empty line (file) '
        'are assumed to depend on previous block of commands. One can also '
        'define names for individual commands by prefixing them with their name '
        'in square brackets, for example `[name] command.',
    )
    parser.add_argument(
        '--custom-options',
        type=str,
        default='',
        help='Custom condor options to specify for the jobs, a comma separated list',
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
        help='Set time limit for jobs in seconds. Default is 3 hours.',
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


def parse_custom_options(custom_options_str: str) -> dict:
    """Parse comma-separated custom options string into a dictionary.

    Arguments:
        custom_options_str (str): comma-separated string of custom options in format 'key=value,key2=value2'

    Returns:
        dict: dictionary of custom options
    """
    custom_options: dict[str, str] = {}
    if not custom_options_str:
        return custom_options

    for pair in custom_options_str.split(','):
        pair = pair.strip()
        if not pair:
            continue
        if '=' in pair:
            key, value = pair.split('=', 1)
            custom_options[key.strip()] = value.strip()
        else:
            raise ValueError(f"Custom option needs to be of form key=value, is {pair}")

    return custom_options


def get_name(command: str) -> str:
    """Get sanitized name from command by replacing various symbols with `_`.

    Handles edge cases like empty strings and enforces length limits.

    Arguments:
        command (str): command to get name from
    Returns:
        str: sanitized name of the job for given command
    """
    if not command or not command.strip():
        raise ValueError("Command cannot be empty")

    strings_to_replace = [
        '--',
        ' ',
        '.',
        '/',
        '-',
        '`',
        '(',
        ')',
        '$',
        '"',
        "'",
        "\\",
        '&&',
        '||',
        '|',
        '>',
        '<',
        ';',
        '&',
        ':',
        '=',
    ]
    for string in strings_to_replace:
        command = command.replace(string, '_')

    # remove multiple _ and strip leading/trailing underscores
    name = '_'.join([x for x in command.split('_') if x != '']).strip('_')

    if not name:
        raise ValueError("Job name empty after sanitization")

    # Limit length to prevent filesystem issues
    max_length = 100
    if len(name) > max_length:
        name = name[:max_length] + "_trunc"

    return name


def parse_job(line: str) -> tuple[str, str]:
    """
    Parse '[NAME] COMMAND' format.
    Returns (name, command). Name is generated from command if no brackets or if name is empty.

    Arguments:
        line (str): line to parse

    Returns:
        tuple[str, str]: (name, command)

    Raises:
        ValueError: if line is empty or command is missing after name
    """
    if not line or not line.strip():
        raise ValueError("Empty line cannot be parsed as a job")

    pattern = r'^\[([^$$]*)\]\s*(.*)$'
    match = re.match(pattern, line.strip())

    if match:
        name = match.group(1).strip()
        command = match.group(2).strip()
        if command == '':
            raise ValueError(f"No command found for {line}")
        if not name:
            name = get_name(command)
        return name, command

    # No valid bracket syntax - entire line is command
    stripped_line = line.strip()
    return get_name(stripped_line), stripped_line


class Block:
    """Holds a block of commands and handles adding them to the manager.

    This is important to handle dependencies between blocks
    """

    def __init__(self, time: int, ncpu: int, custom_options: dict) -> None:
        self.commands: dict[str, job] = {}
        self._lock: bool = False  # No more commands can be added
        self.dependencies: list[job] = []
        self.time = time
        self.ncpu = ncpu
        self.custom_options = custom_options

    def add_command(self, command: str, mgr: manager) -> None:
        """Add command to block and manager.

        Args:
            command (str): command to add
            mgr (manager): HTCondor manager
        Raises:
            AttributeError: if block is locked
            AttributeError: if command is not valid
            AttributeError: if block already has the same command
        """
        if self._lock:
            log.error('Cannot add commands to locked block')
            raise AttributeError
        name, command = parse_job(command)
        if name in self.commands:
            log.error(f'Block {name} already has command {self.commands[name]}')
            raise AttributeError
        self.commands[name] = quick_job(
            name, command, mgr.schedd, mgr.dir + '/log', self.time, self.ncpu, self.custom_options
        )
        mgr.add_job(self.commands[name])
        log.info(f'Added command `{command}` to falconry under name `{name}`')

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


def process_commands(commands: str, mgr: manager, time: int, ncpu: int, custom_options: dict) -> None:
    """Process commands and add them to the manager"""

    # First check if we are dealing with file
    if os.path.isfile(commands):
        log.info(f'Processing commands from file {commands}')
        try:
            with open(commands) as f:
                lines = f.readlines()
        except IOError as e:
            log.error(f'Failed to read commands file {commands}: {e}')
            raise
    else:
        log.info(f'Processing commands string `{commands}`')
        lines = commands.split(';')
    log.debug(lines)

    previous_block = None
    current_block = Block(time, ncpu, custom_options)
    for line in lines:
        line = line.strip()
        if line.startswith('#'):
            continue
        if line == "":
            if current_block.empty:
                continue
            previous_block = current_block
            previous_block.lock()
            current_block = Block(time, ncpu, custom_options)
            # Automatically depends on the previous block
            current_block.add_dependency(previous_block)
            continue
        command = line.strip()
        # remove extra spaces
        command = ' '.join(command.split())

        current_block.add_command(command, mgr)
    current_block.lock()


def main() -> None:
    """Main function for `falconry`"""

    kerberos_auth()
    log.info('Setting up `falconry` to run your commands')
    cfg = config().parse_args()
    condor_dir = os.path.join(cfg.dir, cfg.subdir)
    mgr = manager(condor_dir)  # the argument specifies where the job is saved

    if cfg.verbose:
        log.setLevel(logging.DEBUG)
        logging.getLogger('falconry').setLevel(logging.DEBUG)

    # Check if to run previous instance
    load = False
    status, var = mgr.check_savefile_status()

    if status is True:
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
        custom_options = parse_custom_options(cfg.custom_options)
        process_commands(cfg.commands, mgr, cfg.set_time, cfg.ncpu, custom_options)
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
