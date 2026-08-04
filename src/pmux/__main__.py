#!/usr/bin/env python3

import argparse
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import logging
from falconry import chdir, cli, run_command_local
from datetime import datetime
import re
import pexpect
from textwrap import dedent

logging.basicConfig(level=logging.INFO, format="%(levelname)s (%(name)s): %(message)s")
log = logging.getLogger('persistmux')

# home directory
HOSTFILE_DIR = Path('~/.local/share/persistmux/').expanduser().resolve()
ORIGINAL_CWD = Path.cwd()
LOGFILE_DIR = ORIGINAL_CWD / ".persistmux"


@dataclass
class CommandResult:
    """Standardized return type for command execution."""

    returncode: int
    stdout: str
    stderr: str


def get_hostfile(job_id: str) -> Path:
    """Return path to job's hostfile as `{HOSTFILE_DIR}/{job_id}.host`
    HOSTFILE_DIR defaults to `~/.local/share/persistmux/`.
    Hostfile contains the address of the node where the tmux session is running.

    Arguments:
        job_id (str): job id
    Returns:
        Path: path to hostfile
    """
    # validate job_id
    if not re.match(r'^[A-Za-z0-9_-]+$', job_id):
        log.error(
            f"Invalid job id: {job_id}, must contain only letters, "
            "numbers, hyphens, and underscores"
        )
        raise ValueError
    return HOSTFILE_DIR / f"{job_id}.host"


def read_hostfile(job_id: str) -> Optional[str]:
    """Read hostfile and return the node address,
    or None if the file does not exist.

    Arguments:
        job_id (str): job id
    Returns:
        Optional[str]: node address
    """
    hostfile = get_hostfile(job_id)
    if not hostfile.exists():
        return None

    with open(hostfile) as f:
        lines = [line.strip() for line in f.readlines() if line.strip()]

    if len(lines) != 1:
        log.warning(f"Hostfile {hostfile} is malformed (expected 1 non-empty line)")
        return None

    node = lines[0]
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", node):
        log.warning(f"Hostfile {hostfile} contains invalid node value: {node}")
        return None

    return node


def write_hostfile(job_id: str, node: str) -> None:
    """Write hostfile with forced sync.

    Arguments:
        job_id (str): job id
        node (str): node address
    Raises:
        ValueError: If node is invalid
    """
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", node):
        raise ValueError(
            f"Invalid node value: {node}, must match [A-Za-z0-9][A-Za-z0-9._-]*"
        )

    hostfile = get_hostfile(job_id)
    with open(hostfile, "w") as f:
        f.write(node)
        f.flush()
        os.fsync(f.fileno())


def get_logfile(job_id: str) -> Path:
    """Return path to job's logfile as `{LOGFILE_DIR}/{job_id}.log`.
    LOGFILE_DIR defaults to `.persistmux` in the current working directory.
    Logfile contains stdout and stderr from the tmux session

    Arguments:
        job_id (str): job id
    Returns:
        Path: path to logfile
    """
    return LOGFILE_DIR / f"{job_id}.log"


def tmux_has_session(job_id: str, node: Optional[str] = None) -> bool:
    """Check if tmux session exists locally or on a remote node.

    Arguments:
        job_id (str): job id
        node (Optional[str]): node to check on, or None for local
    Returns:
        bool: True if session exists, False otherwise
    """
    if node and node != os.uname().nodename:
        result = run_command_local(
            ["ssh", node, "tmux", "has-session", "-t", job_id],
        )
        return result.returncode == 0
    else:
        result = run_command_local(
            ["tmux", "has-session", "-t", job_id],
        )
        return result.returncode == 0


def tmux_kill_session(job_id: str, node: Optional[str] = None) -> subprocess.CompletedProcess:
    """Kill tmux session locally or on a remote node.

    Arguments:
        job_id (str): job id
        node (Optional[str]): node to kill on, or None for local
    Returns:
        subprocess.CompletedProcess: result of the command
    """
    log.warning(f"Killing session {job_id}" + (f" on {node}" if node else " locally"))
    if node and node != os.uname().nodename:
        return run_command_local(["ssh", node, "tmux", "kill-session", "-t", job_id])
    else:
        return run_command_local(["tmux", "kill-session", "-t", job_id])


def cleanup_session(job_id: str) -> bool:
    """Remove all artifacts for a job: tmux session, hostfile, and logfile.

    Returns:
        bool: True if cleanup succeeded or no session existed, False if kill failed.
    """
    node = read_hostfile(job_id)
    session_exists = tmux_has_session(job_id, node)

    if session_exists:
        result = tmux_kill_session(job_id, node)
        if result.returncode != 0:
            log.error(f"Failed to kill session {job_id} on {node or 'local'}: {result.stderr}")
            return False

    get_hostfile(job_id).unlink(missing_ok=True)
    get_logfile(job_id).unlink(missing_ok=True)
    return True


def cleanup_failed_start(job_id: str) -> None:
    """Best-effort cleanup for a partially initialized tmux session."""
    cleanup_session(job_id)


def attach_to_session(job_id: str, node: Optional[str]) -> CommandResult:
    """Attach to tmux session, optionally via SSH.

    Arguments:
        job_id (str): job id
        node (str): node address
    Returns:
        CommandResult: result of the command
    """
    tmux_cmd = ["tmux", "attach", "-t", job_id]
    local_node = os.uname().nodename
    if node and node != local_node:
        log.info(f"Connecting to {node} and attaching...")
        # Check if session exists on remote node first
        if not tmux_has_session(job_id, node):
            log.error(f"Session {job_id} not found on {node}.")
            return CommandResult(1, "", f"Session {job_id} not found on {node}")

        child = pexpect.spawn(
            "ssh",
            ["-tt", node, *tmux_cmd],
            encoding='utf-8',
            codec_errors='replace',
        )
        child.expect(".*")
        child.interact()
        stdout = (
            child.before if isinstance(child.before, str) else str(child.before or "")
        )
        stderr = child.after if isinstance(child.after, str) else str(child.after or "")
        if child.exitstatus is not None:
            statuscode = child.exitstatus
        elif child.signalstatus is not None:
            statuscode = 128 + child.signalstatus
        else:
            statuscode = 0  # Assume success if session existed and we attached
        return CommandResult(statuscode, stdout, stderr)
    else:
        log.info("Attaching locally...")
        if not tmux_has_session(job_id):
            log.error(f"Session {job_id} not found on {local_node}.")
            # remove stale hostfile if session does not exist anymore
            if get_hostfile(job_id).exists():
                log.warning(f"Removing stale hostfile for {job_id}")
                get_hostfile(job_id).unlink()
            return CommandResult(1, "", f"Session {job_id} not found on {local_node}")

        result = subprocess.run(tmux_cmd)
        return CommandResult(result.returncode, "", "")


def start_session(
    job_id: str, command: Optional[str] = None, verbose: bool = False
) -> bool:
    """Start new tmux session with command, preserving environment.
    The session runs in the shell tmux inherited, sets a trap to clean up
    the hostfile on exit, and closes automatically when the command exits
    (remain-on-exit is set to off).

    Arguments:
        job_id (str): job id
        command (Optional[str], optional): command to run. Defaults to None.
        verbose (bool, optional): verbose mode, submits -v to tmux. Defaults to False.
    Returns:
        bool: True if session started successfully, False otherwise
    """
    node = os.uname().nodename

    cmd = ["tmux", "new-session", "-d", "-s", job_id, "-c", os.getcwd()]
    if verbose:
        cmd.insert(1, "-v")

    if command:
        hostfile_quoted = shlex.quote(str(get_hostfile(job_id)))
        # Set trap to remove hostfile on exit, then run command, then exit
        # Uses the shell tmux inherited (preserves current shell)
        trap_command = f'trap "rm -f {hostfile_quoted}" EXIT HUP INT TERM'
        wrapped_command = f"{trap_command}; {command}; exit"
    else:
        log.error("No command provided for a new session")
        return False

    # Start session (inherits parent environment by default)
    with chdir(HOSTFILE_DIR):
        result = run_command_local(cmd)
        if result.returncode != 0:
            log.error(f"Failed to start tmux session: {result.stderr}")
            return False

        send_cmd = ["tmux", "send-keys", "-t", job_id, wrapped_command, "C-m"]
        result = run_command_local(send_cmd)
        if result.returncode != 0:
            log.error(f"Failed to dispatch command to tmux session: {result.stderr}")
            cleanup_failed_start(job_id)
            return False

        LOGFILE_DIR.mkdir(exist_ok=True)
        with open(get_logfile(job_id), "a") as f:
            f.write(
                f"#\n# Session {job_id} started on {node} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n#\n"
            )

        logfile_quoted = shlex.quote(str(get_logfile(job_id)))
        pipe_cmd = ["tmux", "pipe-pane", "-t", job_id, f"cat >> {logfile_quoted}"]
        result = run_command_local(pipe_cmd)
        if result.returncode != 0:
            log.error(f"Failed to enable tmux logging: {result.stderr}")
            cleanup_failed_start(job_id)
            return False

        # Set remain-on-exit off so session closes when shell exits
        remain_on_exit_cmd = [
            "tmux",
            "set-option",
            "-t",
            job_id,
            "remain-on-exit",
            "off",
        ]
        result = run_command_local(remain_on_exit_cmd)
        if result.returncode != 0:
            log.warning(f"Failed to set remain-on-exit: {result.stderr}")

    # Write hostfile
    write_hostfile(job_id, node)

    log.info(f"Started {job_id} on {node}")
    if command:
        log.info(f"Command: {command}")
    return True


def handle_lxplus_warning() -> None:
    """Show lxplus persistent tmux warning and prompt user to disable it."""
    log.warning(
        "Since you are on lxplus, make sure you have persistent "
        "tmux sessions enabled, see "
        "https://cern.service-now.com/service-portal?id=kb_article&n=KB0008111."
    )
    status, var = cli.input_checker(
        {'y': 'acknowledged, do not show again', 'n': 'please remind me again'},
        message="Do you want to disable this warning?",
    )
    if status == cli.InputState.SUCCESS and var == 'y':
        (HOSTFILE_DIR / '.lxplus_confirmed').touch()


def handle_new_session(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> None:
    """Handle starting a new session and optionally attaching to it."""
    command = " ".join(args.command)
    if not command:
        parser.error(
            "command is required when starting a new session; "
            "omit command only when attaching to an existing session"
        )

    if (
        'cern.ch' in os.uname().nodename
        and not (HOSTFILE_DIR / '.lxplus_confirmed').exists()
    ):
        handle_lxplus_warning()

    if not start_session(args.job_id, command, args.verbose):
        sys.exit(1)

    if args.attach:
        result = attach_to_session(args.job_id, os.uname().nodename)
        log.info(f"Finished with an exit code of {result.returncode}")
        if result.returncode != 0:
            log.error(f"Error: {result.stderr}")
        log.info(f"You can find the tmux log file at {get_logfile(args.job_id)}")
        sys.exit(result.returncode)

    log.info(f"Session {args.job_id} started successfully")
    sys.exit(0)


def handle_existing_session(args: argparse.Namespace) -> None:
    """Handle attaching to an existing session or just showing info."""
    # Auto-attach if no command provided
    if not args.attach and not args.command:
        args.attach = True

    if not args.attach:
        log.info(
            f"Session {args.job_id} already exists "
            f"(on {read_hostfile(args.job_id) or 'unknown node'}). "
            "Use -a/--attach to attach."
        )
        log.info(f"You can find the tmux log file at {get_logfile(args.job_id)}")
        sys.exit(0)

    node = read_hostfile(args.job_id)
    if not node:
        log.error(f"Hostfile exists but unreadable for {args.job_id}")
        sys.exit(1)

    result = attach_to_session(args.job_id, node)
    log.info(f"Finished with an exit code of {result.returncode}")
    if result.returncode != 0:
        log.error(f"Error: {result.stderr}")
    log.info(f"You can find the tmux log file at {get_logfile(args.job_id)}")
    sys.exit(result.returncode)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=dedent(
            f"""
        pmux: Persistent tmux job tracker across cluster nodes.

        Based on the provided job id, pmux will either start a new session
        locally or attach to an existing one, optionally via SSH.
        To achieve this, pmux will write a hostfile to {HOSTFILE_DIR}.

        In addition, pmux will write a logfile to {LOGFILE_DIR},
        containing stdout and stderr from the tmux session.

        The command should be ideally submitted after `--` to
        avoid capturing pmux's own arguments. If your command
        requires quotes, you need to escape them (or use '"XYZ"').

        Example:

            pmux test -- falconry -s test '"echo first job; echo second job"'

        Use -a/--attach to attach to an existing session.

        If you are on lxplus make sure you have persistent tmux
        enabled:
        https://cern.service-now.com/service-portal?id=kb_article&n=KB0008111
        """
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "-f",
        "--force-start",
        action="store_true",
        help="Force start new session (overwrite existing)",
    )
    parser.add_argument(
        "-a",
        "--attach",
        action="store_true",
        help="Attach to the session after starting or if it already exists",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose mode")
    parser.add_argument("job_id", help="Unique job identifier")
    parser.add_argument(
        "command",
        type=str,
        nargs=argparse.REMAINDER,
        help="Command to run (use -- to separate pmux args from command)",
    )

    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)

    HOSTFILE_DIR.mkdir(parents=True, exist_ok=True)
    host_exists = os.path.exists(get_hostfile(args.job_id))

    if args.force_start:
        if not cleanup_session(args.job_id):
            log.error(f"Cannot force start {args.job_id}: failed to kill existing session")
            sys.exit(1)
        host_exists = False

    if host_exists:
        handle_existing_session(args)
    else:
        handle_new_session(args, parser)


if __name__ == "__main__":
    main()
