#!/usr/bin/env python

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional
import logging
from falconry import chdir, cli, run_command_local
from datetime import datetime
import re

logging.basicConfig(level=logging.INFO, format="%(levelname)s (%(name)s): %(message)s")
log = logging.getLogger('persistmux')

# home directory
HOSTFILE_DIR = Path('~/.local/share/persistmux/').expanduser().resolve()
LOGFILE_DIR = Path('.persistmux').resolve()


def get_hostfile(job_id: str) -> Path:
    f"""Return path to job's hostfile as {HOSTFILE_DIR}/{job_id}.host

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
        lines = f.readlines()
        assert len(lines) == 1
        node = lines[0].strip() if lines else None
    return node


def write_hostfile(job_id: str, node: str) -> None:
    """Write hostfile with forced sync.

    Arguments:
        job_id (str): job id
        node (str): node address
    """
    hostfile = get_hostfile(job_id)
    with open(hostfile, "w") as f:
        f.write(node)
        f.flush()
        os.fsync(f.fileno())


def get_logfile(job_id: str) -> Path:
    f"""Return path to job's logfile as {LOGFILE_DIR}/{job_id}.log
    Logfile contains stdout and stderr from the tmux session

    Arguments:
        job_id (str): job id
    Returns:
        Path: path to logfile
    """
    return LOGFILE_DIR / f"{job_id}.log"


def tmux_has_session(job_id: str) -> bool:
    """Check if tmux session exists locally.

    Arguments:
        job_id (str): job id
    Returns:
        bool: True if session exists, False otherwise
    """
    result = run_command_local(
        ["tmux", "has-session", "-t", job_id],
    )
    return result.returncode == 0


def tmux_kill_session(job_id: str) -> subprocess.CompletedProcess:
    """Kill tmux session.

    Arguments:
        job_id (str): job id
    Returns:
        subprocess.CompletedProcess: result of the command
    """
    log.warning(f"Killing session {job_id}")
    return run_command_local(["tmux", "kill-session", "-t", job_id])


def attach_to_session(job_id: str, node: Optional[str]) -> subprocess.CompletedProcess:
    """Attach to tmux session, optionally via SSH.

    Arguments:
        job_id (str): job id
        node (Optional[str]): node address
    Returns:
        subprocess.CompletedProcess: result of the command
    """
    tmux_cmd = ["tmux", "attach", "-t", job_id]
    if node and node != os.uname().nodename:
        log.info(f"Connecting to {node} and attaching...")
        return run_command_local(["ssh", "-t", node, f'{" ".join(tmux_cmd)}'])
    else:
        log.info("Attaching locally...")
        return run_command_local(tmux_cmd)


def start_session(
    job_id: str, command: Optional[str] = None, verbose: bool = False
) -> bool:
    """Start new tmux session with command, preserving environment.

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
        cmd.extend([f'{command}; rm {get_hostfile(job_id)}'])
    else:
        raise Exception('Running without command not supported yet')

    # Start session (inherits parent environment by default)
    with chdir(HOSTFILE_DIR):
        result = run_command_local(cmd)
        if result.returncode != 0:
            log.error(f"Failed to start tmux session: {result.stderr.decode()}")
            return False

    # Write hostfile
    write_hostfile(job_id, node)

    # TODO: This dones not work ...
    # cmd = ["tmux", "set-hook", "-t", job_id, "session-closed",
    #        f'run-shell "rm {get_hostfile(job_id)}; echo Job test cleaned up"']
    # cmd = ["tmux", "set-hook", "-t", job_id, "client-detached", f'display-message "Job {job_id} cleaned up"']
    # result = run_command_local(cmd)
    # if result.returncode != 0:
    #     log.warning(f"Failed to set tmux hook: {result.stderr.decode()}. "
    #              f"Will continue but hostfile {get_hostfile(job_id)} will not be removed.")
    #     return True

    log.info(f"Started {job_id} on {node}")
    if command:
        log.info(f"Command: {command}")

    LOGFILE_DIR.mkdir(exist_ok=True)
    with open(get_logfile(job_id), "a") as f:
        f.write(
            f"#\n# Session {job_id} started on {node} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n#\n"
        )
    subprocess.run(['tmux', 'pipe-pane', '-t', job_id, f'cat >> {get_logfile(job_id)}'])
    return True


def main() -> None:
    parser = argparse.ArgumentParser(
        description=f"""pmux: Persistent tmux job tracker across cluster nodes.

        Based on the provided job id, pmux will either start a new session
        locally or attach to an existing one, optionally via SSH.
        To achieve this, pmux will write a hostfile to {HOSTFILE_DIR}.

        In addition, pmux will write a logfile to {LOGFILE_DIR},
        containing stdout and stderr from the tmux session.

        The command should be ideally submitted after `--` to
        avoid capturing pmux's own arguments. If your command
        requires quotes, you need to escape them (or use '"XYZ"').

        If you are on lxplus
        """
    )
    parser.add_argument(
        "-f",
        "--force-start",
        action="store_true",
        help="Force start new session (overwrite existing)",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose mode")
    parser.add_argument("job_id", help="Unique job identifier")
    parser.add_argument(
        "command",
        type=str,
        nargs="*",
        # argparse.REMAINDER,
        help="Command to run",
    )

    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)

    HOSTFILE_DIR.mkdir(parents=True, exist_ok=True)
    host_exists = os.path.exists(get_hostfile(args.job_id))

    if args.force_start:
        if tmux_has_session(args.job_id):
            tmux_kill_session(args.job_id)
        if host_exists:
            os.remove(get_hostfile(args.job_id))
            host_exists = False

    command = " ".join(args.command)

    if not host_exists:

        if (
            'cern.ch' in os.uname().nodename
            and not (HOSTFILE_DIR / '.lxplus_confirmed').exists()
        ):
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

        if not start_session(args.job_id, command, args.verbose):
            sys.exit(1)
        result = attach_to_session(args.job_id, None)
    else:
        node = read_hostfile(args.job_id)
        if not node:
            log.info(f"Error: Hostfile exists but unreadable for {args.job_id}")
            sys.exit(1)

        if tmux_has_session(args.job_id):
            log.info("Session exists locally, attaching...")
            result = attach_to_session(args.job_id, None)
        else:
            log.info("Session not found locally, connecting via SSH...")
            result = attach_to_session(args.job_id, node)

    log.info(f"Finished with an exit code of {result.returncode}")
    if result.returncode != 0:
        log.error(f"Error: {result.stderr.decode()}")

    log.info(f"You can find the tmux log file at {get_logfile(args.job_id)}")

    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
