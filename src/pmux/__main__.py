#!/usr/bin/env python3

import argparse
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, cast
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

# Patterns and helpers
NODE_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


@dataclass
class CommandResult:
    """Standardized return type for command execution."""

    returncode: int
    stdout: str
    stderr: str


# Helper functions
def is_remote_node(node: Optional[str]) -> bool:
    """Check if the given node is remote (not the local host)."""
    return node is not None and node != os.uname().nodename


def remove_hostfile(job_id: str) -> None:
    """Remove hostfile for a job if it exists."""
    hostfile = get_hostfile(job_id)
    if hostfile.exists():
        log.warning(f"Removing stale hostfile for job {job_id}")
        hostfile.unlink()


def verify_session_or_return_error(
    job_id: str, node: Optional[str] = None
) -> Optional[CommandResult]:
    """Verify session exists on the given node (or locally if None).

    If verification fails or session doesn't exist, clean up stale hostfile
    and return an error CommandResult. Returns None if session is valid.

    Arguments:
        job_id: The job identifier
        node: The node to check, or None for local
    Returns:
        None if session exists and is verifiable
        CommandResult with error details if verification failed or session missing
    """
    node_desc = node or os.uname().nodename
    is_remote = node is not None
    session_exists = tmux_has_session(job_id, node)

    if session_exists is None:
        if is_remote:
            log.error(
                f"Cannot verify session {job_id} on {node_desc} (SSH/connection error)."
            )
            return CommandResult(
                1, "", f"Cannot connect to {node_desc} to verify session"
            )
        else:
            log.error(f"Cannot verify session {job_id} on {node_desc}.")
            return CommandResult(
                1, "", f"Cannot verify session {job_id} on {node_desc}"
            )

    if not session_exists:
        remove_hostfile(job_id)
        return CommandResult(
            1, "", f"Session {job_id} not found on {node_desc}, cleaned up hostfile. "
            "You can now start a new session."
        )

    return None


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
        raise ValueError(
            f"Invalid job id: {job_id}, must contain only letters, "
            "numbers, hyphens, and underscores"
        )
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

    try:
        with open(hostfile) as f:
            lines = [line.strip() for line in f.readlines() if line.strip()]
    except OSError as e:
        log.error(f"Failed to read hostfile {hostfile} for job {job_id}: {e}")
        return None

    if len(lines) != 1:
        log.warning(
            f"Hostfile {hostfile} for job {job_id} is malformed (expected 1 non-empty line)"
        )
        return None

    node = lines[0]
    if not NODE_PATTERN.fullmatch(node):
        log.warning(
            f"Hostfile {hostfile} for job {job_id} contains invalid node value: {node}"
        )
        return None

    return node


def write_hostfile(job_id: str, node: str) -> None:
    """Write hostfile with forced sync.

    Arguments:
        job_id (str): job id
        node (str): node address
    Raises:
        ValueError: If node is invalid
        OSError: If file write fails
    """
    if not NODE_PATTERN.fullmatch(node):
        raise ValueError(
            f"Invalid node value: {node}, must match [A-Za-z0-9][A-Za-z0-9._-]*"
        )

    hostfile = get_hostfile(job_id)
    try:
        with open(hostfile, "w") as f:
            f.write(node)
            f.flush()
            os.fsync(f.fileno())
    except OSError as e:
        raise OSError(f"Failed to write hostfile {hostfile} for job {job_id}: {e}")


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


def tmux_has_session(job_id: str, node: Optional[str] = None) -> Optional[bool]:
    """Check if tmux session exists locally or on a remote node.

    Arguments:
        job_id (str): job id
        node (Optional[str]): node to check on, or None for local
    Returns:
        Optional[bool]: True if session exists, False if it doesn't, None if check failed (e.g., SSH error)
    """
    if is_remote_node(node):
        result = run_command_local(
            ["ssh", cast(str, node), "tmux", "has-session", "-t", job_id],
        )
        if result.returncode != 0:
            # When running `ssh node tmux has-session -t job_id`:
            # - If SSH succeeds but session doesn't exist: tmux returns 1, stderr is empty or from tmux
            # - If SSH fails: ssh returns non-zero (255 for connection refused, etc.),
            #   stderr contains SSH error message (typically starts with "ssh:")
            # Use stderr to distinguish: SSH writes diagnostic messages there
            if result.stderr and "ssh:" in result.stderr.lower():
                log.warning(
                    f"SSH to {node} failed for job {job_id} with exit code {result.returncode}: {result.stderr.strip()}"
                )
                return None
            # Also treat high exit codes (>1) as SSH errors since tmux only returns 0 or 1
            if result.returncode > 1:
                log.warning(
                    f"SSH to {node} failed for job {job_id} with exit code {result.returncode}: {result.stderr.strip()}"
                )
                return None
        return result.returncode == 0
    else:
        result = run_command_local(
            ["tmux", "has-session", "-t", job_id],
        )
        return result.returncode == 0


def tmux_kill_session(
    job_id: str, node: Optional[str] = None
) -> subprocess.CompletedProcess:
    """Kill tmux session locally or on a remote node.

    Arguments:
        job_id (str): job id
        node (Optional[str]): node to kill on, or None for local
    Returns:
        subprocess.CompletedProcess: result of the command
    """
    log.warning(f"Killing session {job_id}" + (f" on {node}" if node else " locally"))
    if is_remote_node(node):
        return run_command_local(["ssh", cast(str, node), "tmux", "kill-session", "-t", job_id])
    else:
        return run_command_local(["tmux", "kill-session", "-t", job_id])


def cleanup_session(job_id: str) -> bool:
    """Remove all artifacts for a job: tmux session, hostfile, and logfile.

    Returns:
        bool: True if cleanup succeeded or no session existed, False if kill failed
              or session check failed (e.g., SSH error).
    """
    node = read_hostfile(job_id)
    session_exists = tmux_has_session(job_id, node)

    # If we couldn't determine session status (e.g., SSH failed), don't clean up
    if session_exists is None:
        node_desc = node or 'local'
        log.error(
            f"Cannot verify session {job_id} on {node_desc} (SSH/connection error). Keeping hostfile."
        )
        return False

    if session_exists:
        result = tmux_kill_session(job_id, node)
        if result.returncode != 0:
            node_desc = node or 'local'
            log.error(
                f"Failed to kill session {job_id} on {node_desc}: {result.stderr}"
            )
            return False

    get_hostfile(job_id).unlink(missing_ok=True)
    get_logfile(job_id).unlink(missing_ok=True)
    return True


def list_sessions(pattern: str = "*") -> list[tuple[str, str]]:
    """List existing sessions matching a glob pattern.

    Arguments:
        pattern (str): glob pattern to match session job_ids
    Returns:
        list[tuple[str, str]]: list of (job_id, node) tuples for matching sessions
    """
    import glob

    sessions = []
    for hostfile in glob.glob(str(HOSTFILE_DIR / f"{pattern}.host")):
        job_id = Path(hostfile).stem  # Remove .host extension
        node = read_hostfile(job_id)
        if node:
            sessions.append((job_id, node))
    return sessions


def attach_to_session(job_id: str, node: Optional[str]) -> CommandResult:
    """Attach to tmux session, optionally via SSH.

    Arguments:
        job_id (str): job id
        node (str): node address
    Returns:
        CommandResult: result of the command
    """
    tmux_cmd = ["tmux", "attach", "-t", job_id]
    if is_remote_node(node):
        log.info(f"Connecting to {node} and attaching...")
        error_result = verify_session_or_return_error(job_id, node)
        if error_result is not None:
            return error_result

        try:
            child = pexpect.spawn(
                "ssh",
                ["-tt", node, *tmux_cmd],
                encoding='utf-8',
                codec_errors='replace',
            )
        except pexpect.ExceptionPexpect as e:
            log.error(f"Failed to spawn SSH process for job {job_id} on {node}: {e}")
            return CommandResult(1, "", f"SSH connection failed: {e}")
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
        error_result = verify_session_or_return_error(job_id, None)
        if error_result is not None:
            return error_result

        result = subprocess.run(tmux_cmd)
        return CommandResult(result.returncode, "", "")


def setup_tmux_session(job_id: str, cwd: str, verbose: bool = False) -> bool:
    """Create a new tmux session in detached mode.

    Arguments:
        job_id (str): job id for the session
        cwd (str): working directory for the session
        verbose (bool): verbose mode for tmux
    Returns:
        bool: True if session created successfully
    """
    cmd = ["tmux", "new-session", "-d", "-s", job_id, "-c", cwd]
    if verbose:
        cmd.insert(1, "-v")
    result = run_command_local(cmd)
    if result.returncode != 0:
        log.error(f"Failed to start tmux session for job {job_id}: {result.stderr}")
        return False
    return True


def send_command_to_session(job_id: str, command: str) -> bool:
    """Send a command to an existing tmux session.

    Arguments:
        job_id (str): job id of the target session
        command (str): command to send
    Returns:
        bool: True if command sent successfully
    """
    send_cmd = ["tmux", "send-keys", "-t", job_id, command, "C-m"]
    result = run_command_local(send_cmd)
    if result.returncode != 0:
        log.error(
            f"Failed to dispatch command to tmux session for job {job_id}: {result.stderr}"
        )
        return False
    return True


def setup_tmux_logging(job_id: str, node: str) -> bool:
    """Set up tmux pipe-pane for logging to a file.

    Arguments:
        job_id (str): job id for the session
        node (str): node where session is running
    Returns:
        bool: True if logging setup succeeded
    """
    try:
        LOGFILE_DIR.mkdir(exist_ok=True)
    except OSError as e:
        log.error(
            f"Failed to create logfile directory {LOGFILE_DIR} for job {job_id}: {e}"
        )
        return False

    try:
        with open(get_logfile(job_id), "a") as f:
            f.write(
                f"#\n# Session {job_id} started on {node} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n#\n"
            )
    except OSError as e:
        log.error(f"Failed to write to logfile for job {job_id} on {node}: {e}")
        return False

    logfile_quoted = shlex.quote(str(get_logfile(job_id)))
    pipe_cmd = ["tmux", "pipe-pane", "-t", job_id, f"cat >> {logfile_quoted}"]
    result = run_command_local(pipe_cmd)
    if result.returncode != 0:
        log.error(
            f"Failed to enable tmux logging for job {job_id} on {node}: {result.stderr}"
        )
        return False
    return True


def setup_tmux_options(job_id: str, node: str) -> bool:
    """Configure tmux session options.

    Arguments:
        job_id (str): job id for the session
        node (str): node where session is running (for logging)
    Returns:
        bool: True if options set successfully (warning only on failure)
    """
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
        log.warning(
            f"Failed to set remain-on-exit for job {job_id} on {node}: {result.stderr}"
        )
        return False
    return True


def start_session(
    job_id: str, command: Optional[str] = None, verbose: bool = False
) -> bool:
    """Start new tmux session with command, preserving environment.
    The session runs in the shell tmux inherited and closes automatically when
    the command exits (remain-on-exit is set to off). Hostfile cleanup is done
    lazily when attach fails (session not found).

    Arguments:
        job_id (str): job id
        command (Optional[str], optional): command to run. Defaults to None.
        verbose (bool, optional): verbose mode, submits -v to tmux. Defaults to False.
    Returns:
        bool: True if session started successfully, False otherwise
    """
    node = os.uname().nodename

    if not command:
        log.error(f"No command provided for new session {job_id}")
        return False

    # No trap - using lazy cleanup (hostfile removed on attach failure)
    wrapped_command = f"{command}; exit"

    # Start session (inherits parent environment by default)
    cwd = os.getcwd()
    with chdir(HOSTFILE_DIR):
        if not setup_tmux_session(job_id, cwd, verbose):
            return False

        if not send_command_to_session(job_id, wrapped_command):
            cleanup_session(job_id)
            return False

        if not setup_tmux_logging(job_id, node):
            cleanup_session(job_id)
            return False

        if not setup_tmux_options(job_id, node):
            cleanup_session(job_id)
            return False

    # Write hostfile
    try:
        write_hostfile(job_id, node)
    except ValueError as e:
        log.error(f"Invalid node value for job {job_id}: {e}")
        cleanup_session(job_id)
        return False
    except OSError as e:
        log.error(f"Failed to write hostfile for job {job_id} on {node}: {e}")
        cleanup_session(job_id)
        return False

    log.info(f"Started {job_id} on {node}")
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
    command = shlex.join(args.command)
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
        log.error(f"Failed to start session {args.job_id}")
        sys.exit(1)

    log.info(f"You can find the tmux log file at {get_logfile(args.job_id)}")
    if args.attach:
        log.info(f"Attaching to session {args.job_id}")
        result = attach_to_session(args.job_id, os.uname().nodename)
        if result.returncode != 0:
            log.error(f"Finished with an exit code of {result.returncode}")
            log.error(f"Error: {result.stderr}")
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
        hostfile = get_hostfile(args.job_id)
        log.error(f"Hostfile exists but unreadable for job {args.job_id} at {hostfile}")
        sys.exit(1)

    result = attach_to_session(args.job_id, node)
    if result.returncode != 0:
        log.error(f"Finished with an exit code of {result.returncode}")
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
        Use -l/--list to list sessions (optionally with glob pattern).

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
    parser.add_argument(
        "-l",
        "--list",
        type=str,
        nargs="?",
        const="*",
        default=None,
        metavar="PATTERN",
        help="List sessions matching a glob pattern (default: all sessions)",
    )
    parser.add_argument("job_id", nargs="?", help="Unique job identifier")
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

    # Handle list command
    if args.list is not None:
        sessions = list_sessions(args.list)
        if not sessions:
            log.info(f"No sessions found matching pattern '{args.list}'")
            sys.exit(0)
        for job_id, node in sessions:
            print(f"{job_id} on {node}")
        sys.exit(0)

    if args.job_id is None:
        parser.error("job_id is required (unless using --list)")

    try:
        host_exists = os.path.exists(get_hostfile(args.job_id))
    except ValueError as e:
        log.error(f"Invalid job id '{args.job_id}': {e}")
        sys.exit(1)

    if args.force_start:
        if not cleanup_session(args.job_id):
            log.error(
                f"Cannot force start {args.job_id}: failed to kill existing session"
            )
            sys.exit(1)
        host_exists = False

    if host_exists:
        handle_existing_session(args)
    else:
        handle_new_session(args, parser)


if __name__ == "__main__":
    main()
