
#!/usr/bin/env python3
"""Check if tmux session exists for a given job id and node.

This module provides functionality to verify that a tmux session exists
and matches the expected node from a hostfile.
"""

import argparse
import logging
import os
import subprocess
import sys
from typing import Optional


def validate_hostfile_filename(hostfile: str, job_id: str) -> bool:
    """Validate that hostfile basename matches expected format.
    
    Arguments:
        hostfile: Path to the hostfile
        job_id: Expected job ID
        
    Returns:
        True if valid, False otherwise
    """
    return os.path.basename(hostfile) == f"{job_id}.host"


def read_hostfile_node(hostfile: str) -> Optional[str]:
    """Read and validate node from hostfile.
    
    Arguments:
        hostfile: Path to the hostfile
        
    Returns:
        Node string if valid, None otherwise
    """
    if not os.path.exists(hostfile):
        return None
        
    with open(hostfile, "r") as f:
        content = f.read().strip()
        
    if not content:
        return None
    
    return content


def check_tmux_session_local(job_id: str) -> bool:
    """Check if tmux session exists locally.
    
    Arguments:
        job_id: Job ID to check
        
    Returns:
        True if session exists, False otherwise
    """
    command = ["tmux", "has-session", "-t", job_id]
    result = subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return result.returncode == 0


def check_tmux_session_remote(job_id: str, node: str) -> bool:
    """Check if tmux session exists on remote node via SSH.
    
    Arguments:
        job_id: Job ID to check
        node: Remote node to check
        
    Returns:
        True if session exists, False otherwise
    """
    command = ["ssh", node, "tmux", "has-session", "-t", job_id]
    result = subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return result.returncode == 0


def check_session(hostfile: str, job_id: str, node: str, debug: bool = False) -> int:
    """Main check logic for ssh_check.
    
    Arguments:
        hostfile: Path to the hostfile
        job_id: Job ID to check
        node: Expected node
        debug: Enable debug logging
        
    Returns:
        Exit code (0 = success, 1 = error)
    """
    import re
    
    logging.basicConfig(level=logging.DEBUG if debug else logging.INFO)
    log = logging.getLogger("ssh_check")
    
    # Validate job_id format (must match __main__.py validation)
    if not re.match(r'^[A-Za-z0-9_-]+$', job_id):
        log.error(
            f"Invalid job id: {job_id}, must contain only letters, "
            "numbers, hyphens, and underscores"
        )
        return 1
    
    # Validate hostfile name
    if not validate_hostfile_filename(hostfile, job_id):
        log.error(
            f"Hostfile name {os.path.basename(hostfile)} does not match "
            f"expected format {job_id}.host"
        )
        return 1
    
    # Check if hostfile exists
    if not os.path.exists(hostfile):
        log.info(f"Hostfile {hostfile} does not exist")
        return 0
    
    log.info(f"Hostfile exists for {job_id}, checking node...")
    
    # Read node from hostfile
    file_node = read_hostfile_node(hostfile)
    if file_node is None:
        log.error(f"Hostfile {hostfile} is empty or unreadable")
        return 1
    
    # Check if node matches
    if file_node != node:
        log.info(f"Hostfile node {file_node} does not match expected node {node}")
        return 1
    
    # Check if session exists
    local_node = os.uname().nodename
    if file_node == local_node:
        session_exists = check_tmux_session_local(job_id)
    else:
        session_exists = check_tmux_session_remote(job_id, file_node)
    
    if not session_exists:
        log.info(f"Session {job_id} does not exist on {file_node}, removing stale hostfile")
        try:
            os.remove(hostfile)
        except OSError as e:
            log.error(f"Failed to remove stale hostfile {hostfile}: {e}")
            return 1
        return 0
    
    log.info(f"Session {job_id} exists on {file_node}")
    return 0


def main() -> None:
    """Entry point for ssh_check command line tool."""
    parser = argparse.ArgumentParser(
        description="Check if tmux session exists for a given job id and node"
    )
    parser.add_argument("job_id", help="Job ID to check for the tmux session")
    parser.add_argument("hostfile", help="Path to the hostfile to check")
    parser.add_argument("node", help="Node to check for the tmux session")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    
    sys.exit(check_session(args.hostfile, args.job_id, args.node, args.debug))


if __name__ == "__main__":
    main()