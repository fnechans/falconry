from typing import Union
import subprocess
import logging

log = logging.getLogger('falconry')


def run_command_local(command: str) -> bool:
    """Runs a command locally, returns True on success, False on failure"""
    log.debug(f'Running command: {command}')
    result = subprocess.run(command.split(), stdout=subprocess.PIPE)
    return result.returncode == 0


def prepend(old: Union[str, list[str]], new: list[str]) -> list[str]:
    if isinstance(old, list):
        return old + new
    else:
        return [old] + new
