from typing import Union
import subprocess
import logging
import shutil
from pathlib import Path

log = logging.getLogger('falconry')


def run_command_local(command: str) -> bool:
    """Runs a command locally, returns True on success, False on failure.

    Arguments:
        command (str): command to run
    Returns:
        bool: True on success, False on failure
    """
    log.debug(f'Running command: {command}')
    result = subprocess.run(command.split(), stdout=subprocess.PIPE)
    return result.returncode == 0


def prepend(old: Union[str, list[str]], new: list[str]) -> list[str]:
    """For backward compatibility, prepend a list to another list
    or create new list from a single element and prepend it

    Arguments:
        old (Union[str, list[str]]): list to prepend
        new (list[str]): list to be prepended

    Returns:
        list[str]: prepended list
    """
    if isinstance(old, list):
        return old + new
    else:
        return [old] + new


def clean_dir(dir_path: str, keep_files: set[str]) -> None:
    """
    Remove everything inside `dir_path` except the files listed in `keep_files`.

    Arguments:
        dir_path (str): path to the directory to clean
        keep_files (set): set of file names to keep
    """
    base = Path(dir_path)

    for entry in base.iterdir():
        if entry.name in keep_files:
            continue
        if entry.is_dir():
            shutil.rmtree(entry)
        else:
            entry.unlink()
