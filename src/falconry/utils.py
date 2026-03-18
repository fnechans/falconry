from typing import Union
import os
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

    log.debug(f"Cleaning {dir_path}. Keeping {keep_files}")
    for entry in base.iterdir():
        log.debug(f'Processing {entry.name}')
        if entry.name in keep_files:
            log.debug(f'Keeping {entry.name}')
            continue
        if entry.is_dir():
            shutil.rmtree(entry)
        else:
            entry.unlink()


# Source - https://stackoverflow.com/a/73195814
# Posted by Jazz Weisman
# Retrieved 2026-03-18, License - CC BY-SA 4.0


def tail_file(filename: str, lines: int = 10) -> str:
    """Returns the nth before last line of a file (n=1 gives last line).

    Arguments:
        filename (str): path to the file
        lines (int, optional): number of lines to return. Defaults to 10.

    Returns:
        str: last line of the file
    """
    num_newlines = 0
    with open(filename, 'rb') as f:
        try:
            f.seek(-2, os.SEEK_END)
            while num_newlines < lines:
                f.seek(-2, os.SEEK_CUR)
                if f.read(1) == b'\n':
                    num_newlines += 1
                if f.tell() == 0:
                    break
        except OSError:
            f.seek(0)
        return ''.join([line.decode() for line in f.readlines()])
