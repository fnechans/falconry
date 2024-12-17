import sys
import select
import logging
from enum import Enum
from typing import Dict, Optional, Tuple

log = logging.getLogger('falconry')


# helper class to define the status
class InputState(Enum):
    UNKNOWN = -1
    SUCCESS = 0
    TIMEOUT = 1


def input_checker(
    validOptions: Dict[str, str],
    timeout: int = 60,
    message: str = "Following options available:",
    silent: bool = False,
) -> Tuple[InputState, Optional[str]]:
    """Helper function to get input from user

    Arguments:
        validOptions (Dict[str, str]): dictionary of valid options
        timeout (int, optional): timeout in seconds. Defaults to 60.
        message (str, optional): message to print before options.
        Defaults to "Following options available:".
        silent (bool, optional): silent mode. Defaults to False.

    Returns:
        Tuple[InputState, Optional[str]]: returns the state of the input
    """
    if message != "" and not silent:
        log.info(message)
    for opt, desc in validOptions.items():
        if desc != "" and not silent:
            log.info(f"{opt} - {desc}")

    i, o, e = select.select([sys.stdin], [], [], timeout)
    if i:
        inp = sys.stdin.readline().strip()
        if inp in validOptions.keys():
            return InputState.SUCCESS, inp

        if not silent:
            log.info(f"Unknown state {inp}!")
        return InputState.UNKNOWN, inp

    else:
        if not silent:
            log.info("Timed out ...")
        return InputState.TIMEOUT, None
