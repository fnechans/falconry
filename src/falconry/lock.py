from typing import Any, TypeVar, Callable, cast
import os
import logging


log = logging.getLogger('falconry')


class LockFile:
    def __init__(self, path: str) -> None:
        self.path = path
        if os.path.exists(self.path):
            log.error("Manager instance is already running.")
            log.debug(
                f"Delete {self.path} to start a new instance if you think this is a mistake"
            )
            raise LockFileException

    def __enter__(self) -> None:
        log.debug(f"Locking {self.path}")
        with open(self.path, "w") as f:
            f.write("")

    def __exit__(self, *excinfo: Any) -> None:
        log.debug(f"Unlocking {self.path}")
        os.remove(self.path)


class LockFileException(Exception):
    pass


FuncT = TypeVar("FuncT", bound=Callable[..., Any])


def lock(func: FuncT) -> FuncT:
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        args[0]._check_lock()
        with LockFile(args[0].lockFile):
            return func(*args, **kwargs)

    return cast(FuncT, wrapper)
