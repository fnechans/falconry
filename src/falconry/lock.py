import logging
import os
from typing import Any, TypeVar, Callable, cast

import filelock

log = logging.getLogger('falconry')


class LockFile:
    """File-based lock using the filelock library for cross-platform locking."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._lock: filelock.FileLock | None = None

    def __enter__(self) -> 'LockFile':
        log.debug(f"Locking {self.path}")
        self._lock = filelock.FileLock(self.path, timeout=0)
        try:
            self._lock.acquire()
            # Write process ID to lock file for debugging
            with open(self.path, 'w') as f:
                f.write(str(os.getpid()))
                f.flush()
                os.fsync(f.fileno())
        except filelock.Timeout:
            if os.path.exists(self.path):
                log.error(
                    f"Manager instance is already running in {os.path.dirname(self.path)}"
                )
                log.debug(
                    f"Delete {self.path} to start a new instance if you think this is a mistake"
                )
            raise LockFileException
        except Exception:
            # If we acquired the lock but writing failed, release it
            if self._lock.is_locked:
                try:
                    self._lock.release()
                except Exception:
                    pass
            raise
        return self

    def __exit__(self, *excinfo: Any) -> None:
        log.debug(f"Unlocking {self.path}")
        if self._lock is not None:
            try:
                self._lock.release()
            except Exception:
                pass  # Ignore errors during unlock
            finally:
                self._lock = None


class LockFileException(Exception):
    """Exception raised when a lock cannot be acquired."""
    pass


FuncT = TypeVar("FuncT", bound=Callable[..., Any])


def lock(func: FuncT) -> FuncT:
    """Decorator to lock a method using the manager's lock file.

    Expects the first argument (self) to have a 'lockFile' attribute.
    """
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with LockFile(args[0].lockFile):
            return func(*args, **kwargs)

    return cast(FuncT, wrapper)
