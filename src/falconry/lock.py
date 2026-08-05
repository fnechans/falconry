from typing import Any, TYPE_CHECKING, TypeVar, Callable, cast
import os
import logging
import fcntl
import errno

if TYPE_CHECKING:
    from typing import TextIO


log = logging.getLogger('falconry')


class LockFile:
    """File-based lock using fcntl.flock for atomic locking on Unix systems."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.file_obj: 'TextIO | None' = None

    def __enter__(self) -> 'LockFile':
        log.debug(f"Locking {self.path}")
        # Open file for reading/writing (create if doesn't exist)
        self.file_obj = open(self.path, 'w')
        assert self.file_obj is not None
        try:
            # Try to acquire an exclusive lock (non-blocking)
            assert self.file_obj is not None
            fcntl.flock(self.file_obj.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                # Write process ID to lock file for debugging
                assert self.file_obj is not None
                self.file_obj.write(str(os.getpid()))
                self.file_obj.flush()
            except Exception:
                # Lock was acquired but write/flush failed - unlock before re-raising
                assert self.file_obj is not None
                try:
                    fcntl.flock(self.file_obj.fileno(), fcntl.LOCK_UN)
                except OSError:
                    pass
                self.file_obj.close()
                self.file_obj = None
                raise
        except (OSError, ValueError) as e:
            # fcntl.flock can raise OSError (includes IOError) or ValueError
            assert self.file_obj is not None
            self.file_obj.close()
            self.file_obj = None
            if isinstance(e, OSError) and e.errno in (errno.EACCES, errno.EAGAIN):
                # Lock is held by another process
                if os.path.exists(self.path):
                    log.error(
                        f"Manager instance is already running in {os.path.dirname(self.path)}"
                    )
                    log.debug(
                        f"Delete {self.path} to start a new instance if you think this is a mistake"
                    )
                raise LockFileException
            raise
        return self

    def __exit__(self, *excinfo: Any) -> None:
        log.debug(f"Unlocking {self.path}")
        if self.file_obj is not None:
            try:
                # Release the lock
                assert self.file_obj is not None
                fcntl.flock(self.file_obj.fileno(), fcntl.LOCK_UN)
            except IOError:
                pass  # Ignore errors during unlock
            finally:
                assert self.file_obj is not None
                self.file_obj.close()
                self.file_obj = None


class LockFileException(Exception):
    pass


FuncT = TypeVar("FuncT", bound=Callable[..., Any])


def lock(func: FuncT) -> FuncT:
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with LockFile(args[0].lockFile):
            return func(*args, **kwargs)

    return cast(FuncT, wrapper)
