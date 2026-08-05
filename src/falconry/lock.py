from typing import Any, TypeVar, Callable, cast
import os
import logging
import fcntl
import errno


log = logging.getLogger('falconry')


class LockFile:
    """File-based lock using fcntl.flock for atomic locking on Unix systems."""
    
    def __init__(self, path: str) -> None:
        self.path = path
        self.file_obj = None
        
    def __enter__(self) -> 'LockFile':
        log.debug(f"Locking {self.path}")
        # Open file for reading/writing (create if doesn't exist)
        self.file_obj = open(self.path, 'w')
        try:
            # Try to acquire an exclusive lock (non-blocking)
            fcntl.flock(self.file_obj.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError as e:
            if e.errno == errno.EACCES or e.errno == errno.EAGAIN:
                # Lock is held by another process
                self.file_obj.close()
                self.file_obj = None
                if os.path.exists(self.path):
                    log.error(f"Manager instance is already running in {os.path.dirname(self.path)}")
                    log.debug(
                        f"Delete {self.path} to start a new instance if you think this is a mistake"
                    )
                raise LockFileException
            else:
                # Some other error
                self.file_obj.close()
                self.file_obj = None
                raise
        # Write process ID to lock file for debugging
        self.file_obj.write(str(os.getpid()))
        self.file_obj.flush()
        return self

    def __exit__(self, *excinfo: Any) -> None:
        log.debug(f"Unlocking {self.path}")
        if self.file_obj is not None:
            try:
                # Release the lock
                fcntl.flock(self.file_obj.fileno(), fcntl.LOCK_UN)
            except IOError:
                pass  # Ignore errors during unlock
            finally:
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
