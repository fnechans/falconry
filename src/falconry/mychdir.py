import os
from typing import Any

try:
    from contextlib import chdir  # type: ignore
except ImportError:
    # Simple copy from python source for python <= 3.11
    from contextlib import AbstractContextManager

    class chdir(AbstractContextManager):  # type: ignore
        """Non thread-safe context manager to change the current working directory."""

        def __init__(self, path: str) -> None:
            self.path = path
            self._old_cwd: list[str] = []

        def __enter__(self) -> None:
            self._old_cwd.append(os.getcwd())
            os.chdir(self.path)

        def __exit__(self, *excinfo: Any) -> None:
            os.chdir(self._old_cwd.pop())
