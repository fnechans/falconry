import htcondor2 as htcondor
import logging
import functools
from typing import Callable, Any, Iterator
import time
import signal
from contextlib import contextmanager

log = logging.getLogger('falconry')


@contextmanager
def ignore_sigint() -> Iterator[None]:
    """Temporarily ignore SIGINT (Ctrl+C) during critical C extension calls."""
    old_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, old_handler)


# Here the typing did not work properly ...
def schedd_check(func: Callable[["ScheddWrapper"], Any]) -> Any:
    @functools.wraps(func)
    def wrapper(
        self: "ScheddWrapper", *args: Any, **kwargs: Any
    ) -> Callable[["ScheddWrapper"], Any]:
        try:
            # Since htcondor 25 I see segfaults on SIGINT when calling
            # htcondor function and dont have the time to trace an report
            # so for now we are postponing SIGINT until the function returns
            with ignore_sigint():
                return func(self, *args, **kwargs)
        except htcondor.HTCondorException as e:
            log.warning(
                "Possible problem with scheduler, waiting a bit and reloading schedd ..."
            )
            log.debug(str(e))
            time.sleep(60)
            self.schedd = htcondor.Schedd()
            return func(self, *args, **kwargs)

    return wrapper


def kerberos_auth() -> None:
    """Add kerberos creds to the schedd"""
    try:
        credd = htcondor.Credd()
        credd.add_user_cred(htcondor.CredTypes.Kerberos, None)
    except:  # noqa
        log.warning(
            "Kerberos creds not available. This can cause problems on some clusters (like lxplus)."
        )


class ScheddWrapper:
    """Wrapper to allow reload of schedd"""

    def __init__(self) -> None:
        self.schedd = htcondor.Schedd()

    @property
    def location(self) -> str:
        """Return the address of the schedd"""
        return str(self.schedd._addr)

    """Reimplementing all the used functions"""

    @schedd_check
    def act(self, *args: Any, **kwargs: Any) -> int:
        return self.schedd.act(*args, **kwargs)

    @schedd_check
    def query(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        return self.schedd.query(*args, **kwargs)

    @schedd_check
    def history(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        return self.schedd.history(*args, **kwargs)

    @schedd_check
    def submit(self, *args: Any, **kwargs: Any) -> htcondor.SubmitResult:
        return self.schedd.submit(*args, **kwargs)
