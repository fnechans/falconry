import htcondor  # for submitting jobs, querying HTCondor daemons, etc.
import logging
import functools
from typing import Callable, Any
import time

log = logging.getLogger('falconry')


class ScheddWrapper:
    """Wrapper to allow reload of schedd"""
    def __init__(self) -> None:
        self.schedd = htcondor.Schedd()

    # Here the typing did not work properly ...
    def schedd_check(func: Callable[["ScheddWrapper"], Any]):  # type: ignore
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except htcondor.HTCondorIOError:
                log.warning("Possible problem with scheduler, waiting a bit and reloading schedd ...")
                time.sleep(60)
                self.schedd = htcondor.Schedd()
                return func(self, *args, **kwargs)
        return wrapper

    """Reimplementing all the used functions"""
    @schedd_check
    def transaction(self):
        return self.schedd.transaction()

    @schedd_check
    def act(self, *args, **kwargs):
        return self.schedd.act(*args, **kwargs)

    @schedd_check
    def query(self, *args, **kwargs):
        return self.schedd.query(*args, **kwargs)

    @schedd_check
    def history(self, *args, **kwargs):
        return self.schedd.history(*args, **kwargs)

    @schedd_check
    def submit(self, *args, **kwargs):
        return self.schedd.submit(*args, **kwargs)
