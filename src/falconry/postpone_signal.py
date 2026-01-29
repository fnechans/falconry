from typing import Iterator, Any
import signal
from contextlib import contextmanager


class GracefulTerminate(SystemExit):
    """Raised when a postponed SIGTERM is delivered."""

    pass


class HangupReceived(RuntimeError):
    """Raised when a postponed SIGHUP is delivered."""

    pass


_mapping = {
    signal.SIGINT: KeyboardInterrupt,
    signal.SIGTERM: GracefulTerminate,
    signal.SIGHUP: HangupReceived,
}


@contextmanager
def postpone_signal() -> Iterator[None]:
    """
    Temporarily ignore signals while inside the context,
    but remember if the user attempted to interrupt.
    After exiting, re-raise error.
    """
    old_handlers = {}
    for sig, exc in _mapping.items():
        old_handlers[sig] = signal.getsignal(sig)

        def _record_interrupt(
            signum: Any, frame: Any, _sig: Any = sig, _exc: type[BaseException] = exc
        ) -> None:
            signal.signal(_sig, old_handlers[_sig])
            raise _exc

        signal.signal(sig, _record_interrupt)

    try:
        yield
    finally:
        for sig, _ in _mapping.items():
            signal.signal(sig, old_handlers[sig])
