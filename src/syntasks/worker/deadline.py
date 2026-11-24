"""Time limits a running task can see (inject with ``Depends()``)."""

import asyncio
import time

__all__ = ["Deadline"]


class Deadline:
    """
    The time limits of the current run.

    Inject it into a task to checkpoint before the hard timeout::

        @app.task(soft_timeout="50s", timeout="60s")
        async def crunch(deadline: Deadline = Depends()) -> None:
            for chunk in chunks:
                if deadline.soft_expired:
                    save_progress(); return
                ...
    """

    def __init__(
        self,
        *,
        soft_timeout: float | None = None,
        timeout: float | None = None,
        started: float | None = None,
    ) -> None:
        self.started = time.monotonic() if started is None else started
        self.soft_timeout = soft_timeout
        self.timeout = timeout
        self.soft_reached = asyncio.Event()

    def __repr__(self) -> str:
        return f"<Deadline soft={self.soft_timeout} hard={self.timeout}>"

    @property
    def elapsed(self) -> float:
        """Seconds since the run started."""
        return time.monotonic() - self.started

    def remaining(self) -> float | None:
        """Seconds until the hard timeout (None if there is none)."""
        if self.timeout is None:
            return None
        return max(self.timeout - self.elapsed, 0.0)

    def soft_remaining(self) -> float | None:
        """Seconds until the soft timeout (None if there is none)."""
        if self.soft_timeout is None:
            return None
        return max(self.soft_timeout - self.elapsed, 0.0)

    @property
    def soft_expired(self) -> bool:
        """Whether the soft timeout has passed."""
        return self.soft_timeout is not None and self.elapsed >= self.soft_timeout

    @property
    def expired(self) -> bool:
        """Whether the hard timeout has passed."""
        return self.timeout is not None and self.elapsed >= self.timeout
