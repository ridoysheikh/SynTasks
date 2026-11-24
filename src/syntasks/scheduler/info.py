"""What a scheduled run knows about its schedule (inject with ``Depends()``)."""

import datetime as dt
from dataclasses import dataclass
from typing import Any

__all__ = ["ScheduleInfo"]


@dataclass(frozen=True, kw_only=True)
class ScheduleInfo:
    """
    Schedule details of the current run.

    ``schedule_id`` is None when the run was not started by a schedule::

        @app.schedule(every("10s"))
        async def poll(info: Annotated[ScheduleInfo, Depends()]) -> None:
            print(info.schedule_id, info.scheduled_for, info.late_by)
    """

    schedule_id: str | None = None
    fire_time: dt.datetime | None = None
    run_number: int = 0
    attempt: int = 0

    @property
    def scheduled(self) -> bool:
        """Whether this run was started by a schedule."""
        return self.schedule_id is not None

    @property
    def is_retry(self) -> bool:
        """Whether this run is a retry of a failed fire."""
        return self.attempt > 0

    @property
    def scheduled_for(self) -> dt.datetime | None:
        """The fire time this run was planned for (same as ``fire_time``)."""
        return self.fire_time

    @property
    def late_by(self) -> float:
        """
        Seconds between the planned fire time and now.

        Large after downtime: a missed fire runs as soon as it's noticed.
        """
        if self.fire_time is None:
            return 0.0
        return (dt.datetime.now(dt.UTC) - self.fire_time).total_seconds()

    @classmethod
    def from_labels(cls, labels: dict[str, Any]) -> "ScheduleInfo":
        """
        Build from message labels.

        :param labels: message labels.
        :return: schedule info (empty if not scheduled).
        """
        schedule_id = labels.get("_schedule")
        if not schedule_id:
            return cls(attempt=int(labels.get("_attempt", 0) or 0))
        fire_ms = labels.get("_fire")
        fire_time = (
            dt.datetime.fromtimestamp(int(str(fire_ms)) / 1000, tz=dt.UTC)
            if fire_ms not in (None, "")
            else None
        )
        return cls(
            schedule_id=str(schedule_id),
            fire_time=fire_time,
            run_number=int(labels.get("_run", 0) or 0),
            attempt=int(labels.get("_attempt", 0) or 0),
        )
