import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from syntasks.codec import format_duration, to_jsonable
from syntasks.scheduler.scheduled_task.validators import (
    CronOffset,
    dump_cron_offset,
    parse_cron_offset,
    parse_interval,
    validate_interval_value,
)

_FIELDS = (
    "task_name",
    "labels",
    "args",
    "kwargs",
    "task_id",
    "schedule_id",
    "cron",
    "cron_offset",
    "time",
    "interval",
)


@dataclass(kw_only=True)
class ScheduledTask:
    """Abstraction over task schedule."""

    task_name: str
    labels: dict[str, Any]
    args: list[Any]
    kwargs: dict[str, Any]
    task_id: str | None = None
    schedule_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    cron: str | None = None
    cron_offset: CronOffset | None = None
    time: datetime | None = None
    interval: int | timedelta | None = None

    def __post_init__(self) -> None:  # noqa: C901
        """Validate and normalize.

        This method validates,
        that either `cron`, `interval` or `time` field is present.
        For interval tasks, validates that interval is at least 1 second
        and has no fractional seconds.

        :raises ValueError: if cron, interval and time are none, or interval is invalid.
        """
        if not isinstance(self.task_name, str):
            raise ValueError("task_name must be a string.")
        if not isinstance(self.labels, Mapping):
            raise ValueError("labels must be a mapping.")
        if not isinstance(self.args, (list, tuple)):
            raise ValueError("args must be a list.")
        if not isinstance(self.kwargs, Mapping):
            raise ValueError("kwargs must be a mapping.")
        self.labels = dict(self.labels)
        self.args = list(self.args)
        self.kwargs = dict(self.kwargs)
        if self.task_id is not None and not isinstance(self.task_id, str):
            raise ValueError("task_id must be a string or None.")
        if not isinstance(self.schedule_id, str):
            raise ValueError("schedule_id must be a string.")
        if self.cron is not None and not isinstance(self.cron, str):
            raise ValueError("cron must be a string or None.")
        self.cron_offset = parse_cron_offset(self.cron_offset)
        if isinstance(self.time, str):
            try:
                self.time = datetime.fromisoformat(self.time)
            except ValueError as exc:
                raise ValueError(f"Invalid time: {self.time!r}") from exc
        elif self.time is not None and not isinstance(self.time, datetime):
            raise ValueError("time must be a datetime or None.")
        self.interval = parse_interval(self.interval)

        if self.cron is None and self.time is None and self.interval is None:
            raise ValueError("Either cron, interval, or datetime must be present.")

        validate_interval_value(self.interval)

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the schedule to plain, JSON-compatible data.

        :return: schedule as a dict.
        """
        interval: Any = self.interval
        if isinstance(interval, timedelta):
            interval = format_duration(interval)
        return {
            "task_name": self.task_name,
            "labels": to_jsonable(self.labels),
            "args": to_jsonable(self.args),
            "kwargs": to_jsonable(self.kwargs),
            "task_id": self.task_id,
            "schedule_id": self.schedule_id,
            "cron": self.cron,
            "cron_offset": dump_cron_offset(self.cron_offset),
            "time": self.time.isoformat() if self.time is not None else None,
            "interval": interval,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ScheduledTask":
        """
        Build a schedule from plain data.

        :param data: schedule as a dict.
        :raises ValueError: if the data is not a valid schedule.
        :return: schedule.
        """
        if not isinstance(data, Mapping):
            raise ValueError(f"Schedule must be a mapping, got {type(data).__name__}.")
        try:
            return cls(**{k: data[k] for k in _FIELDS if k in data})
        except TypeError as exc:
            raise ValueError(f"Invalid schedule data: {exc}") from exc
