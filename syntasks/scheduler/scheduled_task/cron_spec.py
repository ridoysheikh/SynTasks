from dataclasses import dataclass
from typing import Any

from syntasks.scheduler.scheduled_task.validators import (
    CronOffset,
    dump_cron_offset,
    parse_cron_offset,
)


@dataclass(kw_only=True)
class CronSpec:
    """Cron specification for running tasks."""

    minutes: str | int | None = "*"
    hours: str | int | None = "*"
    days: str | int | None = "*"
    months: str | int | None = "*"
    weekdays: str | int | None = "*"

    offset: CronOffset | None = None

    def __post_init__(self) -> None:
        self.offset = parse_cron_offset(self.offset)

    def to_cron(self) -> str:  # pragma: no cover
        """Converts cron spec to cron string."""
        return f"{self.minutes} {self.hours} {self.days} {self.months} {self.weekdays}"

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to plain data.

        :return: cron spec as a dict.
        """
        return {
            "minutes": self.minutes,
            "hours": self.hours,
            "days": self.days,
            "months": self.months,
            "weekdays": self.weekdays,
            "offset": dump_cron_offset(self.offset),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CronSpec":
        """
        Build from plain data.

        :param data: cron spec as a dict.
        :return: cron spec.
        """
        fields = ("minutes", "hours", "days", "months", "weekdays", "offset")
        return cls(**{k: data[k] for k in fields if k in data})
