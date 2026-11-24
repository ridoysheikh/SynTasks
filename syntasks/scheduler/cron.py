"""
Minimal cron expression matcher (standard library only).

Supports the classic five fields: ``minute hour day-of-month month
day-of-week``. Each field accepts ``*``, numbers, ranges (``1-5``),
steps (``*/15``, ``1-30/2``, ``5/10``), lists (``1,15,30``), month names
(``jan``-``dec``) and weekday names (``sun``-``sat``; ``0`` and ``7`` both
mean Sunday). The macros ``@yearly``, ``@annually``, ``@monthly``,
``@weekly``, ``@daily``, ``@midnight`` and ``@hourly`` are expanded.

As in Vixie cron, when both day-of-month and day-of-week are restricted,
a date matches if *either* of them matches.
"""

from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache

__all__ = ["CronExpression", "is_now"]

_MACROS = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}
_MONTHS = {
    name: i
    for i, name in enumerate(
        [
            "jan",
            "feb",
            "mar",
            "apr",
            "may",
            "jun",
            "jul",
            "aug",
            "sep",
            "oct",
            "nov",
            "dec",
        ],
        start=1,
    )
}
_WEEKDAYS = {
    name: i for i, name in enumerate(["sun", "mon", "tue", "wed", "thu", "fri", "sat"])
}


@dataclass(frozen=True)
class _Field:
    low: int
    high: int
    names: dict[str, int]


_FIELDS = (
    _Field(0, 59, {}),  # minute
    _Field(0, 23, {}),  # hour
    _Field(1, 31, {}),  # day of month
    _Field(1, 12, _MONTHS),  # month
    _Field(0, 7, _WEEKDAYS),  # day of week (7 is Sunday too)
)


def _value(token: str, spec: _Field) -> int:
    token = token.strip().lower()
    if token in spec.names:
        return spec.names[token]
    if not token.isdigit():
        raise ValueError(f"Invalid cron value: {token!r}")
    value = int(token)
    if not spec.low <= value <= spec.high:
        raise ValueError(
            f"Cron value {value} is out of range {spec.low}-{spec.high}.",
        )
    return value


def _parse_field(text: str, spec: _Field) -> frozenset[int]:
    values: set[int] = set()
    for part in text.split(","):
        if not part:
            raise ValueError(f"Empty item in cron field {text!r}")
        base, _, step_text = part.partition("/")
        step = 1
        if step_text:
            if not step_text.isdigit() or int(step_text) == 0:
                raise ValueError(f"Invalid cron step: {part!r}")
            step = int(step_text)
        if base == "*":
            start, end = spec.low, spec.high
        elif "-" in base:
            start_text, end_text = base.split("-", 1)
            start, end = _value(start_text, spec), _value(end_text, spec)
            if start > end:
                raise ValueError(f"Invalid cron range: {part!r}")
        else:
            start = _value(base, spec)
            # "5/10" means "starting at 5, every 10".
            end = spec.high if step_text else start
        values.update(range(start, end + 1, step))
    return frozenset(values)


@dataclass(frozen=True)
class CronExpression:
    """A parsed cron expression."""

    minutes: frozenset[int]
    hours: frozenset[int]
    days: frozenset[int]
    months: frozenset[int]
    weekdays: frozenset[int]
    days_restricted: bool
    weekdays_restricted: bool

    @classmethod
    def parse(cls, expression: str) -> "CronExpression":
        """
        Parse a cron expression.

        :param expression: cron expression or macro.
        :raises ValueError: if the expression is invalid.
        :return: parsed expression.
        """
        text = _MACROS.get(expression.strip().lower(), expression)
        parts = text.split()
        if len(parts) != len(_FIELDS):
            raise ValueError(
                f"Cron expression must have 5 fields, got {len(parts)}: {expression!r}",
            )
        minutes, hours, days, months, weekdays = (
            _parse_field(part, spec) for part, spec in zip(parts, _FIELDS, strict=True)
        )
        # Normalize Sunday: 7 -> 0.
        if 7 in weekdays:
            weekdays = (weekdays - {7}) | {0}
        return cls(
            minutes=minutes,
            hours=hours,
            days=days,
            months=months,
            weekdays=weekdays,
            days_restricted=not parts[2].startswith("*"),
            weekdays_restricted=not parts[4].startswith("*"),
        )

    def matches(self, moment: datetime) -> bool:
        """
        Check whether a moment matches this expression (to the minute).

        :param moment: moment to check.
        :return: True if it matches.
        """
        if (
            moment.minute not in self.minutes
            or moment.hour not in self.hours
            or moment.month not in self.months
        ):
            return False
        day_ok = moment.day in self.days
        weekday_ok = moment.isoweekday() % 7 in self.weekdays
        if self.days_restricted and self.weekdays_restricted:
            return day_ok or weekday_ok
        return day_ok and weekday_ok


@lru_cache(maxsize=4096)
def _parse_cached(expression: str) -> CronExpression:
    return CronExpression.parse(expression)


def is_now(expression: str, moment: datetime) -> bool:
    """
    Check whether ``moment`` matches the cron ``expression``.

    :param expression: cron expression.
    :param moment: moment to check.
    :raises ValueError: if the expression is invalid.
    :return: True if it matches.
    """
    return _parse_cached(expression).matches(moment)
