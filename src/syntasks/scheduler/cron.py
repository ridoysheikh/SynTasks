"""
Cron expressions (standard library only).

Five fields ``minute hour day-of-month month day-of-week``, or six with a
leading ``second`` field. Each field accepts ``*``, numbers, ranges
(``1-5``), steps (``*/15``, ``1-30/2``, ``5/10``), lists (``1,15,30``), month
names (``jan``-``dec``) and weekday names (``sun``-``sat``; ``0`` and ``7``
both mean Sunday). The macros ``@yearly``, ``@annually``, ``@monthly``,
``@weekly``, ``@daily``, ``@midnight`` and ``@hourly`` are expanded.

As in Vixie cron, when both day-of-month and day-of-week are restricted,
a date matches if *either* of them matches.
"""

import datetime as dt
from bisect import bisect_right
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


_SECONDS = _Field(0, 59, {})
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


def _last_day(day: dt.date) -> int:
    following = (day.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    return (following - dt.timedelta(days=1)).day


def _split_extras(
    days_text: str,
    weekdays_text: str,
) -> tuple[str, bool, str, frozenset[tuple[int, int]]]:
    """
    Take the Quartz extras out of the day fields.

    ``L`` in day-of-month is the last day; ``fri#2`` / ``5#2`` is the second
    Friday; ``fri#L``, ``friL`` and ``5L`` are the last Friday.

    :return: (plain day field, last day?, plain weekday field, (weekday, n) pairs).
    """
    day_parts = days_text.split(",")
    last_day = "L" in (part.upper() for part in day_parts)
    plain_days = ",".join(p for p in day_parts if p.upper() != "L")
    nth: set[tuple[int, int]] = set()
    plain_weekdays = []
    for part in weekdays_text.split(","):
        upper = part.upper()
        base, which = None, None
        if "#" in part:
            base, which = part.split("#", 1)
        elif len(part) > 1 and upper.endswith("L"):
            base, which = part[:-1], "L"
        if base is None:
            plain_weekdays.append(part)
            continue
        if which is None or not (
            which.upper() == "L" or which in {"1", "2", "3", "4", "5"}
        ):
            raise ValueError(f"Invalid cron item {part!r}: after '#' use 1-5 or L.")
        weekday = _value(base, _FIELDS[4]) % 7
        nth.add((weekday, -1 if which.upper() == "L" else int(which)))
    return (
        plain_days or ("" if last_day else days_text),
        last_day,
        ",".join(plain_weekdays),
        frozenset(nth),
    )


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
    seconds: frozenset[int] = frozenset({0})
    has_seconds: bool = False
    last_day: bool = False
    nth_weekdays: frozenset[tuple[int, int]] = frozenset()

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
        seconds = frozenset({0})
        has_seconds = len(parts) == len(_FIELDS) + 1
        if has_seconds:
            seconds = _parse_field(parts[0], _SECONDS)
            parts = parts[1:]
        if len(parts) != len(_FIELDS):
            raise ValueError(
                "Cron expression must have 5 fields (or 6 with seconds), "
                f"got {len(text.split())}: {expression!r}",
            )
        days_text, last_day, weekdays_text, nth = _split_extras(parts[2], parts[4])
        minutes, hours, months = (_parse_field(parts[i], _FIELDS[i]) for i in (0, 1, 3))
        days = _parse_field(days_text, _FIELDS[2]) if days_text else frozenset()
        weekdays = (
            _parse_field(weekdays_text, _FIELDS[4]) if weekdays_text else frozenset()
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
            seconds=seconds,
            has_seconds=has_seconds,
            last_day=last_day,
            nth_weekdays=nth,
        )

    def matches(self, moment: datetime) -> bool:
        """
        Check whether a moment matches this expression.

        Five-field expressions compare to the minute; six-field ones also
        compare the second.

        :param moment: moment to check.
        :return: True if it matches.
        """
        if self.has_seconds and moment.second not in self.seconds:
            return False
        if (
            moment.minute not in self.minutes
            or moment.hour not in self.hours
            or moment.month not in self.months
        ):
            return False
        return self._day_ok(moment.date())

    def _day_ok(self, day: dt.date) -> bool:
        last = _last_day(day)
        day_ok = day.day in self.days or (self.last_day and day.day == last)
        weekday = day.isoweekday() % 7
        weekday_ok = weekday in self.weekdays or any(
            weekday == wanted
            and ((day.day - 1) // 7 + 1 == n if n > 0 else day.day > last - 7)
            for wanted, n in self.nth_weekdays
        )
        if self.days_restricted and self.weekdays_restricted:
            return day_ok or weekday_ok
        return day_ok and weekday_ok

    def next_local(self, after: datetime) -> datetime | None:  # noqa: C901, PLR0912
        """
        First naive local time strictly after ``after`` that matches.

        :param after: naive local time.
        :return: next match, or None if there is none within 8 years.
        """
        step = dt.timedelta(seconds=1) if self.has_seconds else dt.timedelta(minutes=1)
        moment = after.replace(microsecond=0) + step
        if not self.has_seconds:
            moment = moment.replace(second=0)
        seconds = sorted(self.seconds)
        minutes = sorted(self.minutes)
        hours = sorted(self.hours)
        limit = after.year + 8
        while moment.year <= limit:
            if moment.month not in self.months:
                year, month = (
                    (moment.year + 1, 1)
                    if moment.month == 12
                    else (moment.year, moment.month + 1)
                )
                moment = datetime(year, month, 1)
                continue
            if not self._day_ok(moment.date()):
                moment = datetime.combine(
                    moment.date() + dt.timedelta(days=1),
                    dt.time(),
                )
                continue
            if moment.hour not in self.hours:
                index = bisect_right(hours, moment.hour)
                if index == len(hours):
                    moment = datetime.combine(
                        moment.date() + dt.timedelta(days=1),
                        dt.time(),
                    )
                else:
                    moment = moment.replace(hour=hours[index], minute=0, second=0)
                continue
            if moment.minute not in self.minutes:
                index = bisect_right(minutes, moment.minute)
                if index == len(minutes):
                    moment = moment.replace(minute=0, second=0) + dt.timedelta(hours=1)
                else:
                    moment = moment.replace(minute=minutes[index], second=0)
                continue
            if moment.second not in self.seconds:
                index = bisect_right(seconds, moment.second)
                if index == len(seconds):
                    moment = moment.replace(second=0) + dt.timedelta(minutes=1)
                else:
                    moment = moment.replace(second=seconds[index])
                continue
            return moment
        return None

    def next_after(
        self,
        after: datetime,
        tz: dt.tzinfo | None = None,
    ) -> datetime | None:
        """
        First time strictly after ``after`` that matches, in time zone ``tz``.

        Local times skipped by a DST jump don't fire; times repeated when
        clocks go back fire once (the first occurrence).

        :param after: aware datetime.
        :param tz: time zone the expression is written in (default UTC).
        :return: aware datetime of the next fire, or None.
        """
        zone = tz or dt.UTC
        local = after.astimezone(zone).replace(tzinfo=None)
        for _ in range(1000):
            candidate = self.next_local(local)
            if candidate is None:
                return None
            aware = candidate.replace(tzinfo=zone, fold=0)
            if (
                aware.astimezone(dt.UTC).astimezone(zone).replace(tzinfo=None)
                != candidate
            ):
                local = candidate  # does not exist (DST gap)
                continue
            if aware > after:
                return aware
            local = candidate
        return None


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
