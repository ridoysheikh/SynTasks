"""
Calendar rules: "09:00 every Sunday", "first Monday of the month", ...

A rule is a date filter plus times of day, in a time zone::

    calendar_rule(weekday="sun", at="09:00")                 # every Sunday
    calendar_rule(weekday="mon", nth=1, at="09:00")          # first Monday
    calendar_rule(weekday="fri", nth=-1, at="17:30")         # last Friday
    calendar_rule(day=-1, at="23:00")                        # last day of month
    calendar_rule(weekday="mon-fri", at=["09:00", "13:00"])  # weekdays, twice
    calendar_rule(month="dec", day=25, at="08:00")           # yearly
    calendar_rule(week=2, weekday="tue", at="10:00")         # every 2nd Tuesday
    windowed_every(900_000, weekday="mon-fri", between=("09:00", "18:00"))

Every filter that is given must match (unlike cron, where day-of-month and
day-of-week are "either"). Times are wall-clock times in ``tz``: a time
skipped by a DST jump fires at the first valid instant after it; a time
repeated when clocks go back fires once, at its first occurrence.
"""

import datetime as dt
import itertools
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from syntasks.scheduler.recurrence import Recurrence
from syntasks.units import format_duration

__all__ = ["CalendarRule", "calendar_rule", "windowed_every"]

_WEEKDAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
_WEEKDAY_NAMES = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]
_MONTHS = [
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
]
_MONTH_NAMES = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]
_ORDINALS = {1: "first", 2: "second", 3: "third", 4: "fourth", 5: "fifth", -1: "last"}
_EPOCH_MONDAY = dt.date(1970, 1, 5)
# Long enough for Feb 29 on a given weekday; a rule that finds nothing in
# this many days never fires.
_SEARCH_DAYS = 366 * 29
_DAY_MS = 86_400_000


def _items(value: Any) -> list[Any]:
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    if isinstance(value, Iterable):
        return list(value)
    return [value]


def _named(value: Any, names: list[str], what: str, first: int) -> frozenset[int]:
    """Parse names, ``a-b`` ranges (wrapping) and numbers into 0-based indexes."""
    out: set[int] = set()

    def index(token: Any) -> int:
        if isinstance(token, int) and not isinstance(token, bool):
            if first <= token < first + len(names):
                return token - first
        elif isinstance(token, str) and token.strip().lower()[:3] in names:
            return names.index(token.strip().lower()[:3])
        raise ValueError(f"Invalid {what}: {token!r}.")

    for item in _items(value):
        if isinstance(item, str) and "-" in item:
            start_text, end_text = item.split("-", 1)
            start, end = index(start_text), index(end_text)
            span = (end - start) % len(names)
            out.update((start + i) % len(names) for i in range(span + 1))
        else:
            out.add(index(item))
    return frozenset(out)


def _ints(value: Any, what: str, allowed: set[int]) -> tuple[int, ...]:
    out = []
    for item in _items(value):
        try:
            number = int(item)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid {what}: {item!r}.") from None
        if number not in allowed:
            raise ValueError(f"Invalid {what}: {number}.")
        out.append(number)
    return tuple(sorted(set(out)))


def _time(value: Any, what: str = "at") -> dt.time:
    if isinstance(value, dt.time):
        return value.replace(microsecond=0, tzinfo=None)
    try:
        return dt.time.fromisoformat(str(value).strip())
    except ValueError:
        raise ValueError(
            f"Invalid {what} time {value!r}; use 'HH:MM' or 'HH:MM:SS'."
        ) from None


def _date(value: Any) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    try:
        return dt.date.fromisoformat(str(value))
    except ValueError:
        raise ValueError(f"Invalid date {value!r}; use 'YYYY-MM-DD'.") from None


def _last_day(day: dt.date) -> int:
    following = (day.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    return (following - dt.timedelta(days=1)).day


def _hhmm(moment: dt.time) -> str:
    return moment.strftime("%H:%M:%S" if moment.second else "%H:%M")


def _join(words: list[str]) -> str:
    return words[0] if len(words) == 1 else ", ".join(words[:-1]) + " and " + words[-1]


@dataclass(frozen=True)
class CalendarRule(Recurrence):
    """A date filter plus times of day (or a windowed interval) in a time zone."""

    months: frozenset[int] = frozenset()
    days: tuple[int, ...] = ()
    weekdays: frozenset[int] = frozenset()
    nth: tuple[int, ...] = ()
    week: int = 1
    anchor: dt.date = _EPOCH_MONDAY
    times: tuple[dt.time, ...] = (dt.time(0),)
    interval_ms: int | None = None
    between: tuple[dt.time, dt.time] | None = None
    exclude: frozenset[dt.date] = frozenset()
    tz: str | None = None
    kind = "calendar"
    zone: dt.tzinfo = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        try:
            zone: dt.tzinfo = ZoneInfo(self.tz) if self.tz else dt.UTC
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError(f"Unknown time zone {self.tz!r}.") from exc
        object.__setattr__(self, "zone", zone)
        if self.nth and not self.weekdays:
            raise ValueError("nth needs weekday, e.g. weekday='mon', nth=1.")
        if self.week < 1:
            raise ValueError("week must be at least 1.")
        if self.interval_ms is not None:
            if self.interval_ms <= 0:
                raise ValueError("Interval must be positive.")
            if self.between is not None and self.between[0] >= self.between[1]:
                raise ValueError("between must be (start, end) with start before end.")
        elif not self.times:
            raise ValueError("at needs at least one time.")

    # -- matching -----------------------------------------------------------

    def date_ok(self, day: dt.date) -> bool:
        """
        Whether the rule fires on a local date.

        :param day: local date.
        :return: True if every given filter matches.
        """
        if self.months and day.month - 1 not in self.months:
            return False
        if self.weekdays and day.weekday() not in self.weekdays:
            return False
        if self.days:
            last = _last_day(day)
            if not any(day.day == (d if d > 0 else last + 1 + d) for d in self.days):
                return False
        if self.nth:
            last = _last_day(day)
            position = (day.day - 1) // 7 + 1
            from_end = -((last - day.day) // 7 + 1)
            if position not in self.nth and from_end not in self.nth:
                return False
        if self.week > 1:
            start = self.anchor - dt.timedelta(days=self.anchor.weekday())
            if ((day - start).days // 7) % self.week:
                return False
        return day not in self.exclude

    def _instant(self, day: dt.date, moment: dt.time) -> int:
        """Unix ms of a local wall time; the first valid instant if it doesn't exist."""
        target = dt.datetime.combine(day, moment)
        later = int(target.replace(tzinfo=self.zone, fold=0).timestamp() * 1000)
        if self._local(later) == target:
            return later
        earlier = int(target.replace(tzinfo=self.zone, fold=1).timestamp() * 1000)
        low, high = min(earlier, later), max(earlier, later)
        if self._local(high) < target:  # pragma: no cover - not a DST gap
            return high
        # The first instant whose wall time is at or after the target.
        low_s, high_s = low // 1000, high // 1000
        while low_s < high_s:
            middle = (low_s + high_s) // 2
            if self._local(middle * 1000) >= target:
                high_s = middle
            else:
                low_s = middle + 1
        return high_s * 1000

    def _local(self, ms: int) -> dt.datetime:
        return dt.datetime.fromtimestamp(ms / 1000, tz=self.zone).replace(tzinfo=None)

    def _candidates(self, day: dt.date) -> list[int]:
        if self.interval_ms is None:
            return sorted({self._instant(day, moment) for moment in self.times})
        start_time, end_time = self.between or (dt.time(0), None)
        start = self._instant(day, start_time)
        end = (
            self._instant(day + dt.timedelta(days=1), dt.time(0))
            if end_time is None
            else self._instant(day, end_time)
        )
        step = self.interval_ms
        first = -(-start // step) * step
        return list(range(first, end, step))

    def next_after(self, after_ms: int) -> int | None:
        """
        First fire time strictly after ``after_ms``.

        :param after_ms: Unix milliseconds.
        :return: Unix milliseconds, or None if the rule never fires.
        """
        day = self._local(after_ms).date() - dt.timedelta(days=1)
        for _ in range(_SEARCH_DAYS):
            if self.date_ok(day):
                for candidate in self._candidates(day):
                    if candidate > after_ms:
                        return candidate
            day += dt.timedelta(days=1)
        return None

    def min_interval_ms(self) -> int | None:
        """Shortest gap between two fires on one day."""
        if self.interval_ms is not None:
            return self.interval_ms
        if len(self.times) < 2:
            return None
        seconds = sorted(t.hour * 3600 + t.minute * 60 + t.second for t in self.times)
        return min(b - a for a, b in itertools.pairwise(seconds)) * 1000

    # -- plain data and text ------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """Plain data."""
        return {
            "kind": "calendar",
            "months": sorted(self.months),
            "days": list(self.days),
            "weekdays": sorted(self.weekdays),
            "nth": list(self.nth),
            "week": self.week,
            "anchor": self.anchor.isoformat(),
            "times": [t.isoformat() for t in self.times],
            "interval_ms": self.interval_ms,
            "between": [t.isoformat() for t in self.between] if self.between else None,
            "exclude": sorted(d.isoformat() for d in self.exclude),
            "tz": self.tz,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CalendarRule":
        """
        Rebuild a rule from :meth:`to_dict` data.

        :param data: plain data.
        :return: rule.
        """
        between = data.get("between")
        return cls(
            months=frozenset(data.get("months") or ()),
            days=tuple(data.get("days") or ()),
            weekdays=frozenset(data.get("weekdays") or ()),
            nth=tuple(data.get("nth") or ()),
            week=int(data.get("week", 1)),
            anchor=dt.date.fromisoformat(
                data.get("anchor") or _EPOCH_MONDAY.isoformat()
            ),
            times=tuple(dt.time.fromisoformat(t) for t in data.get("times") or ()),
            interval_ms=data.get("interval_ms"),
            between=(
                dt.time.fromisoformat(between[0]),
                dt.time.fromisoformat(between[1]),
            )
            if between
            else None,
            exclude=frozenset(
                dt.date.fromisoformat(d) for d in data.get("exclude") or ()
            ),
            tz=data.get("tz"),
        )

    def _weekday_text(self) -> str:
        indexes = sorted(self.weekdays)
        if len(indexes) > 2 and indexes == list(range(indexes[0], indexes[-1] + 1)):
            return f"{_WEEKDAY_NAMES[indexes[0]]} to {_WEEKDAY_NAMES[indexes[-1]]}"
        return _join([_WEEKDAY_NAMES[i] for i in indexes])

    def describe(self) -> str:
        """Human description, e.g. ``"first Monday at 09:00 (Europe/Berlin)"``."""
        parts: list[str] = []
        if self.week > 1:
            parts.append(f"every {self.week} weeks,")
        if self.nth:
            ordinals = _join([_ORDINALS.get(n, f"{n}th") for n in self.nth])
            parts.append(f"{ordinals} {self._weekday_text()}")
        elif self.weekdays:
            parts.append(self._weekday_text())
        if self.days:
            days = _join(["last day" if d == -1 else f"day {d}" for d in self.days])
            parts.append(days if self.months else f"{days} of the month")
        if self.months:
            parts.append("in " + _join([_MONTH_NAMES[m] for m in sorted(self.months)]))
        if not parts:
            parts.append("every day")
        if self.interval_ms is not None:
            window = ""
            if self.between:
                window = f" {_hhmm(self.between[0])}-{_hhmm(self.between[1])}"
            parts.append(f"every {format_duration(self.interval_ms / 1000)}{window}")
        else:
            parts.append("at " + _join([_hhmm(t) for t in self.times]))
        if self.exclude:
            parts.append(f"except {len(self.exclude)} dates")
        text = " ".join(parts)
        return text + (f" ({self.tz})" if self.tz else "")


def _build(
    *,
    month: Any = None,
    day: Any = None,
    weekday: Any = None,
    nth: Any = None,
    week: int = 1,
    anchor: Any = None,
    exclude: Any = None,
    tz: str | None = None,
    **timing: Any,
) -> CalendarRule:
    rule = CalendarRule(
        months=_named(month, _MONTHS, "month", 1) if month is not None else frozenset(),
        days=_ints(day, "day", set(range(1, 32)) | set(range(-31, 0)))
        if day is not None
        else (),
        weekdays=_named(weekday, _WEEKDAYS, "weekday", 1)
        if weekday is not None
        else frozenset(),
        nth=_ints(nth, "nth", {1, 2, 3, 4, 5, -1, -2, -3, -4, -5})
        if nth is not None
        else (),
        week=int(week),
        anchor=_date(anchor) if anchor is not None else _EPOCH_MONDAY,
        exclude=frozenset(_date(d) for d in _items(exclude))
        if exclude is not None
        else frozenset(),
        tz=tz,
        **timing,
    )
    if rule.next_after(int(time.time() * 1000)) is None:
        raise ValueError(f"The calendar rule {rule.describe()!r} never fires.")
    return rule


def calendar_rule(*, at: Any = "00:00", **filters: Any) -> CalendarRule:
    """
    Build a calendar rule.

    :param at: time or times of day, ``"HH:MM[:SS]"`` (default midnight).
    :param filters: ``month``, ``day``, ``weekday``, ``nth``, ``week``,
        ``anchor``, ``exclude``, ``tz`` (see the module docs).
    :raises ValueError: if a value is invalid or the rule never fires.
    :return: rule.
    """
    times = tuple(sorted({_time(t) for t in _items(at)}))
    return _build(times=times, **filters)


def windowed_every(
    interval_ms: int,
    *,
    between: tuple[Any, Any] | None = None,
    **filters: Any,
) -> CalendarRule:
    """
    Every ``interval_ms`` on matching dates, inside a daily window.

    Fire times are aligned to the Unix epoch, as with plain ``every``.

    :param interval_ms: interval.
    :param between: ``(start, end)`` local times; end is exclusive.
    :param filters: date filters as for :func:`calendar_rule`.
    :raises ValueError: if a value is invalid or the rule never fires.
    :return: rule.
    """
    window = (
        (_time(between[0], "between"), _time(between[1], "between"))
        if between
        else None
    )
    if interval_ms > _DAY_MS:
        raise ValueError("A windowed interval can't be longer than a day.")
    return _build(interval_ms=interval_ms, between=window, times=(), **filters)
