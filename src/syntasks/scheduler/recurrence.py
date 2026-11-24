"""
When a schedule fires.

* :func:`every` - fixed interval with millisecond resolution, aligned to an
  anchor (default: the Unix epoch, so ``every("10s")`` fires at :00, :10,
  :20 … on every machine and after every restart); with ``weekday=`` /
  ``between=`` only on those days and in that daily window;
* :func:`cron` - cron expression, 5 fields or 6 with seconds, in a time zone
  (with the Quartz extras ``L`` and ``#``);
* :func:`calendar` - weekdays, nth weekdays, days of the month, months and
  times of day (see :mod:`syntasks.scheduler.calendar`);
* :func:`at` - once, at a given time;
* :func:`after` - once, after a delay from creation.

These functions build *declarations* used with ``@app.schedule(...)``; they
also accept the run options (``id``, ``args``, ``kwargs``, policies).
"""

import datetime as dt
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from syntasks.scheduler.cron import CronExpression
from syntasks.units import format_duration, parse_duration_ms

__all__ = [
    "At",
    "Cron",
    "Every",
    "Recurrence",
    "ScheduleDecl",
    "after",
    "at",
    "calendar",
    "cron",
    "every",
    "parse_time_ms",
    "recurrence_from_dict",
]


def parse_time_ms(value: Any) -> int:
    """
    Parse a point in time into Unix milliseconds.

    :param value: aware datetime (naive = UTC), ISO 8601 text, or Unix seconds.
    :raises ValueError: if the value is not a time.
    :return: Unix milliseconds.
    """
    if isinstance(value, dt.datetime):
        moment = value if value.tzinfo else value.replace(tzinfo=dt.UTC)
        return int(moment.timestamp() * 1000)
    if isinstance(value, bool):
        raise ValueError(f"Invalid time: {value!r}")
    if isinstance(value, (int, float)):
        return int(value * 1000)
    if isinstance(value, str):
        text = value.strip().replace("Z", "+00:00")
        try:
            moment = dt.datetime.fromisoformat(text)
        except ValueError:
            raise ValueError(f"Invalid time {value!r}; use ISO 8601.") from None
        return parse_time_ms(moment)
    raise ValueError(f"Invalid time: {value!r}")


def _iso(ms: int) -> str:
    return (
        dt.datetime.fromtimestamp(ms / 1000, tz=dt.UTC)
        .isoformat()
        .replace("+00:00", "Z")
    )


class Recurrence(ABC):
    """A rule producing fire times."""

    kind = ""

    @abstractmethod
    def next_after(self, after_ms: int) -> int | None:
        """
        First fire time strictly after ``after_ms``.

        :param after_ms: Unix milliseconds.
        :return: Unix milliseconds, or None if it never fires again.
        """

    @abstractmethod
    def to_dict(self) -> dict[str, Any]:
        """Plain data."""

    @abstractmethod
    def describe(self) -> str:
        """Short human description."""

    def min_interval_ms(self) -> int | None:
        """Shortest gap between fires, if it can be known cheaply."""
        return None


@dataclass(frozen=True)
class Every(Recurrence):
    """Fire every ``interval_ms``, aligned to ``anchor_ms``."""

    interval_ms: int
    anchor_ms: int = 0
    kind = "every"

    def __post_init__(self) -> None:
        if self.interval_ms <= 0:
            raise ValueError("Interval must be positive.")

    def next_after(self, after_ms: int) -> int | None:
        """
        Next aligned fire time.

        :param after_ms: Unix milliseconds.
        :return: Unix milliseconds.
        """
        steps = (after_ms - self.anchor_ms) // self.interval_ms + 1
        return self.anchor_ms + steps * self.interval_ms

    def to_dict(self) -> dict[str, Any]:
        """Plain data."""
        return {
            "kind": "every",
            "interval_ms": self.interval_ms,
            "anchor_ms": self.anchor_ms,
        }

    def describe(self) -> str:
        """Human description."""
        return f"every {format_duration(self.interval_ms / 1000)}"

    def min_interval_ms(self) -> int | None:
        """The interval."""
        return self.interval_ms


@dataclass(frozen=True)
class Cron(Recurrence):
    """Fire on a cron expression in a time zone."""

    expression: str
    tz: str | None = None
    kind = "cron"
    parsed: CronExpression = field(init=False, repr=False, compare=False)
    zone: dt.tzinfo = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "parsed", CronExpression.parse(self.expression))
        try:
            zone: dt.tzinfo = ZoneInfo(self.tz) if self.tz else dt.UTC
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError(f"Unknown time zone {self.tz!r}.") from exc
        object.__setattr__(self, "zone", zone)

    def next_after(self, after_ms: int) -> int | None:
        """
        Next matching time.

        :param after_ms: Unix milliseconds.
        :return: Unix milliseconds or None.
        """
        moment = dt.datetime.fromtimestamp(after_ms / 1000, tz=dt.UTC)
        found = self.parsed.next_after(moment, self.zone)
        return None if found is None else int(found.timestamp() * 1000)

    def to_dict(self) -> dict[str, Any]:
        """Plain data."""
        return {"kind": "cron", "expression": self.expression, "tz": self.tz}

    def describe(self) -> str:
        """Human description."""
        return f"cron {self.expression}" + (f" ({self.tz})" if self.tz else "")

    def min_interval_ms(self) -> int | None:
        """One second for 6-field expressions firing every second, else None."""
        if self.parsed.has_seconds and len(self.parsed.seconds) == 60:
            return 1000
        return None


@dataclass(frozen=True)
class At(Recurrence):
    """Fire once at ``when_ms``."""

    when_ms: int
    kind = "at"

    def next_after(self, after_ms: int) -> int | None:
        """
        The time, if still ahead.

        :param after_ms: Unix milliseconds.
        :return: Unix milliseconds or None.
        """
        return self.when_ms if self.when_ms > after_ms else None

    def to_dict(self) -> dict[str, Any]:
        """Plain data."""
        return {"kind": "at", "when_ms": self.when_ms}

    def describe(self) -> str:
        """Human description."""
        return f"at {_iso(self.when_ms)}"


def recurrence_from_dict(data: dict[str, Any]) -> Recurrence:
    """
    Rebuild a recurrence from :meth:`Recurrence.to_dict` data.

    :param data: plain data.
    :raises ValueError: on unknown kinds.
    :return: recurrence.
    """
    kind = data.get("kind")
    if kind == "every":
        return Every(int(data["interval_ms"]), int(data.get("anchor_ms", 0)))
    if kind == "cron":
        return Cron(str(data["expression"]), data.get("tz"))
    if kind == "at":
        return At(int(data["when_ms"]))
    if kind == "calendar":
        from syntasks.scheduler.calendar import CalendarRule  # noqa: PLC0415

        return CalendarRule.from_dict(data)
    raise ValueError(f"Unknown recurrence kind: {kind!r}")


@dataclass(kw_only=True)
class ScheduleDecl:
    """A timing with its run options (from :func:`every`, :func:`cron`, ...)."""

    recurrence: Recurrence | None = None
    after_ms: int | None = None
    id: str | None = None
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    options: dict[str, Any] = field(default_factory=dict)


def _decl(
    recurrence: Recurrence | None,
    *,
    id: str | None,
    args: Any,
    kwargs: Any,
    options: dict[str, Any],
    after_ms: int | None = None,
) -> ScheduleDecl:
    return ScheduleDecl(
        recurrence=recurrence,
        after_ms=after_ms,
        id=id,
        args=list(args or ()),
        kwargs=dict(kwargs or {}),
        options=options,
    )


_CALENDAR_FILTERS = ("month", "day", "weekday", "nth", "week", "exclude")


def every(
    interval: Any,
    *,
    anchor: Any = None,
    between: tuple[Any, Any] | None = None,
    tz: str | None = None,
    id: str | None = None,
    args: Any = None,
    kwargs: Any = None,
    **options: Any,
) -> ScheduleDecl:
    """
    Fire every ``interval`` (``"1s"``, ``"250ms"``, ``"5m"``, seconds, timedelta).

    With any of ``between``, ``weekday``, ``day``, ``nth``, ``month``,
    ``week`` or ``exclude`` it fires only inside that daily window and on
    those dates, in ``tz``: ``every("15m", weekday="mon-fri",
    between=("09:00", "18:00"))``.

    :param interval: interval.
    :param anchor: time the fires align to (default: the Unix epoch).
    :param between: ``(start, end)`` local times of a daily window.
    :param tz: time zone of the window and dates (default UTC).
    :param id: schedule id.
    :param args: task arguments.
    :param kwargs: task keyword arguments.
    :param options: date filters, then policies (``misfire``, ``overlap``, ...).
    :return: declaration.
    """
    interval_ms = parse_duration_ms(interval)
    filters = {name: options.pop(name) for name in _CALENDAR_FILTERS if name in options}
    recurrence: Recurrence
    if filters or between is not None or tz is not None:
        from syntasks.scheduler.calendar import windowed_every  # noqa: PLC0415

        if anchor is not None:
            raise ValueError("anchor can't be combined with a window or date filters.")
        recurrence = windowed_every(interval_ms, between=between, tz=tz, **filters)
    else:
        anchor_ms = 0 if anchor is None else parse_time_ms(anchor)
        recurrence = Every(interval_ms, anchor_ms)
    return _decl(recurrence, id=id, args=args, kwargs=kwargs, options=options)


def calendar(
    *,
    at: Any = "00:00",
    month: Any = None,
    day: Any = None,
    weekday: Any = None,
    nth: Any = None,
    week: int = 1,
    anchor: Any = None,
    exclude: Any = None,
    tz: str | None = None,
    id: str | None = None,
    args: Any = None,
    kwargs: Any = None,
    **options: Any,
) -> ScheduleDecl:
    """
    Fire on calendar dates at times of day.

    ``calendar(weekday="sun", at="09:00")``, ``calendar(weekday="mon",
    nth=1, at="09:00")`` (first Monday), ``calendar(day=-1, at="23:00")``
    (last day of the month), ``calendar(month="dec", day=25, at="08:00")``.
    Every filter given must match. See :mod:`syntasks.scheduler.calendar`.

    :param at: time or times of day, ``"HH:MM[:SS]"``.
    :param month: months (names, ``"jan-mar"``, 1-12).
    :param day: days of the month; negative counts from the end.
    :param weekday: weekdays (``"mon"``, ``"mon-fri"``, ``"sat,sun"``).
    :param nth: which weekday of the month (1-5, or -1 for the last).
    :param week: every N weeks, counted from ``anchor``.
    :param anchor: a date in the first week for ``week`` (default 1970-01-05).
    :param exclude: dates to skip (``"YYYY-MM-DD"`` or dates).
    :param tz: IANA time zone (default UTC).
    :param id: schedule id.
    :param args: task arguments.
    :param kwargs: task keyword arguments.
    :param options: policies.
    :raises ValueError: if a value is invalid or the rule never fires.
    :return: declaration.
    """
    from syntasks.scheduler.calendar import calendar_rule  # noqa: PLC0415

    rule = calendar_rule(
        at=at,
        month=month,
        day=day,
        weekday=weekday,
        nth=nth,
        week=week,
        anchor=anchor,
        exclude=exclude,
        tz=tz,
    )
    return _decl(rule, id=id, args=args, kwargs=kwargs, options=options)


def cron(
    expression: str,
    *,
    tz: str | None = None,
    id: str | None = None,
    args: Any = None,
    kwargs: Any = None,
    **options: Any,
) -> ScheduleDecl:
    """
    Fire on a cron expression (5 fields, or 6 with seconds first).

    :param expression: cron expression or macro (``@hourly``, ...).
    :param tz: IANA time zone, e.g. ``"Europe/London"`` (default UTC).
    :param id: schedule id.
    :param args: task arguments.
    :param kwargs: task keyword arguments.
    :param options: policies.
    :return: declaration.
    """
    return _decl(Cron(expression, tz), id=id, args=args, kwargs=kwargs, options=options)


def at(
    when: Any,
    *,
    id: str | None = None,
    args: Any = None,
    kwargs: Any = None,
    **options: Any,
) -> ScheduleDecl:
    """
    Fire once at ``when`` (datetime, ISO 8601 text, or Unix seconds).

    :param when: time.
    :param id: schedule id.
    :param args: task arguments.
    :param kwargs: task keyword arguments.
    :param options: policies.
    :return: declaration.
    """
    return _decl(
        At(parse_time_ms(when)),
        id=id,
        args=args,
        kwargs=kwargs,
        options=options,
    )


def after(
    delay: Any,
    *,
    id: str | None = None,
    args: Any = None,
    kwargs: Any = None,
    **options: Any,
) -> ScheduleDecl:
    """
    Fire once, ``delay`` after the schedule is created.

    :param delay: duration.
    :param id: schedule id.
    :param args: task arguments.
    :param kwargs: task keyword arguments.
    :param options: policies.
    :return: declaration.
    """
    return _decl(
        None,
        id=id,
        args=args,
        kwargs=kwargs,
        options=options,
        after_ms=parse_duration_ms(delay),
    )
