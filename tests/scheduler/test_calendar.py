"""
Calendar rules against a brute-force oracle.

The oracle walks every quarter hour of two years in UTC, converts each instant
to local time, and lists the instants a rule must fire at: for each matching
local date and target time, the first instant whose local wall time on that
date is at or after the target. That is "fires at the first valid instant" in
a DST gap and "fires once, at the first occurrence" in a DST overlap. All
targets and all DST jumps in these zones fall on quarter hours.
"""

import datetime as dt
from collections.abc import Callable
from functools import cache
from zoneinfo import ZoneInfo

import pytest

from syntasks.scheduler.calendar import CalendarRule, calendar_rule, windowed_every
from syntasks.scheduler.recurrence import Recurrence

ZONES = [
    "UTC",
    "Europe/Berlin",
    "America/New_York",
    "Australia/Lord_Howe",
    "Asia/Dhaka",
]
START = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
END = dt.datetime(2028, 1, 1, tzinfo=dt.UTC)
QUARTER = dt.timedelta(minutes=15)


@cache
def quarter_hours(zone: str) -> list[tuple[int, dt.datetime]]:
    """(unix ms, local naive time) for every quarter hour in the range."""
    tz = ZoneInfo(zone)
    out = []
    moment = START
    while moment < END:
        out.append(
            (int(moment.timestamp() * 1000), moment.astimezone(tz).replace(tzinfo=None))
        )
        moment += QUARTER
    return out


def oracle(
    zone: str, date_ok: Callable[[dt.date], bool], times: list[dt.time]
) -> list[int]:
    expected: list[int] = []
    seen: set[tuple[dt.date, dt.time]] = set()  # repeated hours fire once
    previous = (START - QUARTER).astimezone(ZoneInfo(zone)).replace(tzinfo=None)
    for ms, local in quarter_hours(zone):
        day = local.date()
        if date_ok(day):
            for target in times:
                # A crossing: this quarter hour is at or after the target and the
                # one before it was earlier that day (or on another day).
                before = previous.date() != day or previous.time() < target
                if local.time() >= target and before and (day, target) not in seen:
                    seen.add((day, target))
                    expected.append(ms)
        previous = local
    return sorted(set(expected))


def fires(rule: Recurrence) -> list[int]:
    out: list[int] = []
    after = int(START.timestamp() * 1000) - 1
    end = int(END.timestamp() * 1000)
    while True:
        nxt = rule.next_after(after)
        if nxt is None or nxt >= end:
            return out
        out.append(nxt)
        after = nxt


def last_day(day: dt.date) -> int:
    following = (day.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    return (following - dt.timedelta(days=1)).day


def t(text: str) -> dt.time:
    return dt.time.fromisoformat(text)


CASES: list[tuple[str, dict[str, object], Callable[[dt.date], bool], list[dt.time]]] = [
    (
        "sunday 09:00",
        {"weekday": "sun", "at": "09:00"},
        lambda d: d.weekday() == 6,
        [t("09:00")],
    ),
    (
        "first monday",
        {"weekday": "mon", "nth": 1, "at": "09:00"},
        lambda d: d.weekday() == 0 and d.day <= 7,
        [t("09:00")],
    ),
    (
        "last friday",
        {"weekday": "fri", "nth": -1, "at": "17:30"},
        lambda d: d.weekday() == 4 and d.day > last_day(d) - 7,
        [t("17:30")],
    ),
    (
        "last day",
        {"day": -1, "at": "23:00"},
        lambda d: d.day == last_day(d),
        [t("23:00")],
    ),
    (
        "weekdays twice",
        {"weekday": "mon-fri", "at": ["09:00", "13:00"]},
        lambda d: d.weekday() < 5,
        [t("09:00"), t("13:00")],
    ),
    (
        "christmas",
        {"month": "dec", "day": 25, "at": "08:00"},
        lambda d: (d.month, d.day) == (12, 25),
        [t("08:00")],
    ),
    (
        "weekdays except holidays",
        {"weekday": "mon-fri", "at": "09:00", "exclude": ["2026-12-25", "2027-01-01"]},
        lambda d: (
            d.weekday() < 5 and d not in (dt.date(2026, 12, 25), dt.date(2027, 1, 1))
        ),
        [t("09:00")],
    ),
    (
        "every 2nd tuesday",
        {"week": 2, "weekday": "tue", "at": "10:00", "anchor": "2026-01-05"},
        lambda d: d.weekday() == 1 and ((d - dt.date(2026, 1, 5)).days // 7) % 2 == 0,
        [t("10:00")],
    ),
    # 02:30 doesn't exist on spring-forward days and exists twice on fall-back days.
    ("dst edge 02:30", {"at": "02:30"}, lambda d: True, [t("02:30")]),
    ("1st at midnight", {"day": 1, "at": "00:00"}, lambda d: d.day == 1, [t("00:00")]),
]


@pytest.mark.parametrize("zone", ZONES)
@pytest.mark.parametrize(
    ("name", "rule", "date_ok", "times"), CASES, ids=[c[0] for c in CASES]
)
def test_calendar_matches_oracle(
    zone: str,
    name: str,
    rule: dict[str, object],
    date_ok: Callable[[dt.date], bool],
    times: list[dt.time],
) -> None:
    built = calendar_rule(tz=zone, **rule)  # type: ignore[arg-type]
    assert fires(built) == oracle(zone, date_ok, times)


@pytest.mark.parametrize("zone", ZONES)
def test_windowed_every_matches_oracle(zone: str) -> None:
    rule = windowed_every(
        15 * 60 * 1000, weekday="mon-fri", between=("09:00", "18:00"), tz=zone
    )
    expected = [
        ms
        for ms, local in quarter_hours(zone)
        if local.weekday() < 5 and t("09:00") <= local.time() < t("18:00")
    ]
    assert fires(rule) == expected


def test_never_fires_rejected() -> None:
    with pytest.raises(ValueError, match="never fires"):
        calendar_rule(month="feb", day=30, at="09:00")


def test_day_and_weekday_both_must_match() -> None:
    rule = calendar_rule(day=13, weekday="fri", at="00:00")
    found = fires(rule)
    days = [dt.datetime.fromtimestamp(ms / 1000, tz=dt.UTC).date() for ms in found]
    assert days == [
        dt.date(2026, 2, 13),
        dt.date(2026, 3, 13),
        dt.date(2026, 11, 13),
        dt.date(2027, 8, 13),
    ]


def test_invalid_values_rejected() -> None:
    with pytest.raises(ValueError, match="weekday"):
        calendar_rule(weekday="funday", at="09:00")
    with pytest.raises(ValueError, match="nth"):
        calendar_rule(nth=1, at="09:00")
    with pytest.raises(ValueError, match="at"):
        calendar_rule(weekday="mon", at="25:00")


def test_round_trip_and_describe() -> None:
    rule = calendar_rule(weekday="mon", nth=1, at="09:00", tz="Europe/Berlin")
    assert isinstance(rule, CalendarRule)
    assert CalendarRule.from_dict(rule.to_dict()) == rule
    assert rule.describe() == "first Monday at 09:00 (Europe/Berlin)"
    assert calendar_rule(weekday="sun", at="09:00").describe() == "Sunday at 09:00"
    assert (
        calendar_rule(day=-1, at="23:00").describe() == "last day of the month at 23:00"
    )
