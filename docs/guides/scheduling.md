# Scheduling

Schedules live in Redis and are fired by the workers themselves; there is
no separate scheduler process. Each fire becomes an ordinary task message,
so it runs once, on one free worker, with the task's retries, limits and
timeouts.

## Declared schedules

```python
from syntasks import calendar, cron, every


@app.schedule(every("10s"), timeout="5s", retries=0, max_tasks=1)
async def poll_prices() -> None: ...


@app.schedule(
    [
        calendar(weekday="mon-fri", at="09:30", tz="Europe/London", id="open"),
        every("10m", id="incremental", kwargs={"full": False}),
    ],
    timeout="10m",
    retries=2,
    max_tasks=1,
)
async def sync_rates(full: bool = True) -> None: ...
```

`@app.schedule(timing, ...)` (also on every [module](modules.md)) makes
the function a task with the required limits, and declares its schedules.
The function can still be sent by hand (`.kiq()`). A schedule's id is the
task name, or `<task>:<n>` when there are several timings, unless you
give `id=`.

Every worker syncs the declared schedules into Redis when it starts: new
ones are added, changed ones updated (a paused schedule stays paused,
counters are kept), removed ones deleted (`[scheduler] prune_removed`).
Set `[app] revision` (e.g. your build number) so that during a rolling
deploy an old worker can't bring back schedules the new code removed.

Declared schedules can be paused, resumed and triggered at runtime, but
not changed or deleted through the API: change the code. To let your
users create schedules, see [Runtime schedules](runtime-schedules.md).

## Timings

| Timing | Example | Fires |
|---|---|---|
| `every(interval)` | `every("1s")`, `every("250ms")` | aligned to the Unix epoch (or `anchor=`), so fires don't drift across restarts |
| `every(interval, weekday=, between=)` | `every("15m", weekday="mon-fri", between=("09:00", "18:00"))` | only on those days, inside that daily window |
| `cron(expr, tz=)` | `cron("0 9 * * mon#1")`, `cron("*/5 * * * * *")` | 5 fields, or 6 with seconds first; `L` (last day), `fri#2`, `fri#L` |
| `calendar(...)` | below | weekdays, nth weekdays, days of the month, months, times of day |
| `at(when)` | `at("2026-12-01T09:00Z")` | once |

### Calendar rules

```python
calendar(weekday="sun", at="09:00")                          # 09:00 every Sunday
calendar(weekday="mon", nth=1, at="09:00")                   # first Monday of each month
calendar(weekday="fri", nth=-1, at="17:30")                  # last Friday
calendar(day=1, at="00:05")                                  # the 1st of the month
calendar(day=-1, at="23:00")                                 # last day of the month
calendar(weekday="mon-fri", at=["09:00", "13:00"])           # weekdays, twice
calendar(month="dec", day=25, at="08:00")                    # once a year
calendar(weekday="mon-fri", at="09:00", exclude=["2026-12-25", "2027-01-01"])
calendar(week=2, weekday="tue", at="10:00", anchor="2026-01-06")  # every 2nd Tuesday
```

| Field | Values |
|---|---|
| `at` | `"HH:MM"` or `"HH:MM:SS"`, or a list |
| `weekday` | `"mon"`, `"mon-fri"`, `"sat,sun"`, `["mon", "wed"]`; ranges wrap (`"fri-mon"`) |
| `nth` | which weekday of the month: `1`-`5`, or `-1` for the last (with `weekday`) |
| `day` | days of the month, `1`-`31`; negative counts from the end (`-1` is the last day) |
| `month` | `"jan"`-`"dec"`, `"jan-mar"`, or `1`-`12` |
| `week`, `anchor` | every N weeks, counted from the week of `anchor` (default 1970-01-05) |
| `exclude` | dates to skip, `"YYYY-MM-DD"` |
| `tz` | IANA time zone, default UTC |

- **Every filter you give must match.** `day=13, weekday="fri"` is Friday
  the 13th (cron would fire on every 13th *and* every Friday).
- **Wall-clock times in `tz`.** A time skipped when clocks go forward
  fires at the first valid instant after it; a time repeated when clocks
  go back fires once, the first time.
- **Checked when declared.** A rule that never fires (`month="feb",
  day=30`) is an error.
- **Preview:** `syntasks schedules preview ID -n 10`, or
  `schedule.spec.next_times(10)` in code.

## Missed fires run as soon as possible

A fire is **missed** when the cluster was down or behind: a newer fire
time has passed too, or it is dispatched later than
`[scheduler] late_after` (30 s). The dispatcher checks every round, not
only at start, so a missed fire runs as soon as any worker is back.

| `misfire` | After downtime |
|---|---|
| `"once"` (default) | run once now, then continue on the normal times |
| `"all"` | run every missed time, up to `max_catchup` (`[scheduler] max_catchup`, 10) |
| `"skip"` | run none; wait for the next time |

`misfire_grace` (per schedule, default unlimited) drops missed times
older than it, whatever the policy: set it for things that are useless
late, like "remind 5 minutes before the meeting". Runtime schedules
follow the same rules: a one-shot `at` whose time passed while everything
was down fires when a dispatcher sees it.

The run can tell: `ScheduleInfo.scheduled_for` and `late_by` (seconds).
`syntasks schedules runs ID` shows each run's lateness, and
`syntasks schedules list` shows schedules that are overdue now.

## Policies

| Option | Values | Meaning |
|---|---|---|
| `misfire`, `misfire_grace`, `max_catchup` | see above | missed fires |
| `overlap` | `skip` (default), `allow`, `queue` | when the previous run is still going: drop this one, run both, or wait (`max_instances`) |
| `jitter` | duration | random delay added to each fire |
| `start_at`, `end_at` | time | window in which it fires |
| `max_runs` | int | stop after N runs |
| `pause_after_failures` | int | pause the schedule after N failures in a row |
| `paused` | bool | declare it paused |

## In the task

```python
from typing import Annotated

from syntasks import Depends, ScheduleInfo


@app.schedule(every("1m"), timeout="30s", retries=0, max_tasks=1)
async def report(info: Annotated[ScheduleInfo, Depends()]) -> None:
    info.schedule_id, info.scheduled_for, info.run_number, info.late_by
```

## Operating schedules

```python
await app.schedules.get(schedule_id)
async for s in app.schedules.list(task="billing.tasks:report"): ...
await app.schedules.pause(schedule_id)          # kept across deploys
await app.schedules.resume(schedule_id)
await app.schedules.trigger(schedule_id)        # run now, timing unchanged
await app.schedules.runs(schedule_id)           # recent runs with late_by
await app.schedules.cancel_running(schedule_id)
```

Or `syntasks schedules list|show|preview|pause|resume|trigger|runs`.

## How it works

The worker holding the dispatcher lease sleeps until the next due time,
claims due schedules in one script, computes their next fire, and commits
the task messages and the new due time in a second script. If the
dispatcher dies between the two, the claim expires and the next
dispatcher sends the same fire with the same task id: late, but never
lost and never twice. See [Cluster](cluster.md).
