---
order: 7
---

# Scheduling

Schedules live in Redis and are fired by the workers themselves; there is
no separate scheduler process. Each fire becomes an ordinary task message,
so it runs once, on one free worker, with the task's retries, limits and
timeouts.

## Declared schedules

```python
from syntasks import cron, every

@app.task(schedule=every("10s"))
async def poll_prices() -> None: ...


@app.task(schedule=[
    cron("0 30 9 * * mon-fri", tz="Europe/London", id="open"),  # 6 fields: with seconds
    every("10m", id="incremental", kwargs={"full": False}),
])
async def sync_rates(full: bool = True) -> None: ...
```

or in config:

```toml
[schedules.nightly-report]
task = "billing.tasks:report"
cron = "0 0 2 * * *"
tz = "UTC"
kwargs = { format = "pdf" }
timeout = "20m"
```

Every worker syncs the declared schedules into Redis when it starts:
new ones are added, changed ones updated (a paused schedule stays paused,
counters are kept), removed ones deleted (`scheduler.prune_removed`). Set
`[app] revision` (e.g. your build number) so that during a rolling deploy
an old worker can't bring back schedules the new code removed.

Declared schedules can be paused, resumed and triggered at runtime, but
not changed or deleted through the API: change the code or the file.

## When they fire

| Recurrence | Example | Notes |
|---|---|---|
| `every(interval)` | `every("1s")`, `every("250ms")` | aligned to the Unix epoch (or `anchor=`), so fires don't drift across restarts |
| `cron(expr, tz=)` | `cron("*/5 * * * * *")` | 5 fields, or 6 with seconds first; time zones with correct DST handling |
| `at(when)` | `at("2026-12-01T09:00Z")` | once |
| `after(delay)` | dynamic schedules only | once, after a delay |

Fires reach the queue within tens of milliseconds of their due time.

## Dynamic schedules

Programs create, change and delete schedules while running:

```python
schedule = await app.schedules.create(
    check_price,                         # task or task name
    kwargs={"symbol": "AAPL", "above": 250},
    every="1s",
    ttl="2h",                            # or expires_at=, end_at=, max_runs=
    id="alert:user42:AAPL",              # optional
    if_exists="replace",                 # error | replace | ignore
)
await check_price.schedule(every="5s", kwargs={...})   # shortcut

await app.schedules.get(schedule.id)
async for s in app.schedules.list(task="alerts:check_price", state="active"): ...
await app.schedules.update(schedule.id, every="5s", version=schedule.version)
await app.schedules.pause(schedule.id)
await app.schedules.resume(schedule.id)
await app.schedules.trigger(schedule.id)       # run now, timing unchanged
await app.schedules.runs(schedule.id)          # recent runs
await app.schedules.cancel_running(schedule.id)
await app.schedules.delete(schedule.id)
```

- Arguments must be JSON, at most `scheduler.max_payload` (16 KiB), and
  are checked against the task's signature when the schedule is created.
- `update(..., version=)` fails with `ScheduleConflictError` if someone
  changed the schedule since you read it.
- `scheduler.min_interval` (1 s) is the shortest allowed interval for
  dynamic schedules; `scheduler.max_dynamic` caps how many exist.
- Expired and finished dynamic schedules are deleted by the cleanup job.

## Policies

| Option | Values | Meaning |
|---|---|---|
| `misfire` | `once` (default), `skip`, `all` | fires missed while the cluster was down: run one now, none, or each (up to `max_catchup`) |
| `misfire_grace` | duration | how late a fire may be and still count as on time |
| `overlap` | `skip` (default), `allow`, `queue` | when the previous run is still going: drop this one, run both, or wait (`max_instances`) |
| `jitter` | duration | random delay added to each fire |
| `start_at`, `end_at` | time | window in which it fires |
| `max_runs`, `ttl`, `expires_at` | | end of a dynamic schedule |
| `timeout`, `retries` | | override the task's for these runs |
| `queue`, `priority`, `labels` | | routing of the fired messages |
| `pause_after_failures` | int | pause the schedule after N failures in a row |

## In the task

```python
from syntasks import Depends, ScheduleInfo

@app.task(schedule=every("1m"))
async def report(info: ScheduleInfo = Depends()) -> None:
    info.schedule_id, info.fire_time, info.run_number, info.late_by
```

## How it works

The worker holding the dispatcher lease sleeps until the next due time,
claims due schedules in one script, computes their next fire, and
commits the task messages and the new due time in a second script. If the
dispatcher dies between the two, the claim expires and the next
dispatcher sends the same fire with the same task id: late, but never lost
and never twice. See [Cluster](cluster.md).
