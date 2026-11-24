# Python API

Everything below is importable from `syntasks` unless a path is given. The
guides explain when to use what; this page lists what exists.

## `SynTasks`

```python
from syntasks import SynTasks

app = SynTasks()                                   # syntasks.toml, $SYNTASKS_CONFIG or [tool.syntasks]
app = SynTasks("deploy/syntasks.toml", profile="production")
app = SynTasks(Config(...))                        # built in code
app = SynTasks(name="tests-42", redis_url="redis://127.0.0.1:6390/0", search_config=False)
```

| Member | Does |
| --- | --- |
| `@app.task` / `@app.task(name, **options, schedule=...)` | register a task ([Tasks](../guides/tasks.md)); other keywords become labels |
| `@app.channel("room:{room_id}", idle_timeout=, max_lifetime=, max_restarts=)` | register a channel handler ([Channels](../guides/channels.md)) |
| `await app.send(name, *args, **kwargs)` | send a task by name, without importing it |
| `app.kicker(name)` | a kicker by name, to set queue, priority, delay or timeout first |
| `app.group(group_id=None)` | a `TaskGroup` ([below](#taskgroup)) |
| `await app.startup()` / `await app.shutdown()` | open / close connections (sending code calls these; workers do it themselves) |
| `await app.run_worker(stop, processes=1, process_index=0)` | run a worker inside this process until `stop` is set (tests, embedding) |
| `await app.nodes()` | `NodeInfo` of every worker process in the cluster |
| `app.options_for(name)` | a task's effective `TaskOptions` after defaults, code and config |
| `app.config` | the loaded `Config` |
| `app.queues` | `QueueManager` |
| `app.schedules` | `ScheduleManager` |
| `app.channels` | `ChannelManager` |
| `app.tasks` | `TaskControl` |
| `app.logs` | `LogManager` |
| `app.on_event(event)`, `app.add_middlewares(...)`, `app.dependency_overrides` | as on every broker ([Dependencies](../guides/dependencies.md), [Middlewares](../guides/middlewares.md)) |

## Tasks

A decorated function stays callable (`await add(1, 2)` runs it in place).

| Member | Returns / does |
| --- | --- |
| `await add.kiq(*args, **kwargs)` | send; returns an `AsyncSynTasksTask` handle |
| `add.kicker().with_queue(q).with_priority(n).with_delay(s).with_timeout(s).with_labels(...)` | per-call overrides, then `.kiq(...)` |
| `await add.schedule(**options)` | create a dynamic schedule for this task (same options as `app.schedules.create`) |

Handle (`AsyncSynTasksTask`):

| Member | Returns / does |
| --- | --- |
| `task.task_id` | the id |
| `await task.wait_result(timeout=-1, with_logs=False)` | `SynTasksResult`; pushed by Redis, no polling; `SynTasksResultTimeoutError` on timeout |
| `await task.is_ready()` / `await task.get_result()` | check / read without waiting |
| `task.watch_progress(timeout=None)` | async iterator of progress updates until the task ends |
| `await task.get_progress()` | the latest progress, or `None` |

`SynTasksResult`: `return_value`, `is_err`, `error`, `execution_time`,
`labels`, `log`; `raise_for_error()` re-raises the task's exception.

`await gather(*tasks, timeout=-1)` waits for several handles.

## `TaskGroup`

```python
group = app.group()
for url in urls:
    await group.kiq(fetch, url)
results = await group.wait(timeout=60)              # all, in send order
async for task, result in group.as_completed():    # as they finish
    ...
first = await group.first(2)                        # the first two to finish
```

## Schedules

Declared with a task: `schedule=every("10s")`, `cron("*/5 * * * *", tz="Europe/Berlin")`,
`at("2026-12-31T23:59:00Z")`, `after("90s")`, or a list of them. Each takes
`id=`, `args=`, `kwargs=` and the schedule options below.

`app.schedules` (`ScheduleManager`), all `async`:

| Member | Returns / does |
| --- | --- |
| `create(task, *, args=(), kwargs=None, every=, cron=, tz=, at=, after=, id=None, ttl=None, expires_at=None, if_exists="error", **options)` | a new dynamic `Schedule`; `if_exists` is `"error"`, `"replace"` or `"ignore"` |
| `get(id)` | `Schedule`; `ScheduleNotFoundError` if missing |
| `list(task=None, source=None, state=None)` | async iterator of `Schedule` |
| `update(id, version=None, **changes)` | changes a dynamic schedule; with `version`, `ScheduleConflictError` if someone changed it first |
| `delete(id)` | deletes a dynamic schedule |
| `pause(id)` / `resume(id)` | for any schedule, kept across deploys |
| `trigger(id)` | runs its task now; returns the handle |
| `runs(id, limit=20)` | recent runs, newest first |
| `cancel_running(id)` | cancels its runs executing now |

Options: `misfire` (`"skip"`, `"once"`, `"all"`), `misfire_grace`,
`max_catchup`, `overlap` (`"skip"`, `"allow"`, `"queue"`), `max_instances`,
`jitter`, `start_at`, `end_at`, `max_runs`, `pause_after_failures`,
`timeout`, `retries`, `queue`, `priority`, `labels`, `paused`.

`Schedule` has `spec` (the definition), `source` (`code`, `config`, `api`),
`state`, `version`, `next_ms`, `runs`, `failures`, `last_status`,
`last_error`, `expires_ms`. Errors derive from `ScheduleError`;
`ScheduleNotFoundError`, `ScheduleConflictError` and `StaticScheduleError`
are in `syntasks.scheduler.manager`.

A task run by a schedule can ask for `ScheduleInfo` (`schedule_id`,
`fire_time`, `run_number`, `attempt`).

## Channels

Handler side (`Channel`, given to the function registered with `@app.channel`):

| Member | Does |
| --- | --- |
| `ch.name`, `ch.params` | the channel name and the values matched by the pattern |
| `ch.inbox(batch=10)` | async iterator of `ChannelMessage` sent by clients |
| `await ch.publish(data, kind="message")` | broadcast to every reader |
| `ch.state`, `await ch.save_state()` | a dict kept in Redis; a takeover after a crash loads it first |
| `await ch.readers()` | readers connected now |
| `await ch.sleep(seconds)` | sleep, returning early if the channel is lost |

Client side (`app.channels`, `ChannelManager`):

| Member | Does |
| --- | --- |
| `async with app.channels.connect(name, last_id=None) as conn` | a reader; `async for msg in conn`, `await conn.receive(timeout)`, `await conn.send(data)` |
| `await app.channels.send(name, data)` | send to the channel's handler (its inbox), starting it if needed |
| `await app.channels.publish(name, data)` | broadcast to readers directly |
| `app.channels.consume(name, group)` | async iterator that shares messages among many consumers |
| `await app.channels.history(name, after=None, limit=100)` | messages still kept |
| `await app.channels.info(name)` / `list(prefix="")` | `ChannelInfo` |
| `await app.channels.close(name)` / `restart(name)` | close it / clear a failed handler |

`ChannelMessage`: `id`, `data`, `sender`, `kind`, `time_ms`.

## Queues

`app.queues` (`QueueManager`), all `async`: `names()`, `list()`,
`info(q)`, `peek(q, limit=10)`, `pause(q)`, `resume(q)`, `purge(q)`,
`delete_message(q, id)`, `dead_letters(q, limit=50)`,
`replay_dead(q, ids=None)`, `purge_dead(q)`, `move(source, target)`.
They return `QueueInfo`, `QueuedMessage` and `DeadLetter`. See
[Queues](../guides/queues.md).

## Running tasks and logs

| Member | Does |
| --- | --- |
| `await app.tasks.running()` | tasks running now (`task_id`, `task_name`, `node_id`) |
| `await app.tasks.cancel(task_id)` | cancel it wherever it runs; the run ends with `TaskCancelledError` |
| `app.tasks.handle(task_id)` | a handle for a task id, to wait for its result |
| `app.logs.read(task_id, follow=False)` | async iterator of captured `LogRecord`s |

## Inside a task

| Ask for | Get |
| --- | --- |
| `Deadline` | `remaining()`, `soft_remaining()`, `elapsed`, `soft_expired`, `expired` |
| `Context` | `message`, `broker`, `state`; `await context.requeue()`, `context.reject()` |
| `ScheduleInfo` | see [Schedules](#schedules) |
| `ProgressTracker[T]` (`syntasks.depends.progress_tracker`) | `await set_progress(state, meta=None)` |

## Config

```python
from syntasks import Config, load_config

config = load_config("syntasks.toml", profile="production", overrides={"worker.processes": 4})
```

`load_config(path=None, *, profile=None, overrides=None, env=None, use_env=True, search=True)`
raises `ConfigError` with every problem found. Every setting is in the
[configuration reference](config.md).

## Errors

All derive from `SynTasksError`.

| Error | Raised when |
| --- | --- |
| `SendTaskError` → `QueueFullError` | a queue is at `max_length` and `on_full = "reject"` |
| `SendTaskError` → `DuplicateTaskError` | (internal) a `unique_for` task is already queued; `kiq` returns the first one's handle instead |
| `PayloadTooLargeError`, `PayloadInvalidError` | arguments too big for `max_payload`, or not JSON |
| `TaskTimeoutError`, `TaskMemoryError`, `TaskCancelledError` | stored as the result's `error` when a run is stopped |
| `ResultTooLargeError` | a return value is over `max_result` |
| `SynTasksResultTimeoutError` | `wait_result` timed out |
| `ScheduleError` and subclasses | schedule CRUD failed |
| `ConfigError` | the config is invalid |

## Other brokers

`InMemoryBroker` runs tasks in-process (unit tests; no delays or
schedules). `RedisStreamBroker`, `RedisListBroker`, `RedisPubSubBroker`,
`RedisResultBackend` and `MultiBroker` are the lower-level building blocks
kept from taskiq; a `SynTasks` app uses its own engine and doesn't need them.
