# Python API

Everything below is importable from `syntasks` unless a path is given. The
guides explain when to use what; this page lists what exists.

## `SynTasks` and `Module`

```python
from syntasks import Module, SynTasks

app = SynTasks(timeout="5m", retries=0, max_tasks=None)    # + syntasks.toml
app = SynTasks("deploy/syntasks.toml", profile="production", timeout="1m", retries=2, max_tasks=100)
app = SynTasks(Config(...), name="tests-42", redis_url="redis://127.0.0.1:6390/0", search_config=False)

billing = Module("billing", timeout="30s", max_tasks=50)
app.extends(billing)                       # or app.extends("billing.tasks:module")
```

The keyword arguments of `SynTasks(...)` and `Module(...)` are task
options that everything below them starts from ([Modules](../guides/modules.md)).

| Member (app and module) | Does |
| --- | --- |
| `@app.task` / `@app.task(name, *, labels=None, **options)` | a task ([Tasks](../guides/tasks.md)) |
| `@app.schedule(timing, *, id=, args=, kwargs=, runtime=, task_name=, labels=, **policies_and_options)` | a scheduled task, or a runtime template ([Scheduling](../guides/scheduling.md), [Runtime schedules](../guides/runtime-schedules.md)) |
| `@app.channel(pattern, *, timeout=, retries=, max_tasks=, idle_timeout=, authorize=)` | a channel handler ([Channels](../guides/channels.md)) |
| `app.queue(name, **queue_options)` | declare a queue ([Queues](../guides/queues.md)) |
| `app.extends(*modules)` | attach modules (objects or `"package.module:attr"` paths) |

| Member (app only) | Does |
| --- | --- |
| `await app.send(name, *args, **kwargs)` | send a task by name, without importing it |
| `app.kicker(name)` | a kicker by name |
| `app.group(group_id=None)` | a `TaskGroup` ([below](#groups-and-batches)) |
| `await app.batch(batch_id, owner=None)` | a `Batch` ([below](#groups-and-batches)) |
| `await app.startup()` / `await app.shutdown()` | open / close connections (sending code calls these; workers do it themselves) |
| `await app.run_worker(stop, processes=1, process_index=0)` | run a worker inside this process until `stop` is set (tests, embedding) |
| `await app.nodes()` | `NodeInfo` of every worker process |
| `app.options_for(name)` | a task's effective `TaskOptions` |
| `app.queue_options(name)` | a queue's options |
| `app.config` | the loaded `Config` |
| `app.queues`, `app.schedules`, `app.channels`, `app.tasks`, `app.logs` | managers, below |
| `app.on_event(event)`, `app.add_middlewares(...)`, `app.dependency_overrides` | as on every broker ([Dependencies](../guides/dependencies.md), [Middlewares](../guides/middlewares.md)) |

Errors: `MissingLimitError` (a required limit is set nowhere; in
`syntasks.options`), `ModuleNotAttachedError`, `ConfigError`.

## Tasks

A decorated function stays callable (`await add(1, 2)` runs it in place).

| Member | Returns / does |
| --- | --- |
| `await add.kiq(*args, **kwargs)` | send; an `AsyncSynTasksTask` handle |
| `await add.call(*args, **kwargs)` | send and wait for the value; raises the task's error or `TaskTimeoutError` |
| `add.kicker()` | `.with_queue(q)`, `.with_priority(n)`, `.with_delay(s)`, `.with_timeout(s)`, `.with_labels(...)`, `.then(task)`, `.catch(task)`, then `.kiq(...)` or `.call(...)` |
| `await add.schedule(timing, **options)` | a runtime copy (templates only) |
| `add.schedules` | `TemplateSchedules`: `list(owner=, tag=, state=, limit=, cursor=)`, `get`, `update`, `cancel`, `cancel_all` |

Handle (`AsyncSynTasksTask`):

| Member | Returns / does |
| --- | --- |
| `task.task_id`, `task.task_name` | ids |
| `await task.wait_result(timeout=-1)` | `SynTasksResult`; pushed by Redis, no polling |
| `await task.is_ready()` / `await task.get_result()` | check / read without waiting |
| `task.watch_progress(timeout=None)` | async iterator of progress updates |
| `task.events(timeout=None)` | async iterator of events (`events=True` tasks) |
| `await task.get_progress()` | the latest progress, or `None` |

`SynTasksResult`: `return_value`, `is_err`, `error`, `execution_time`,
`labels`, `log`; `raise_for_error()`. `await gather(*tasks)` waits for
several handles.

## Timings

`every(interval, *, anchor=, between=, weekday=, day=, nth=, month=, week=, exclude=, tz=)`,
`cron(expression, *, tz=)`,
`calendar(*, at="00:00", month=, day=, weekday=, nth=, week=, anchor=, exclude=, tz=)`,
`at(when)` and `after(delay)` (runtime copies only). Each also takes `id=`,
`args=`, `kwargs=` and schedule policies.

## Schedules

`app.schedules` (`ScheduleManager`), all `async`:

| Member | Returns / does |
| --- | --- |
| `create(task, timing=None, *, at=, after=, every=, cron=, tz=, args=, kwargs=, owner=, tags=, meta=, ttl=, expires_at=, max_runs=, id=, if_exists="error", **policies)` | a runtime copy of a template; `if_exists` is `"error"`, `"replace"` or `"ignore"` |
| `get(id)` | `Schedule` |
| `list(task=None, source=None, state=None)` | async iterator of every schedule |
| `find(owner=, tag=, template=, state=, limit=50, cursor=None)` | a `Page` (`items`, `cursor`) |
| `update(id, version=None, **changes)` | change a runtime copy; `ScheduleConflictError` on a stale `version` |
| `cancel(id)` / `delete(id)` | delete a runtime copy |
| `pause(id)` / `resume(id)` / `trigger(id)` | any schedule |
| `pause_all` / `resume_all` / `cancel_all(owner=, tag=, template=)` | in bulk; the count |
| `runs(id, limit=20)` | recent runs, with `late_by` |
| `cancel_running(id)` | cancel its runs executing now |

`Schedule`: `id`, `task`, `template`, `spec`, `source` (`code` / `api`),
`state` (`active`, `paused`, `done`, `expired`, `orphaned`, `invalid`),
`version`, `next_ms`, `runs`, `failures`, `last_status`, `last_error`,
`expires_ms`, `owner`, `tags`, `meta`, `describe()`.

`Runtime(max_per_owner=1000, max_ttl=None, min_interval=None, max_payload=None, keep_finished="7d")`.

Errors derive from `ScheduleError`: `ScheduleLimitError` (in `syntasks`);
`ScheduleNotFoundError`, `ScheduleConflictError`, `StaticScheduleError`
(in `syntasks.scheduler`).

A scheduled run can ask for `ScheduleInfo`: `schedule_id`,
`scheduled_for`, `run_number`, `attempt`, `late_by`.

## Groups and batches

| `TaskGroup` (`app.group()`) | |
| --- | --- |
| `await group.kiq(task, *args, **kwargs)` | send a member |
| `await group.wait(timeout=None)` / `first(n)` / `as_completed()` | wait for results |
| `await group.then(task)` | send `task` once with a `GroupOutcome` when all members finished |

| `Batch` (`await app.batch(id, owner=None)`) | |
| --- | --- |
| `await batch.map(task, items, chunk=1000, seal=True, resume=False)` | send one member per item |
| `await batch.kiq(task, ...)`, `await batch.seal()` | add one; no more members |
| `await batch.pause()` / `resume()` / `cancel()` | control |
| `await batch.progress()` / `batch.watch()` | `BatchProgress`: `state`, `total`, `done`, `failed`, `cancelled`, `pending` |
| `await batch.then(task)` | send `task` once with a `BatchOutcome` |

Outcomes: `TaskOutcome` (`task_id`, `task_name`, `ok`, `return_value`,
`error`, `error_type`, `labels`), `GroupOutcome` (`group_id`, `total`,
`ok`, `failed`, `task_ids`), `BatchOutcome` (`batch_id`, `total`, `done`,
`failed`, `cancelled`).

## Channels

Handler side (`Channel`):

| Member | Does |
| --- | --- |
| `ch.name`, `ch.params` | the channel name and the values matched by the pattern |
| `ch.inbox(batch=10)` | async iterator of `ChannelMessage` sent by clients (also `join` / `leave`) |
| `await ch.publish(data, kind="message")` | broadcast to every reader |
| `ch.state`, `await ch.save_state()` | a dict kept in Redis; a takeover loads it first |
| `await ch.readers()` | readers connected now |
| `await ch.sleep(seconds)` | sleep, returning early if the channel is lost |

Client side (`app.channels`):

| Member | Does |
| --- | --- |
| `async with app.channels.connect(name, *, identity=None, meta=None, last_id=None, presence=True) as conn` | a reader; `async for msg in conn`, `await conn.receive(timeout)`, `await conn.send(data)` |
| `await app.channels.send(name, data)` | to the handler's inbox (starting it if needed), or to readers |
| `await app.channels.publish(name, data)` | broadcast to readers |
| `await app.channels.presence(name)` | `[Presence(reader_id, identity, meta)]` |
| `await app.channels.authorize(name, identity, meta, action)` | run the pattern's `authorize`; `ChannelDenied` |
| `app.channels.consume(name, group)` | share messages among many consumers |
| `await app.channels.history(name, after=None, limit=100)` | messages still kept |
| `await app.channels.info(name)` / `list(prefix="")` / `close(name)` / `restart(name)` | operate |

`ChannelMessage`: `id`, `data`, `sender`, `kind`, `time_ms`.
`ConnectRequest`: `name`, `params`, `identity`, `meta`, `action`.

## Queues, running tasks, logs

`app.queues` (`QueueManager`), all `async`: `names()`, `list()`,
`info(q)`, `peek(q, limit=10)`, `pause(q)`, `resume(q)`, `purge(q)`,
`delete_message(q, id)`, `dead_letters(q, limit=50)`,
`replay_dead(q, ids=None)`, `purge_dead(q)`, `move(source, target)`.

| Member | Does |
| --- | --- |
| `await app.tasks.running()` | tasks running now |
| `await app.tasks.cancel(task_id)` | cancel it wherever it runs (`TaskCancelledError`) |
| `app.tasks.handle(task_id)` | a handle for a task id |
| `app.logs.read(task_id, follow=False)` | captured `LogRecord`s |

## Inside a task

| Ask for | Get |
| --- | --- |
| `Deadline` | `remaining()`, `soft_remaining()`, `elapsed`, `soft_expired`, `expired` |
| `Context` | `message`, `broker`, `state`; `await context.requeue()`, `context.reject()` |
| `ScheduleInfo` | see [Schedules](#schedules) |
| `Emit` | `await emit(data)`: an `emit` event for `task.events()` |
| `ProgressTracker[T]` (`syntasks.depends.progress_tracker`) | `await set_progress(state, meta=None)` |

## Config

```python
from syntasks import Config, load_config

config = load_config("syntasks.toml", profile="production", overrides={"worker.processes": 4})
```

Every setting is in the [configuration reference](config.md).

## Errors

All derive from `SynTasksError`.

| Error | Raised when |
| --- | --- |
| `QueueFullError` | a queue is at `max_length` and `on_full="reject"` |
| `PayloadTooLargeError`, `PayloadInvalidError` | arguments too big for `max_payload`, or not JSON |
| `TaskTimeoutError`, `TaskMemoryError`, `TaskCancelledError` | stored as the result's `error` when a run is stopped |
| `TaskSupersededError` | the stored result of a debounced call replaced by a later one |
| `ResultTooLargeError` | a return value is over `max_result` |
| `SynTasksResultTimeoutError` | `wait_result` timed out |
| `ScheduleError`, `ScheduleLimitError` | schedule CRUD failed or is over a limit |
| `ChannelDenied` | `authorize` refused |
| `ModuleNotAttachedError` | a module's task was sent before `app.extends(...)` |
| `ConfigError` (`MissingLimitError`) | the config or a declaration is invalid |

## Other brokers

`InMemoryBroker` runs tasks in-process (unit tests; no limits, delays or
schedules). `RedisStreamBroker`, `RedisListBroker`, `RedisPubSubBroker`,
`RedisResultBackend` and `MultiBroker` are lower-level building blocks
kept from taskiq; a `SynTasks` app uses its own engine and doesn't need
them.
