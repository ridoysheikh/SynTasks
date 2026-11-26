# Changelog

All notable changes are listed here. Versions follow [semantic versioning](https://semver.org); until 1.0, minor versions may contain breaking changes, and each entry says how to upgrade.

## [0.2.0]

### Changed (breaking)

- **`syntasks worker` is now `syntasks run`.** It is the one command that runs the app on a machine: its worker processes execute tasks, fire schedules and host channels. `worker` is gone; there is no alias.

### Added

- **Watch protocol:** `list` also returns `saved`, the groups saved for the identity from any connection, next to `groups`, which this connection has joined.
- **`test-preview/`** has FastAPI web servers with a browser page that drives every feature, and `dev.sh` to run it all (`uv sync --group preview`).

### Fixed

- mypy passes whether or not FastAPI is installed.

### Upgrading from 0.1.0

Change the command wherever a machine is started (service files, containers, compose files, scripts) from `syntasks worker …` to `syntasks run …`. Its arguments and flags are the same, and so is `syntasks.toml`, including the `[worker]` section.

## [0.1.0]

The first release. SynTasks started from [taskiq](https://github.com/taskiq-python/taskiq); coming from taskiq, see [Upgrading from taskiq](#upgrading-from-taskiq) below.

### Added

- **No dependencies.** The core uses only the Python standard library, including its own async Redis client (single server or Sentinel). Extras add Prometheus, OpenTelemetry, uvloop, `--reload` and extra serializers.
- **Code first, for big apps.** Tasks, schedules, channels and queues are declared in code with `@app.task`, `@app.schedule`, `@app.channel` and `app.queue`, and split into `Module`s attached with `app.extends(...)`. Options set on the app or a module pass down to everything in it; the nearest link wins. `syntasks.toml` holds infrastructure only (Redis, workers, nodes, timings), with `${VAR:-default}` values, `SYNTASKS__SECTION__KEY` overrides, profiles and units. See [Modules](docs/guides/modules.md) and [Configuration](docs/guides/configuration.md).
- **Required limits.** Every task, schedule and channel must have `timeout`, `retries` and `max_tasks` from itself or a module above it; `None` is an explicit "unbounded". Workers publish the limits their code declares with a deploy `revision`, so machines never enforce different limits.
- **Built for many machines.** Every machine runs the same `syntasks worker`, a supervisor over `worker.processes` worker processes. Jobs that need a single owner move by lease when a machine dies. Every decision shared between machines is one Lua script on Redis time. A worker that can't start stops the supervisor after `max_startup_failures`. See [Cluster](docs/guides/cluster.md).
- **Queues.** Named queues, strict priorities, delays, routing to machines by tag, one-at-a-time per key with order kept, `unique_for` dedupe, `max_length`, dead letters with replay, pause / resume / purge / peek. Per-key flow control: `fair` (round-robin between keys so one tenant's backlog can't starve the others), `debounce`, `throttle`, `rate_per_key`. See [Queues](docs/guides/queues.md).
- **Limits.** Concurrency per machine, per task per process, per task across the cluster and per key; rate limits, also per key; soft and hard timeouts with a grace period; memory limits per process and, with `isolation="process"`, per run in a child process; recycling of stuck or bloated workers. Native retries with backoff and jitter; cancelling running tasks. See [Limits](docs/guides/limits.md).
- **Scheduler in the library.** `@app.schedule(every("10s"))`, `cron(...)` with seconds, time zones and `L` / `#`, `calendar(...)` rules (weekdays, first or last weekday of the month, days counted from the end, months, every N weeks, several times a day, excluded dates, with correct DST handling), windowed `every(..., weekday=, between=)` and `at(...)`. Declared schedules are reconciled into Redis on every start; each fire runs exactly once across the cluster. Missed fires run as soon as the cluster is back (`misfire` once / all / skip, `misfire_grace`), and each run knows how late it is. See [Scheduling](docs/guides/scheduling.md).
- **Runtime schedules.** `@app.schedule(runtime=Runtime(...))` makes a template your app's users can copy at runtime, with an owner, tags and metadata, listed and paged by owner, tag or template, edited with optimistic locking, paused or cancelled in bulk, and capped per owner. Finished one-shots are kept for a while; copies of a template that left the code are orphaned. They replace a table and its CRUD for scheduled posts or campaign sends. See [Runtime schedules](docs/guides/runtime-schedules.md).
- **Callbacks and batches.** `on_success` / `on_failure` (or `.then()` / `.catch()` per send) with a `TaskOutcome`, sent exactly once; `group.then(task)` once a whole group is done, on any machine; `task.call(...)` for request / reply; `app.batch(...)` for campaigns with chunked, resumable `map`, pause / resume / cancel, progress and `then`. See [Callbacks and batches](docs/guides/callbacks-and-batches.md).
- **Realtime.** `events=True` publishes a task's life (queued, started, progress, retrying, succeeded / failed / cancelled) and `Emit` output to `task.events()`. Channels: broadcast streams with replay; long-running handlers kept alive once per channel, which move with their saved state when a machine dies; `authorize`, presence with identity and metadata, join / leave messages, a cap on live handlers per pattern; shared consumers. See [Channels](docs/guides/channels.md) and [Results and logs](docs/guides/results-and-logs.md).
- **Feeds and watchlists.** `@app.feed(kind, every=...)` declares a shared data source: a polled `fetch(key)`, or a streamed `run(feed)` that holds one upstream for the whole kind. It runs once for the cluster as a channel handler, only while keys are watched and for `linger` after the last watcher leaves, optionally in shards; the last value of each key is kept and the feed's health (`live` / `degraded`) reaches watchers. `app.feeds.watch(identity)` is one connection's session: groups of `kind:key` channels subscribed and dropped at runtime, saved per identity and restored on request, the newest snapshot per channel for slow readers, and `handle(request)` for a JSON `subscribe` / `unsubscribe` / `list` / `restore` protocol. See [Feeds and watchlists](docs/guides/feeds.md).
- **Results.** `wait_result` is pushed by Redis instead of polled; task groups (`wait`, `as_completed`, `first`); live progress; result TTL and size limit. Log capture for tasks with `capture_logs=True`.
- **CLI.** `syntasks worker`, `config`, `queues`, `schedules` (with `preview`, owner / tag filters and `cancel`), `channels`, `nodes`, `tasks` and `logs`, with `--json`. See the [CLI reference](docs/reference/cli.md).
- **Documentation** in `docs/`, examples in `examples/`, and a runnable walkthrough in `test-preview/`.

### Upgrading from taskiq

- The import is `syntasks`, the base classes are `SynTasks*` (`SynTasksMessage`, `SynTasksMiddleware`, …) and `SynTasksDepends` replaces `TaskiqDepends`.
- Use `app = SynTasks(timeout=..., retries=..., max_tasks=...)` instead of a broker plus result backend; the worker is `syntasks worker` (the app comes from `[app] app`), configured in `syntasks.toml` instead of flags.
- Labels passed as decorator keywords go in `labels={...}`; unknown keywords are errors.
- `TaskiqScheduler`, schedule sources and the `schedule` label are gone: use `@app.schedule(...)`, or runtime schedules for ones created while running. There is no separate scheduler process.
- `InMemoryBroker` and the lower-level Redis brokers (`RedisStreamBroker`, …) are still there for simple cases and tests.
