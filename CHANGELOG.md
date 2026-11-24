# Changelog

All notable changes are listed here. Versions follow [semantic versioning](https://semver.org); until 1.0, minor versions may contain breaking changes, and each entry says how to upgrade.

## [0.1.0]

The first release. SynTasks started from [taskiq](https://github.com/taskiq-python/taskiq); coming from taskiq, see [Upgrading from taskiq](#upgrading-from-taskiq) below.

### Added

- **No dependencies.** The core uses only the Python standard library, including its own async Redis client (single server or Sentinel). Extras add Prometheus, OpenTelemetry, uvloop, `--reload` and extra serializers.
- **One app object and one config file.** `SynTasks()` reads `syntasks.toml` (or `[tool.syntasks]` in `pyproject.toml`), with `${VAR:-default}` values, `SYNTASKS__SECTION__KEY` environment overrides, profiles, units (`"30s"`, `"64KiB"`, `"100/s"`) and typo suggestions. Cluster-wide settings are published to Redis with a deploy `revision`, so machines never enforce different limits. See [Configuration](docs/guides/configuration.md).
- **Built for many machines.** Every machine runs the same `syntasks worker`, a supervisor over `worker.processes` worker processes. Jobs that need a single owner move by lease when a machine dies. Every decision shared between machines is one Lua script on Redis time. See [Cluster](docs/guides/cluster.md).
- **Queues.** Named queues, strict priorities, delays, routing to machines by tag, one-at-a-time per key with order kept, `unique_for` dedupe, `max_length`, dead letters with replay, pause / resume / purge / peek. A long task is never taken over while it's still running. See [Queues](docs/guides/queues.md).
- **Limits.** Concurrency per machine, per task per machine, per task across the cluster and per key; rate limits; soft and hard timeouts with a grace period; memory limits per process and, with `isolate = true`, per run in a child process; recycling of stuck or bloated workers. Native retries with backoff and jitter; cancelling running tasks. See [Limits](docs/guides/limits.md).
- **Scheduler in the library.** `@app.task(schedule=every("10s"))`, `cron(...)` with a seconds field and time zones, `at(...)`, `after(...)`, or `[schedules.<id>]` in the config file, reconciled into Redis on every start. Dynamic schedules with create / update / delete at runtime, TTLs, run limits and optimistic locking. Each fire runs exactly once across the cluster, with misfire and overlap policies. See [Scheduling](docs/guides/scheduling.md).
- **Channels.** Broadcast streams with replay; long-running handlers kept alive once per channel, which move with their saved state when a machine dies; a websocket-style connect / send API; shared consumers. See [Channels](docs/guides/channels.md).
- **Results.** `wait_result` is pushed by Redis instead of polled; task groups (`wait`, `as_completed`, `first`); live progress; result TTL and size limit. Log capture for tasks with `capture_logs = true`. See [Results and logs](docs/guides/results-and-logs.md).
- **CLI.** `syntasks worker`, `config`, `queues`, `schedules`, `channels`, `nodes`, `tasks` and `logs`, with `--json`. See the [CLI reference](docs/reference/cli.md).
- **Documentation** in `docs/`, examples in `examples/`, and a runnable walkthrough in `test-preview/`.

### Upgrading from taskiq

- The import is `syntasks`, the base classes are `SynTasks*` (`SynTasksMessage`, `SynTasksMiddleware`, …) and `SynTasksDepends` replaces `TaskiqDepends`.
- Use `app = SynTasks()` instead of a broker plus result backend; the worker is `syntasks worker` (the app comes from `[app] app`), configured in `syntasks.toml` instead of flags.
- `TaskiqScheduler`, schedule sources and the `schedule` label are gone: declare schedules with `schedule=` or create them with `app.schedules.create(...)`. There is no separate scheduler process.
- `InMemoryBroker` and the lower-level Redis brokers (`RedisStreamBroker`, …) are still there for simple cases and tests.
