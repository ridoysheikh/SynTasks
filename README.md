<div align="center">
<img src="imgs/logo.svg" width=600>
<hr/>
</div>

# SynTasks

SynTasks is a distributed async task queue for Python, by
[Synlor](https://synlor.com), built for clusters of tens to hundreds of
machines.

- **No dependencies.** Only the Python standard library. The Redis client,
  queues, results, scheduler and channels are part of the library.
- **Redis is the source of truth.** Queues, schedules, limits, channel
  ownership and node membership live in Redis. Every machine runs the same
  `syntasks worker`; jobs that need a single owner move by lease when a
  machine dies.
- **Code first, for big apps.** Tasks, schedules, channels and queues are
  declared in code, split into modules that pass their limits down.
  `timeout`, `retries` and `max_tasks` are required, so nothing runs
  unbounded by accident.
- **Scheduling in the library.** Every N seconds, cron, or calendar rules
  ("first Monday at 09:00"); missed fires run as soon as the cluster is
  back. Schedules your users create at runtime, listed by owner, replace a
  table and its CRUD.
- **Limits that hold.** Per machine, per task across the cluster, per key,
  rate limits (also per key), fair sharing between tenants, debounce and
  throttle, soft/hard timeouts with grace, memory limits, process
  isolation.
- **Workflows.** Callbacks, fan-in when a group is done, request/reply,
  and campaigns you can pause, cancel and watch.
- **Realtime.** Live task events and streamed output; channels with
  replay, authorization, presence and long-running handlers kept alive
  once per channel.

> **Status:** early development. APIs may change until the first stable
> release.

## Install

Requires Python 3.11+ and Redis 6.2+.

```bash
pip install syntasks
```

Until it's on PyPI, install from a [GitHub release](https://github.com/ridoysheikh/syntasks/releases):

```bash
pip install "syntasks @ https://github.com/ridoysheikh/syntasks/releases/download/v0.1.0/syntasks-0.1.0-py3-none-any.whl"
```

## In five minutes

```python
# app.py
from syntasks import SynTasks, calendar, every

app = SynTasks(timeout="5m", retries=0, max_tasks=None)  # + syntasks.toml


@app.task(timeout="30s", retries=3, max_tasks=20)
async def add(a: int, b: int) -> int:
    return a + b


@app.schedule(every("10s"))
async def heartbeat() -> None:
    print("still here")


@app.schedule(calendar(weekday="mon", nth=1, at="09:00", tz="Asia/Dhaka"))
async def monthly_report() -> None: ...
```

```toml
# syntasks.toml
[app]
name = "demo"
app = "app:app"

[redis]
url = "${REDIS_URL:-redis://localhost:6379/0}"

[worker]
processes = 2
```

```bash
syntasks worker
```

```python
task = await add.kiq(1, 2)
result = await task.wait_result(timeout=10)
print(result.return_value)  # 3
```

To see more running at once (several workers, schedules, limits, channels
and groups), try [`test-preview/`](test-preview/README.md).

## Documentation

Short pages, one question each. Start at **[docs/](docs/README.md)**.

| | |
| --- | --- |
| [Install](docs/getting-started/install.md) · [Quickstart](docs/getting-started/quickstart.md) | get going |
| [Modules](docs/guides/modules.md) · [Tasks](docs/guides/tasks.md) · [Results and logs](docs/guides/results-and-logs.md) · [Callbacks and batches](docs/guides/callbacks-and-batches.md) · [Dependencies](docs/guides/dependencies.md) | write tasks |
| [Configuration](docs/guides/configuration.md) · [Queues](docs/guides/queues.md) · [Limits](docs/guides/limits.md) · [Cluster](docs/guides/cluster.md) | run them in production |
| [Scheduling](docs/guides/scheduling.md) · [Runtime schedules](docs/guides/runtime-schedules.md) · [Channels](docs/guides/channels.md) | on time, for users, and long-running |
| [Config](docs/reference/config.md) · [CLI](docs/reference/cli.md) · [Python API](docs/reference/python-api.md) | reference |

## Optional extras

| Extra | Adds |
|---|---|
| `syntasks[metrics]` | Prometheus middleware |
| `syntasks[opentelemetry]` | OpenTelemetry instrumentation |
| `syntasks[reload]` | `--reload` for workers |
| `syntasks[orjson]`, `[msgpack]`, `[cbor]` | extra serializers |
| `syntasks[uv]` | uvloop event loop |

## Credits

SynTasks is built on top of [taskiq](https://github.com/taskiq-python/taskiq),
created by Pavel Kirilin and the taskiq contributors. A large part of the
original codebase comes from taskiq, and its design is the direct inspiration
for this project. Dependency injection is vendored from
[taskiq-dependencies](https://github.com/taskiq-python/taskiq-dependencies).
All are released under the MIT License; see
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

## Development

```bash
uv sync --all-extras
uv run pytest -q -n auto   # Redis tests start a throwaway redis-server
```

See [Testing](docs/development/testing.md) and [Releasing](docs/development/releasing.md).

## License

[MIT](LICENSE)
