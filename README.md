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
- **Scheduling in the library.** Second-level schedules declared next to
  tasks or created at runtime with full CRUD, fired once across the cluster.
- **Limits that hold.** Per machine, per task across the cluster, per key,
  rate limits, soft/hard timeouts with grace, memory limits, process
  isolation.
- **Channels.** Broadcast streams with replay, and long-running handlers kept
  alive once per channel (websocket rooms, stateful loops).
- **One config file.** `syntasks.toml`, with environment overrides and
  profiles.

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
from syntasks import SynTasks, every

app = SynTasks()  # reads syntasks.toml


@app.task(timeout="30s", retries=3)
async def add(a: int, b: int) -> int:
    return a + b


@app.task(schedule=every("10s"))
async def heartbeat() -> None:
    print("still here")
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
| [Tasks](docs/guides/tasks.md) · [Results and logs](docs/guides/results-and-logs.md) · [Dependencies](docs/guides/dependencies.md) | write tasks |
| [Configuration](docs/guides/configuration.md) · [Queues](docs/guides/queues.md) · [Limits](docs/guides/limits.md) · [Cluster](docs/guides/cluster.md) | run them in production |
| [Scheduling](docs/guides/scheduling.md) · [Channels](docs/guides/channels.md) | on time, and long-running |
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
