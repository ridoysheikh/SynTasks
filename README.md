<div align="center">
<img src="imgs/logo.svg" width=600>
<hr/>
</div>

# SynTasks

SynTasks is an asynchronous distributed task queue for Python, by
[Synlor](https://synlor.com).

- **No dependencies.** The library uses only the Python standard library.
- **Redis built in.** Client, brokers, result backend and persistent
  schedules ship with the library. There are no plugin packages to install.
- **Async first.** It runs both async and sync functions, and its type
  hints give accurate autocompletion
  ([PEP-612](https://peps.python.org/pep-0612/)).

> **Status:** early development. APIs may change without notice until the
> first stable release.

## Credits

SynTasks is built on top of [taskiq](https://github.com/taskiq-python/taskiq),
created by Pavel Kirilin and the taskiq contributors. A large part of the
current codebase comes from taskiq, and its design is the direct inspiration
for this project. Dependency injection is vendored from
[taskiq-dependencies](https://github.com/taskiq-python/taskiq-dependencies).
All are released under the MIT License; see [LICENSE](LICENSE).

## Installation

Requires Python 3.11+ and, for the Redis components, Redis 6.2+.

SynTasks is not published on PyPI yet. Install it from source:

```bash
pip install git+https://github.com/synlor/syntasks
```

## Usage

```python
# app.py
import asyncio

from syntasks import RedisResultBackend, RedisStreamBroker

broker = RedisStreamBroker("redis://localhost:6379/0").with_result_backend(
    RedisResultBackend("redis://localhost:6379/0", result_ex_time=3600),
)


@broker.task
async def add(a: int, b: int) -> int:
    return a + b


async def main() -> None:
    await broker.startup()
    task = await add.kiq(1, 2)
    result = await task.wait_result(timeout=10)
    print(result.return_value)  # 3
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

Start workers:

```bash
syntasks worker app:broker
```

### Redis components

| Component | What it does |
|---|---|
| `RedisStreamBroker` | Reliable queue on a Redis stream. Workers acknowledge messages after processing, and messages a crashed worker left unacknowledged are taken over by another worker. Supports delayed tasks. |
| `RedisListBroker` | Simple, fast queue on a list. No acknowledgements. Supports delayed tasks. |
| `RedisPubSubBroker` | Broadcasts every task to every worker. |
| `RedisResultBackend` | Results and progress, with optional expiry. |
| `RedisScheduleSource` | Schedules stored in Redis, which can be added and removed at runtime. |
| `syntasks.redis.Redis` | The async client underneath: connection pool, pipelines, transactions, pub/sub, TLS, Unix sockets, Sentinel. |

URLs: `redis://[user:password@]host:port/db`, `rediss://...` (TLS),
`unix:///path/redis.sock?db=0`.

### Optional extras

Integrations with other ecosystems are optional and installed separately:

| Extra | Adds |
|---|---|
| `syntasks[metrics]` | Prometheus middleware |
| `syntasks[opentelemetry]` | OpenTelemetry instrumentation |
| `syntasks[reload]` | `--reload` for workers |
| `syntasks[orjson]`, `[msgpack]`, `[cbor]` | extra serializers |
| `syntasks[uv]` | uvloop event loop |

## Local development

```bash
uv sync --all-extras
uv run pytest              # Redis tests start a throwaway redis-server
uv run pre-commit install  # linting
```

The Redis tests need `redis-server` on `PATH`, or `SYNTASKS_TEST_REDIS_URL`
pointing at a disposable server. If neither is available, they are skipped.
