---
order: 2
---

# Getting started

## Install

SynTasks isn't on PyPI yet; install it from source:

```bash
pip install git+https://github.com/synlor/syntasks
```

You also need a Redis server (6.2 or newer).

## Write an app

```python
# billing/app.py
from syntasks import SynTasks

app = SynTasks()


@app.task(timeout="30s")
async def add(a: int, b: int) -> int:
    return a + b
```

`SynTasks()` reads `syntasks.toml` from the current directory (or
`[tool.syntasks]` in `pyproject.toml`). Without a file it uses defaults:
Redis on `localhost:6379`, one queue called `default`.

## Configure

```toml
# syntasks.toml
[app]
name = "billing"           # namespace in Redis
app = "billing.app:app"    # where workers find the app

[redis]
url = "${REDIS_URL:-redis://localhost:6379/0}"

[worker]
processes = 4
max_async_tasks = 200
```

Check it with `syntasks config check`. Every setting is described in the
[configuration reference](../reference/config.md).

## Run workers

```bash
syntasks worker
```

This starts `worker.processes` processes on this machine. Run the same
command on as many machines as you like; they share the work through
Redis.

## Send tasks

```python
import asyncio

from billing.app import add, app


async def main() -> None:
    task = await add.kiq(1, 2)              # send
    result = await task.wait_result(timeout=10)
    print(result.return_value)               # 3
    await app.shutdown()


asyncio.run(main())
```

`kiq` returns as soon as the message is in Redis. `wait_result` blocks
until a worker stores the result; it doesn't poll.

## Next

- [Tasks](tasks.md): options, retries, routing.
- [Scheduling](scheduling.md): run tasks every N seconds, on cron, once.
- [Limits](limits.md): concurrency, rate limits, timeouts, memory.
