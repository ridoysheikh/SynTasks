# Quickstart

From zero to tasks running on workers in five minutes. You need SynTasks
[installed](install.md) and a Redis server.

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

To see more running at once (schedules, limits, channels, groups, several
workers), try [`test-preview/`](../../test-preview/README.md).

## Next

- [Tasks](../guides/tasks.md): options, retries, routing.
- [Scheduling](../guides/scheduling.md): run tasks every N seconds, on cron, once.
- [Limits](../guides/limits.md): concurrency, rate limits, timeouts, memory.
