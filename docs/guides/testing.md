# Testing

## A real worker in the test process

```python
import asyncio

from myapp import app, add


async def test_add() -> None:
    stop = asyncio.Event()
    worker = asyncio.create_task(app.run_worker(stop))
    try:
        task = await add.kiq(1, 2)
        assert (await task.wait_result(timeout=10)).return_value == 3
    finally:
        stop.set()
        await worker
```

Point the app at a disposable Redis (`SynTasks(redis_url=...)` or
`SYNTASKS__REDIS__URL`) and give each test its own `app.name` to keep
their keys apart. Test apps can set the required limits once, at the
root: `SynTasks(name="test-1", timeout=None, retries=0, max_tasks=None)`.

## Without Redis

For unit tests of task logic, call the function directly: a decorated
task is still callable (`await add(1, 2)`). For code that kicks tasks,
`InMemoryBroker` runs them in-process:

```python
from syntasks import InMemoryBroker

broker = InMemoryBroker()


@broker.task
async def add(a: int, b: int) -> int:
    return a + b


async def test_add_in_memory() -> None:
    task = await add.kiq(1, 2)
    assert (await task.wait_result()).return_value == 3
```

## Dependency overrides

```python
app.dependency_overrides[get_db] = fake_db
```
