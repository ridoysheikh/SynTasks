---
order: 4
---

# Tasks

## Declaring

```python
@app.task
async def resize(image_id: str, width: int = 800) -> str: ...


@app.task(task_name="reports:build", queue="reports", timeout="20m", retries=2)
def build_report(month: str) -> bytes:  # sync functions run in a thread pool
    ...
```

Keyword arguments that are task options configure the task (all of them
are listed under `[tasks."module:function"]` in the
[reference](../reference/config.md)); `schedule=` attaches
[schedules](scheduling.md); anything else becomes a message label.

The default name is `module:function`. Config can set the same options
without touching code:

```toml
[tasks."images.tasks:resize"]
queue = "images"
timeout = "2m"
concurrency = 20
```

## Sending

```python
task = await resize.kiq("img-1", width=400)

# per call: another queue, priority, delay or timeout
await resize.kicker().with_queue("bulk").with_priority(2).kiq("img-2")
await resize.kicker().with_delay(30).kiq("img-3")
await resize.kicker().with_timeout(5).kiq("img-4")

# by name, without importing the function
await app.send("images.tasks:resize", "img-5")
```

Arguments are converted to JSON (dataclasses and pydantic-style models
too) and converted back to the annotated types on the worker. A message
is limited to `max_payload` bytes (256 KiB by default).

## Retries

```python
@app.task(retries=5, retry_delay="1s", retry_backoff=2, retry_max_delay="5m")
async def call_partner(order_id: str) -> None: ...
```

A run that raises is sent again after a growing delay (with jitter by
default). `NoResultError`, `TaskRejectedError` and cancelled runs are not
retried. While retrying, no result is stored; the final attempt stores
its result or error.

## Dead letters

After the last retry, a failed message goes to its queue's dead letters
(`dead_letter = true` by default), with the error. So do messages for
tasks no worker knows, and messages that crashed workers too often
(`queues.<name>.max_deliveries`). Inspect and replay them with
`app.queues.dead_letters(...)`, `app.queues.replay_dead(...)` or
`syntasks queues dlq/replay` ([Queues](queues.md)).

## Unique tasks

```python
@app.task(unique_for="10m", unique_key="kwargs.order_id")
async def ship(order_id: str, express: bool = False) -> None: ...
```

A second kick with the same key within the window doesn't send anything
and returns the handle of the first one. Without `unique_key`, all
arguments form the key.

## Results

Results are stored for `result_ttl` (1 day by default; `store_result =
false` to skip). `max_result` caps their size. See
[Results and logs](results-and-logs.md).

## Dependencies

Tasks can receive dependencies: the message `Context`, the run's
`Deadline` ([Limits](limits.md)), `ScheduleInfo` ([Scheduling](scheduling.md)),
`ProgressTracker`, or your own. See [State and dependencies](state-and-deps.md).
