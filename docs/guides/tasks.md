# Tasks

## Declaring

```python
app = SynTasks(timeout="5m", retries=0, max_tasks=None)


@app.task
async def resize(image_id: str, width: int = 800) -> str: ...


@app.task("reports:build", queue="reports", timeout="20m", retries=2, max_tasks=4)
def build_report(month: str) -> bytes:  # sync functions run in a thread pool
    ...
```

Keyword arguments are task options; a misspelled one is an error. Labels
sent with every message go in `labels={...}`. The default name is
`module:function`. Every task needs `timeout`, `retries` and `max_tasks`
from itself or a [module](modules.md) above it.

| Option | Meaning |
|---|---|
| `timeout`, `soft_timeout`, `grace` | hard time limit, early warning, cleanup time ([Limits](limits.md)) |
| `retries`, `retry_delay`, `retry_backoff`, `retry_max_delay`, `retry_jitter` | retries after an exception |
| `max_tasks` | runs at once across the cluster |
| `max_tasks_per_node` | runs at once per worker process |
| `key` | the argument that groups calls: `"kwargs.customer_id"`, `"args.0"` or a parameter name |
| `max_tasks_per_key` | runs at once per key; `1` keeps calls of a key in order |
| `rate`, `rate_burst`, `rate_per_key` | rate limits across the cluster, and per key |
| `fair`, `debounce`, `throttle` | per-key flow control ([Queues](queues.md#per-key-flow-control)) |
| `queue`, `priority`, `tags` | routing ([Queues](queues.md)) |
| `isolation`, `memory_limit` | run each call in its own process |
| `unique_for`, `unique_key` | drop identical calls for a while |
| `on_success`, `on_failure`, `events` | callbacks and live events ([Callbacks and batches](callbacks-and-batches.md), [Results](results-and-logs.md#live-events)) |
| `max_payload`, `max_result`, `result_ttl`, `store_result`, `capture_logs`, `dead_letter` | sizes, results, logs |

## Sending

```python
task = await resize.kiq("img-1", width=400)

# per call: another queue, priority, delay or timeout
await resize.kicker().with_queue("bulk").with_priority(2).kiq("img-2")
await resize.kicker().with_delay(30).kiq("img-3")
await resize.kicker().with_timeout(5).kiq("img-4")

# by name, without importing the function
await app.send("images.tasks:resize", "img-5")

# send and wait for the value (request / reply)
url = await resize.call("img-6")
```

Arguments are converted to JSON (dataclasses and pydantic-style models
too) and back to the annotated types on the worker. A message is limited
to `max_payload` bytes (256 KiB by default).

`call()` sends at the highest priority with a short-lived result and
returns the value, raises the task's own exception, or raises
`TaskTimeoutError` (`kicker().with_timeout(5).call(...)` to wait less).

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
(`dead_letter=True` by default), with the error. So do messages for
tasks no worker knows, and messages that crashed workers too often
(`max_deliveries` of the queue). Inspect and replay them with
`app.queues.dead_letters(...)`, `app.queues.replay_dead(...)` or
`syntasks queues dlq/replay` ([Queues](queues.md)).

## Unique tasks

```python
@app.task(unique_for="10m", unique_key="kwargs.order_id")
async def ship(order_id: str, express: bool = False) -> None: ...
```

A second call with the same key within the window doesn't send anything
and returns the handle of the first. Without `unique_key`, all arguments
form the key.

## Results and dependencies

Results are stored for `result_ttl` (1 day by default; `store_result=False`
to skip), and `max_result` caps their size; see
[Results and logs](results-and-logs.md). Tasks can receive dependencies:
`Context`, the run's `Deadline` ([Limits](limits.md)), `ScheduleInfo`
([Scheduling](scheduling.md)), `ProgressTracker`, `Emit`, or your own;
see [State and dependencies](dependencies.md).
