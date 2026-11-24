# Limits

| Limit | Scope | Setting |
|---|---|---|
| Tasks at once | this machine | `worker.max_async_tasks` (split across its processes) |
| Threads for sync tasks | one process | `worker.max_threads` |
| One task at once | this machine | `node_concurrency` |
| One task at once | whole cluster | `concurrency` |
| One task per key at once | whole cluster | `concurrency_key` + `key_concurrency` |
| Rate | whole cluster | `rate = "100/s"`, `rate_burst` |
| Time | one run | `soft_timeout`, `timeout`, `grace` |
| Memory | one process | `worker.memory_soft_limit`, `worker.memory_hard_limit` |
| Memory | one isolated run | `isolation = "process"` + `memory_limit` |
| Size | one message / result | `max_payload`, `max_result` |

## Cluster-wide concurrency and rate limits

```python
@app.task(concurrency=10, rate="50/s")
async def call_partner_api(order_id: str) -> None: ...
```

At most 10 calls run at once across all machines, and at most 50 start
per second. Workers take a slot *before* they take a message, in one
atomic step, so no machine holds messages it can't run. Waiting workers
are woken when a slot frees up or a message arrives; they don't poll.
Slots of a machine that dies are freed when its heartbeat stops.

## Time limits

```python
from syntasks import Deadline, Depends


@app.task(soft_timeout="50s", timeout="60s", grace="10s")
async def crunch(batch_id: str, deadline: Deadline = Depends()) -> None:
    for chunk in load(batch_id):
        if deadline.soft_expired:
            save_checkpoint()
            return
        process(chunk)
```

1. **Soft timeout:** `Deadline.soft_reached` is set and a warning is
   logged. The task can stop on its own.
2. **Timeout:** the run is cancelled (`CancelledError` inside the task,
   so `finally` blocks run).
3. **Grace:** the run gets `grace` to finish cleaning up. If it is still
   running after that, its result is `TaskTimeoutError` and the process is
   marked for recycling: it stops taking work, finishes what it has, and
   the supervisor starts a fresh process.

Threads can't be killed, so a sync task that overruns is abandoned the
same way. For a true hard kill, use process isolation.

## Process isolation

```python
@app.task(isolation="process", timeout="5m", grace="10s", memory_limit="1GiB")
def render(job_id: str) -> str: ...
```

Each call runs in a child process: `SIGTERM` at the timeout, `SIGKILL`
after the grace period, and the child is killed (`TaskMemoryError`) when
it uses more than `memory_limit`. The function must be defined at module
level; arguments and results are pickled.

## Memory of worker processes

- `worker.memory_soft_limit`: above it, the process stops taking work,
  finishes its running tasks and is replaced. Nothing is killed.
- `worker.memory_hard_limit`: `RLIMIT_AS` on Linux; allocations above it
  fail with `MemoryError`. Off by default.
- `worker.max_tasks_per_process`: replace a process after N tasks.

## Blocked event loop

A watchdog thread logs the stack of the main thread when the event loop
is blocked longer than `worker.loop_block_warn` (sync code inside an
async task), and restarts the process past `worker.loop_block_kill`.
