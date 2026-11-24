# Callbacks and batches

What happens after a task: run another one, wait for a whole group, or
manage a campaign of thousands of tasks as one thing.

## Callbacks

```python
from syntasks import TaskOutcome


@app.task(timeout="10s", retries=3, max_tasks=None)
async def audit(outcome: TaskOutcome) -> None:
    print(outcome.task_name, outcome.ok, outcome.return_value, outcome.error)


@app.task(timeout="30s", retries=3, max_tasks=20, on_success=audit, on_failure=alert)
async def charge(order_id: str, cents: int) -> str: ...


# or per send
await charge.kicker().then(audit).catch(alert).kiq("o-1", 1999)
```

The callback is sent after the **final** attempt: `on_failure` once,
after the last retry; never while retrying. It receives a `TaskOutcome`:
`task_id`, `task_name`, `ok`, `return_value` (if JSON), `error`,
`error_type` and the parent's labels.

Each callback is **sent exactly once**: its task id is derived from the
parent's, and a one-time key drops repeats if the parent's worker dies
after storing the result. The parent itself is at-least-once, as every
task is.

## Fan-in: run something when a group is done

```python
group = app.group()
for url in urls:
    await group.kiq(fetch, url)
await group.then(merge)      # after the last kiq
```

`merge` runs once, with a `GroupOutcome` (`group_id`, `total`, `ok`,
`failed`, and the member `task_ids` for groups up to 1000), when every
member has finished, on any machine, even if the members finished before
`then()` was called. Read the results through the handles or
`app.tasks.handle(task_id)`.

## Request / reply

```python
price = await quote.call("KB-01")                     # waits for the value
price = await quote.kicker().with_timeout(2).call("KB-01")
```

Sent at the highest priority with a result kept for 60 seconds. It
returns the value, raises the task's own exception, or raises
`TaskTimeoutError`.

## Batches (campaigns)

A batch is a named, persistent group: you can pause it, resume it, cancel
it and watch it from anywhere.

```python
campaign = await app.batch("spring-sale", owner="team:marketing")
await campaign.map(send_email, ({"to": a} for a in addresses), chunk=1000)
await campaign.then(report)            # once with a BatchOutcome when it ends

await campaign.pause()                 # members not started yet wait
await campaign.resume()
await campaign.cancel()                # members not started yet are dropped

progress = await campaign.progress()   # state, total, done, failed, cancelled, pending
async for progress in campaign.watch():
    ...
```

- `map(task, items)`: a dict item is keyword arguments, a tuple or list
  positional arguments, anything else the only argument. It sends in
  chunks and saves progress after each, so `map(..., resume=True)` with
  the same items continues where an interrupted call stopped.
  `seal=False` lets you add more later with `kiq` or another `map`, then
  `seal()`.
- Pause and cancel are checked as workers take a batch's messages:
  paused members are set aside and come back on `resume()`; cancelled
  ones are counted and dropped as workers reach them.
- `then(task)` seals the batch and runs `task` once with a
  `BatchOutcome` (`batch_id`, `total`, `done`, `failed`, `cancelled`)
  when every member has finished or the batch was cancelled.

To start a campaign later or every week, give a
[runtime schedule](runtime-schedules.md) a task that calls
`app.batch(...).map(...)`.
