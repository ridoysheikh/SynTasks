---
order: 9
---

# Results and logs

## Waiting for results

```python
task = await add.kiq(1, 2)
result = await task.wait_result(timeout=10)
result.return_value, result.is_err, result.error, result.execution_time
```

Waiting doesn't poll: the worker writes a small "done" stream when it
stores a result, and the waiter blocks on it. Look a task up later with
`app.tasks.handle(task_id)`.

## Groups

```python
group = app.group()
for n in range(100):
    await group.kiq(square, n)

results = await group.wait(timeout=30)             # all, in send order
fastest = await group.first(3)                     # first three to finish
async for handle, result in group.as_completed():  # as they finish
    ...
```

Group members report to one group stream, so waiting for 10 000 tasks is
one blocking read.

## Progress

```python
from syntasks import Depends
from syntasks.depends.progress_tracker import ProgressTracker, TaskState

@app.task
async def import_file(path: str, progress: ProgressTracker[int] = Depends()) -> None:
    for percent in range(0, 101, 10):
        await progress.set_progress(TaskState.STARTED, percent)

task = await import_file.kiq("data.csv")
async for update in task.watch_progress():
    print(update.meta)
```

## Log capture

Log capture is for the tasks that need it:

```python
@app.task(capture_logs=True)
async def import_file(path: str) -> None:
    logger.info("starting %s", path)   # plain logging
```

Every record logged while the run is going, from the task, its
sub-coroutines and sync code in threads, goes to a Redis stream for that
task: at most `logging.capture_max_lines` lines, each cut at
`capture_max_record` bytes, kept for `capture_retention`. Other tasks pay
one context-variable lookup per log record.

```python
async for record in app.logs.read(task.task_id, follow=True):
    print(record.level, record.message)
```

```bash
syntasks logs <task_id> -f
```

## Running tasks

```python
await app.tasks.running()          # task id, name and node of every running task
await app.tasks.cancel(task_id)    # the run ends with TaskCancelledError
```
