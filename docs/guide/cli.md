---
order: 12
---

# CLI

Every command reads the same config (`-c/--config`, `--profile`).
Management commands also accept `--app module:attribute` and `--json`.

| Command | |
|---|---|
| `syntasks worker [APP]` | run workers; `-p` processes, `-q` queues, `--max-async-tasks`, `--log-level`, `--reload` |
| `syntasks config check` | validate the config and the app's tasks and schedules |
| `syntasks config show [--sources]` | print the effective config |
| `syntasks queues list` | queues with waiting, running, delayed and dead counts |
| `syntasks queues info\|peek\|pause\|resume\|purge Q` | inspect and operate a queue |
| `syntasks queues dlq\|replay\|purge-dlq Q` | dead letters |
| `syntasks schedules list [--task T] [--source S]` | schedules |
| `syntasks schedules show\|pause\|resume\|trigger\|delete\|runs ID` | one schedule |
| `syntasks channels list\|info\|close` | channels |
| `syntasks nodes` | worker processes, load and last heartbeat |
| `syntasks tasks running\|cancel ID\|result ID` | running tasks and results |
| `syntasks logs TASK_ID [-f]` | captured logs |
