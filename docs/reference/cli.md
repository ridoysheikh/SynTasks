# CLI reference

Installing SynTasks gives you one command, `syntasks`, with a subcommand per
job. `python -m syntasks …` works too, and `syntasks --version` prints the
version.

| Command | Job |
| --- | --- |
| `syntasks worker` | run worker processes on this machine |
| `syntasks config` | check or print the configuration |
| `syntasks queues` | inspect and operate queues and dead letters |
| `syntasks schedules` | inspect and operate schedules |
| `syntasks channels` | inspect and close channels |
| `syntasks nodes` | list worker processes in the cluster |
| `syntasks tasks` | running tasks, cancelling, results |
| `syntasks logs` | captured logs of a task |

**Every command accepts:**

| Option | Meaning |
| --- | --- |
| `--config PATH`, `-c` | config file (default: `$SYNTASKS_CONFIG`, then `./syntasks.toml`, then `[tool.syntasks]` in `pyproject.toml`) |
| `--profile NAME` | apply a [profile](../guides/configuration.md#profiles) (env `SYNTASKS_PROFILE`) |

**Management commands** (everything except `worker` and `config`) also accept:

| Option | Meaning |
| --- | --- |
| `--app module:attribute` | the app, when `[app] app` isn't set; needed for commands that check task names |
| `--json` | print JSON instead of a table |

**Errors** print one line and exit with code 1.

## worker

```bash
syntasks worker [APP] [options]
```

Starts `worker.processes` worker processes and supervises them (restarts
after a crash or a recycle, stops gracefully on `SIGTERM`/`SIGINT`,
restarts them all on `SIGHUP`). `APP` defaults to `[app] app`. Flags
override the config file.

| Option | Meaning |
| --- | --- |
| `--processes N`, `-p` | worker processes; `0` = one per CPU |
| `--queues a,b`, `-q` | serve only these queues |
| `--max-async-tasks N` | tasks at once on this machine (split across its processes) |
| `--log-level LEVEL` | e.g. `INFO`, `DEBUG` |
| `--reload` | restart on file changes (needs `syntasks[reload]`) |
| `--reload-dir DIR` | directory to watch; repeat for several (default: current) |
| `--no-gitignore` | also reload on changes to git-ignored files |

See [Cluster](../guides/cluster.md) for what each process does.

## config

| Command | Does |
| --- | --- |
| `syntasks config check` | validates the file, imports the app and checks its tasks and schedules; exits 1 on a problem |
| `syntasks config show` | prints the effective config as TOML |
| `  --sources` | with the source of each value (default, file, profile, env, CLI) |

## queues

| Command | Does |
| --- | --- |
| `syntasks queues list` | every queue: waiting, running, delayed and dead counts, paused or not |
| `syntasks queues info Q` | the same for one queue |
| `syntasks queues peek Q [-n N]` | oldest waiting messages, without taking them |
| `syntasks queues pause Q` / `resume Q` | stop / restart workers taking messages (sending still works) |
| `syntasks queues purge Q [--yes]` | delete waiting and delayed messages |
| `syntasks queues dlq Q [-n N]` | dead letters, newest first, with their error |
| `syntasks queues replay Q [ID ...]` | send dead letters back (all, or these ids), with fresh retry counts |
| `syntasks queues purge-dlq Q` | delete all dead letters |

See [Queues](../guides/queues.md).

## schedules

| Command | Does |
| --- | --- |
| `syntasks schedules list` | every schedule with its next fire time and state |
| `  --task NAME` | only this task's |
| `  --source code\|config\|api` | only those declared in code, in the config file, or created at runtime |
| `syntasks schedules show ID` | one schedule in full |
| `syntasks schedules pause ID` / `resume ID` | stop / restart firing; kept across deploys |
| `syntasks schedules trigger ID` | run its task now, without changing its timing |
| `syntasks schedules delete ID` | delete a dynamic schedule (static ones are removed from code or config) |
| `syntasks schedules runs ID [-n N]` | recent runs: fire time, task id, status, duration |

See [Scheduling](../guides/scheduling.md).

## channels

| Command | Does |
| --- | --- |
| `syntasks channels list [--prefix P]` | channels used recently: messages, inbox, readers, which node hosts the handler, last activity |
| `syntasks channels info NAME` | one channel in full |
| `syntasks channels close NAME` | tell readers, stop its handler and delete it |

See [Channels](../guides/channels.md).

## nodes

`syntasks nodes` lists worker processes: node id, host, pid, alive or
dead (missed heartbeats), running tasks out of capacity, hosted channels,
queues and last heartbeat.

## tasks

| Command | Does |
| --- | --- |
| `syntasks tasks running` | tasks running now, from node heartbeats |
| `syntasks tasks cancel ID` | ask the node running it to cancel it |
| `syntasks tasks result ID` | its stored result or error |

## logs

```bash
syntasks logs TASK_ID [--follow]
```

Prints logs captured for a task that has `capture_logs = true`; `-f`
waits for new lines until the task ends. See
[Results and logs](../guides/results-and-logs.md).
