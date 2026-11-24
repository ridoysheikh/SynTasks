# SynTasks documentation

SynTasks runs Python functions (async or sync) on a fleet of worker
machines, with Redis as the only other moving part. Each page answers one
question and stays short. Find what you want to do below and follow the link.

> **New here?** Read [Install](getting-started/install.md), then [Quickstart](getting-started/quickstart.md).

## Core ideas

- **Redis is the source of truth.** No worker keeps cluster state only in
  memory, so any machine can die at any moment.
- **No special processes.** Every machine runs `syntasks worker`. Jobs that
  need one owner (firing schedules, cleanup, a channel's handler) are taken
  with leases and move to another machine when their owner dies.
- **Atomic decisions.** Taking a concurrency slot, firing a schedule,
  moving a delayed message: each is one Lua script inside Redis, so many
  machines racing is safe.
- **One clock.** Due times, leases and expiries use Redis server time, so
  drifting machine clocks don't matter.
- **Everything bounded.** Payloads have size limits, runs have time limits,
  and everything kept in Redis has a retention rule.
- **Opt-in cost.** Log capture, cluster-wide limits, per-key ordering and
  similar features cost nothing for tasks that don't use them.

## I want to…

### Get started

| …do this | Read |
| --- | --- |
| install SynTasks and run Redis | [Install](getting-started/install.md) |
| go from zero to tasks running on workers | [Quickstart](getting-started/quickstart.md) |
| see schedules, limits, channels and groups running together | [`test-preview/`](../test-preview/README.md) |
| copy a small working app | [`examples/`](../examples/) |

### Write tasks

| …do this | Read |
| --- | --- |
| split a big app into files and folders, and set limits once per module | [Modules](guides/modules.md) |
| declare tasks and send them | [Tasks](guides/tasks.md) |
| send a call and wait for its value | [Tasks › Sending](guides/tasks.md#sending) (`call()`) |
| run something after a task, or after a whole group | [Callbacks and batches](guides/callbacks-and-batches.md) |
| run a campaign of thousands of sends you can pause and cancel | [Callbacks and batches › Batches](guides/callbacks-and-batches.md#batches-campaigns) |
| retry failed runs, or make a task unique for a while | [Tasks › Retries](guides/tasks.md#retries), [Tasks › Unique tasks](guides/tasks.md#unique-tasks) |
| wait for results, send a group, report progress | [Results and logs](guides/results-and-logs.md) |
| keep the logs a task prints | [Results and logs › Log capture](guides/results-and-logs.md#log-capture) |
| give tasks a database client, or other dependencies | [State and dependencies](guides/dependencies.md) |
| hook into every send and run (metrics, tracing) | [Middlewares](guides/middlewares.md) |
| test code that uses tasks | [Testing](guides/testing.md) |

### Run them in production

| …do this | Read |
| --- | --- |
| configure Redis and workers in `syntasks.toml` | [Configuration](guides/configuration.md) |
| use a different config per environment | [Configuration › Profiles](guides/configuration.md#profiles) |
| split work into queues, with priorities | [Queues](guides/queues.md) |
| run one task per customer at a time, in order | [Queues › Ordering per key](guides/queues.md#ordering-per-key) |
| stop one tenant's backlog from starving the others | [Queues › Per-key flow control](guides/queues.md#per-key-flow-control) (`fair`) |
| collapse bursts of calls into one (debounce), or drop repeats (throttle) | [Queues › Per-key flow control](guides/queues.md#per-key-flow-control) |
| inspect, pause, purge or replay a queue | [Queues › Operations](guides/queues.md#operations) |
| limit how many run at once, on a machine or across all of them | [Limits](guides/limits.md) |
| rate-limit calls to a partner API, or per account | [Limits › Cluster-wide concurrency and rate limits](guides/limits.md#cluster-wide-concurrency-and-rate-limits), [Per key](guides/limits.md#per-key) |
| stop runs that take too long or use too much memory | [Limits › Time limits](guides/limits.md#time-limits), [Process isolation](guides/limits.md#process-isolation) |
| know what happens when a machine dies, or during a deploy | [Cluster](guides/cluster.md) |

### Run things on time

| …do this | Read |
| --- | --- |
| run a task every 10 seconds, on cron, or "first Monday at 09:00" | [Scheduling](guides/scheduling.md) |
| let users schedule posts or emails, listed by owner, without a database table | [Runtime schedules](guides/runtime-schedules.md) |
| decide what happens after downtime, or when runs overlap | [Scheduling › Missed fires](guides/scheduling.md#missed-fires-run-as-soon-as-possible), [Policies](guides/scheduling.md#policies) |
| run a task once, later | [Tasks › Sending](guides/tasks.md#sending) (`with_delay`) |

### Keep something running

| …do this | Read |
| --- | --- |
| push updates to many readers (websocket rooms, live feeds) | [Channels › Publishing and reading](guides/channels.md#publishing-and-reading) |
| keep one stateful loop alive per room, game or device | [Channels › Handlers](guides/channels.md#handlers) |
| show prices or a status to thousands of users from one source | [Feeds and watchlists](guides/feeds.md) |
| let users keep several watchlists and change them while connected | [Feeds › Watching](guides/feeds.md#watching) |
| check who may join a channel, and see who is there | [Channels › Authorization](guides/channels.md#authorization), [Presence](guides/channels.md#presence) |
| stream a task's progress or output (AI tokens) to a browser | [Results › Live events](guides/results-and-logs.md#live-events) |

### Look something up

| …do this | Read |
| --- | --- |
| every setting, its default and scope | [Configuration reference](reference/config.md) |
| every `syntasks` command and flag | [CLI reference](reference/cli.md) |
| the Python API | [Python API](reference/python-api.md) |

### Work on SynTasks

| …do this | Read |
| --- | --- |
| run the tests | [Testing](development/testing.md) |
| cut a release | [Releasing](development/releasing.md) |

SynTasks started from the [taskiq](https://github.com/taskiq-python/taskiq)
codebase (MIT) by Pavel Kirilin and contributors, and keeps parts of it: the
task decorator and kicker, dependency injection, middlewares. The runtime,
queues, scheduler, channels and configuration are new. See
[THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md).
