---
order: 1
dir:
  order: 1
---

# Introduction

SynTasks runs Python functions (async or sync) on a fleet of worker
machines, with Redis as the only moving part. It is built for clusters of
tens to hundreds of machines and keeps everything that must be shared
(queues, schedules, limits, channel ownership, node membership) in Redis.

## Core ideas

- **Redis is the source of truth.** No worker keeps cluster state only in
  memory, so any machine can die at any moment.
- **No special processes.** There is no separate scheduler or coordinator
  to keep alive. Every machine runs `syntasks worker`; jobs that need a
  single owner (dispatching schedules, cleanup, a channel's handler) are
  taken with **leases** and move to another machine when their owner dies.
- **Atomic decisions.** Taking a concurrency slot, firing a schedule,
  moving a delayed message: each is one Lua script inside Redis, so many
  machines racing is safe by construction.
- **One clock.** Due times, leases and expiries use Redis server time, so
  drifting machine clocks don't matter.
- **Everything bounded.** Payloads have size limits, runs have time
  limits, and every collection in Redis has a retention rule.
- **Opt-in cost.** Log capture, cluster-wide limits, per-key ordering and
  similar features cost nothing for tasks that don't use them.

## What's in the box

| Area | Pages |
|---|---|
| Running tasks | [Getting started](getting-started.md), [Tasks](tasks.md), [Results and logs](results-and-logs.md) |
| Operating them | [Configuration](configuration.md), [Queues](queues.md), [Limits](limits.md), [CLI](cli.md) |
| Time | [Scheduling](scheduling.md) |
| Long-running work | [Channels](channels.md) |
| How it works | [Cluster](cluster.md) |
| Tests | [Testing](testing.md) |
| Reference | [Configuration reference](../reference/config.md) |

## Requirements

Python 3.11+ and Redis 6.2+ (a single server, or behind Sentinel).
SynTasks has no required Python dependencies; optional extras add
Prometheus metrics, OpenTelemetry, uvloop, extra serializers and `--reload`.

## Origin

SynTasks started from the [taskiq](https://github.com/taskiq-python/taskiq)
codebase (MIT) by Pavel Kirilin and contributors, and keeps parts of it:
the task decorator and kicker, dependency injection, middlewares. The
runtime, queues, scheduler, channels and configuration are new.
