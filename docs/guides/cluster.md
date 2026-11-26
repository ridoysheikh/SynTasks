# Cluster

How SynTasks behaves on 10 to 100 machines.

## Processes

`syntasks run` is a supervisor. It starts `worker.processes` worker
processes and restarts them: right away when they exit to be recycled
(memory soft limit, a stuck task, `max_tasks_per_process`), with a short
backoff after a crash (`worker.max_restarts` stops the supervisor after
too many). `SIGTERM`/`SIGINT` stop everything gracefully; `SIGHUP`
restarts the workers.

Each worker process is a **node**: it registers in Redis, sends a
heartbeat every `node.heartbeat`, and is considered dead after
`node.dead_after` without one.

```bash
syntasks nodes
```

## Roles

Some jobs need exactly one owner. Every node competes for them with a
**lease** (a Redis key with a TTL that the owner renews):

| Role | Job |
|---|---|
| `dispatcher:<shard>` | move due delayed messages into queues; fire due schedules |
| `janitor` | remove dead nodes and free their slots; delete expired schedules; restart orphaned channel handlers; unpark waiting messages; remove old consumers |
| channel owner | one per channel with a handler |

If the owner dies, its lease expires and another node takes over within
about one TTL. If an owner fails to renew, it stops the work at once.
Leases carry a fencing token, so writes from a stale owner are rejected.

Only the dispatcher polls for due work, so polling doesn't grow with the
number of machines. For very large schedule counts, `scheduler.shards`
splits dispatching across several leases.

## Time

Due times, leases, expiries and rate limits use Redis server time,
inside Lua scripts. Python code keeps the offset between its clock and
Redis's. Machine clock drift doesn't matter.

## Deploys

- Set `[app] revision` to a number that grows with each deploy. Workers
  publish the limits their code declares and sync declared schedules with
  it; an older revision never overrides a newer one, and uses the newer
  limits for the tasks it also has.
- A worker that can't start (the app doesn't import, a startup handler
  raises) exits with code 78; the supervisor retries with a growing delay
  and stops after `[worker] max_startup_failures` (5) in a row, so a
  broken deploy fails loudly instead of restarting forever.
- Rolling restarts are safe: running tasks finish within
  `worker.shutdown_grace`, messages a process took but didn't start are
  put back, and anything a dead process held is taken over.

## Redis keys

All keys of an app start with `syntasks:{<app.name>}:`. Two apps can
share a Redis server.

| Prefix | Used by |
|---|---|
| `q:` | queues, delayed messages, dead letters, parked messages |
| `lim:` | semaphores, rate limits, wake lists |
| `sched:` | schedules |
| `ch:` | channels |
| `res:` | results, done notifications, groups |
| `log:` | captured logs |
| `node:` | node registry |
| `lease:` | leases |
| `cfg:` | published cluster settings |
| `ctl` | control events (pub/sub) |

Schedules never have a Redis TTL. Results, captured logs and unused
channels expire.

## Failure cases

| What fails | What happens |
|---|---|
| A worker process crashes | its running messages are taken over after `worker.claim_idle`; its slots are freed after `node.dead_after`; the supervisor starts a new process |
| A whole machine dies | same, for all its processes; its leased roles move within one TTL |
| The dispatcher dies mid-fire | the fire is sent by the next dispatcher, once |
| Redis restarts | workers reconnect with backoff; state survives if Redis persistence is on |
| A task hangs | its timeout cancels it; if it ignores cancellation, its process is recycled |
