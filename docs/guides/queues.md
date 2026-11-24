# Queues

Every message goes to a queue: a set of Redis streams read by a consumer
group. Plain tasks need nothing more; tasks that need queue features opt
in.

## Named queues and routing

```python
@app.task(queue="emails")
async def send_email(to: str) -> None: ...
```

Machines choose what they serve, in priority order:

```toml
[worker]
queues = ["payments", "default"]
```

Tasks with `tags = ["gpu"]` only run on machines whose `worker.tags`
include all of them.

## Priorities

```toml
[queues.default]
priorities = 3        # levels 0 (highest) .. 2
```

```python
await job.kicker().with_priority(0).kiq()
```

Workers take higher levels first.

## Ordering per key

```python
@app.task(concurrency_key="customer_id")   # key_concurrency defaults to 1
async def charge(customer_id: str, cents: int) -> None: ...
```

Calls with the same `customer_id` run one at a time, in the order they
were sent, across the whole cluster; different customers run in
parallel. A call whose key is busy waits in a per-key list and is handed
the slot when the previous call finishes. `key_concurrency = N` allows N
at once per key.

## Limits

```toml
[queues.default]
max_length = 100000     # waiting + delayed messages
on_full = "reject"      # kick raises QueueFullError; or "drop_oldest"
max_deliveries = 5      # crashes before a message is dead-lettered
dead_letter_max = 10000 # dead letters kept
```

## Operations

```python
await app.queues.list()                 # length, running, delayed, dead, paused, oldest
await app.queues.peek("default", 10)
await app.queues.pause("default")       # workers stop taking messages; kicks still work
await app.queues.resume("default")
await app.queues.purge("default")       # delete waiting and delayed messages
await app.queues.dead_letters("default")
await app.queues.replay_dead("default") # all, or a list of ids
await app.queues.move("old", "new")
```

The same from the shell: `syntasks queues list|info|peek|pause|resume|purge|dlq|replay|purge-dlq`.

## Delivery guarantees

Delivery is at least once. A worker acknowledges a message after
storing its result (`worker.ack`). While a task runs, its worker keeps the
message claimed; if the worker dies, another worker takes the message
over after `worker.claim_idle`. Write tasks so that running twice is
safe, or use `unique_for` on the sending side.
