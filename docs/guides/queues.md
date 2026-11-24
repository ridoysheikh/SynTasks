# Queues

Every message goes to a queue: a set of Redis streams read by a consumer
group. Plain tasks need nothing more; tasks that need queue features opt
in.

## Named queues and routing

```python
app.queue("emails", max_length=1_000_000, on_full="reject")


@app.task(queue="emails")
async def send_email(to: str) -> None: ...
```

Machines choose what they serve, in priority order, in their config:

```toml
[worker]
queues = ["payments", "default"]
```

Tasks with `tags=["gpu"]` only run on machines whose `[worker] tags`
include all of them.

## Queue options

Declared in code with `app.queue(name, ...)` or `module.queue(...)`; a
queue no one declared gets the defaults.

| Option | Default | Meaning |
|---|---|---|
| `priorities` | `1` | priority levels, 0 (highest) to N-1 |
| `max_length` | none | waiting + delayed messages |
| `on_full` | `"reject"` | `send` raises `QueueFullError`; or `"drop_oldest"` |
| `max_deliveries` | `5` | crashed deliveries before a message is dead-lettered |
| `dead_letter_max` | `10000` | dead letters kept |

## Priorities

```python
app.queue("default", priorities=3)
await job.kicker().with_priority(0).kiq()
```

Workers take higher levels first.

## Ordering per key

```python
@app.task(key="kwargs.customer_id", max_tasks_per_key=1)
async def charge(customer_id: str, cents: int) -> None: ...
```

Calls with the same `customer_id` run one at a time, in the order they
were sent, across the whole cluster; different customers run in
parallel. A call whose key is busy waits in a per-key list and is handed
the slot when the previous call finishes. `max_tasks_per_key=N` allows N
at once per key.

## Per-key flow control

All of these need `key=`.

| Option | Does |
|---|---|
| `fair=True` | share the queue fairly between keys: waiting messages are kept per key and handed out one key at a time, round-robin, so a tenant with a million queued messages gets the same turns as a tenant with one |
| `debounce="2s"` | each call for a key replaces the waiting one and pushes its time back; it runs 2s after the last call, with the last arguments. Replaced calls' handles get `TaskSupersededError` |
| `throttle="10s"` | the first call per key runs; later calls within 10s are dropped and return the first call's handle |
| `rate_per_key="5/m"` | a rate limit per key ([Limits](limits.md#per-key)) |

```python
@app.task(fair=True, key="kwargs.tenant_id", max_tasks=50)
async def send_campaign_email(tenant_id: str, to: str) -> None: ...


@app.task(debounce="2s", key="kwargs.doc_id")
async def reindex(doc_id: str) -> None: ...
```

Debounce is atomic across clients: two web servers calling at once still
leave exactly one waiting run. Messages of a fair task that are delayed
(`with_delay`, retry backoff) skip the per-key turn and enter the queue
when due, and messages waiting in the per-key lists don't count toward
`max_length` or `syntasks queues list`.

## Operations

```python
await app.queues.list()                 # length, running, delayed, dead, paused, oldest
await app.queues.peek("default", 10)
await app.queues.pause("default")       # workers stop taking messages; sending still works
await app.queues.resume("default")
await app.queues.purge("default")       # delete waiting and delayed messages
await app.queues.dead_letters("default")
await app.queues.replay_dead("default") # all, or a list of ids
await app.queues.move("old", "new")
```

The same from the shell: `syntasks queues list|info|peek|pause|resume|purge|dlq|replay|purge-dlq`.

## Delivery guarantees

Delivery is at least once. A worker acknowledges a message after
storing its result (`[worker] ack`). While a task runs, its worker keeps
the message claimed; if the worker dies, another worker takes the
message over after `[worker] claim_idle`. Write tasks so that running
twice is safe, or use `unique_for` on the sending side.
