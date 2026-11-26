# Everything on one page

Every task, schedule, channel and feed option, in a sentence each, and what
runs where when two machines are up. Details are in the linked guides.

## One command runs everything

```bash
syntasks run           # on worker1
syntasks run           # on worker2 (the same command, the same code)
```

That is the whole deployment. Each command starts `worker.processes`
processes (default 2); every process is a **node** that does all of this:

| Job | What the node does |
|---|---|
| tasks | takes messages from the queues and runs them |
| schedules | one node holds the `dispatcher` lease and fires due schedules (and moves delayed messages into queues) |
| channels and feeds | one node per channel name holds its handler and runs it |
| housekeeping | one node holds the `janitor` lease: frees dead nodes, deletes expired schedules |

There is **no separate scheduler process** and no "beat" to start, and so
nothing to keep at exactly one copy. With taskiq you run `taskiq worker` and
`taskiq scheduler` as two deployments, and the scheduler must stay single. Here
the single owner is chosen by a lease in Redis and moves to another node within
about one TTL when its owner dies.

A real run of the `test-preview` shop on two `syntasks run` commands
(worker1 and worker2, 2 processes each, so four nodes in one pool; throwaway
Redis):

```text
$ syntasks nodes
node                           state  running  channels  queues
ridoy-workstation-412697-9f66  alive  0/25     0         default
ridoy-workstation-412698-edaf  alive  0/25     1         default
ridoy-workstation-412699-7eef  alive  0/25     0         default
ridoy-workstation-412700-cea9  alive  0/25     0         default

$ syntasks schedules list
id                             task                           state   when                                next
heartbeat                      shop.marketing:heartbeat       active  every 2s                            2026-11-25T02:43:06
shop.marketing:monthly_report  shop.marketing:monthly_report  active  first Monday at 09:00 (Asia/Dhaka)  2026-12-07T09:00:00

$ syntasks channels list
channel     messages  inbox  readers  handler on
sales:live  5         0      0        ridoy-workstation-412698-edaf
```

Who held what in that run: `…700` the dispatcher (so it fired `heartbeat`
every 2 s; the *task* then ran on whichever node had a free slot), `…698` the
janitor and the `sales:live` handler. Stop the machine that owns a role and
another node takes it over.

Optional, per machine: `--queues a,b` to serve only some queues, `--processes N`,
`[scheduler] enabled = false` to keep a machine out of dispatching,
`[worker] tags` to route tasks. Nothing else needs a second command.

Your web server (FastAPI and so on) is not started by SynTasks. It sends
tasks and channel messages through the same Redis.

## Declaring: one app, modules, three required limits

```python
app = SynTasks(timeout="1m", retries=0, max_tasks=None)   # the root
orders = Module("orders", timeout="30s", retries=3, max_tasks=20)
app.extends(orders, marketing.module, live.module)
```

| | Meaning |
|---|---|
| `SynTasks(**options)` / `Module(name, **options)` | defaults for everything declared below them; the nearest one wins |
| `app.extends(module_or_"pkg.mod:attr", ...)` | attach modules (files and folders nest the same way) |
| `timeout`, `retries`, `max_tasks` | **required** for every task, schedule, channel and feed, from itself or a module above it; `None` means unbounded, `retries=0` means no retries |
| `app.queue(name, ...)` | declare a queue (below) |

`syntasks config check` imports the app and checks every declaration.

## Tasks: `@app.task(...)` ([guide](guides/tasks.md))

```python
@app.task("reports:build", queue="reports", timeout="20m", retries=2, max_tasks=4)
async def build_report(month: str) -> bytes: ...
```

The first argument is the task name (default `module:function`). `labels={...}`
adds labels to every message. A misspelled option is an error. Sync functions
run in a thread pool.

| Option | Meaning |
|---|---|
| **Time** | |
| `timeout` | hard limit; the run is cancelled |
| `soft_timeout` | earlier warning (must be below `timeout`) |
| `grace` | extra time to clean up after the cancel (default 30 s) |
| **Retries** | |
| `retries` | retries after an exception |
| `retry_delay` | first delay (1 s) |
| `retry_backoff` | multiplier (2) |
| `retry_max_delay` | delay cap (5 m) |
| `retry_jitter` | randomise delays (on) |
| **Limits across all machines** | |
| `max_tasks` | runs at once in the whole cluster |
| `max_tasks_per_node` | runs at once per worker process |
| `rate`, `rate_burst` | rate limit, e.g. `"10/s"` |
| **Per key** (each needs `key`) | |
| `key` | which argument groups calls: `"kwargs.customer"`, `"args.0"`, or a parameter name |
| `max_tasks_per_key` | runs at once per key; `1` = in order per key |
| `rate_per_key` | rate limit per key, e.g. `"5/m"` |
| `fair` | serve keys round-robin, so one big backlog can't starve others |
| `debounce` | run once, this long after the last call of a key, with its arguments |
| `throttle` | run the first call of a key, drop the rest for this long |
| **Routing** | |
| `queue` | queue name (`"default"`) |
| `priority` | `0` is highest |
| `tags` | only machines with these `[worker] tags` run it |
| **Safety** | |
| `unique_for`, `unique_key` | drop identical calls within a window (all arguments, or `unique_key`) |
| `isolation="process"` | each run in its own child process, hard-killable |
| `memory_limit` | memory cap of an isolated run |
| **After the run** | |
| `on_success`, `on_failure` | task sent with a `TaskOutcome` after success, or after the final failure |
| `events` | publish lifecycle events and `Emit` output to the channel `task:<id>` |
| `capture_logs` | keep the task's log lines |
| `store_result`, `result_ttl` | keep the result (on) for this long (1 day) |
| `max_payload`, `max_result` | size limits (256 KiB in; results unlimited) |
| `dead_letter` | keep final failures in the queue's dead letters (on) |

Not every combination is valid: `debounce` can't go with `throttle` or `fair`,
`throttle` not with `unique_for`, `memory_limit` needs `isolation="process"`.

**Sending**

| Call | Does |
|---|---|
| `await task.kiq(*args, **kw)` | send; returns a handle (`wait_result()`) |
| `await task.call(*args, **kw)` | send at top priority and wait for the value |
| `task.kicker().with_queue(q)` `.with_priority(n)` `.with_delay(s)` `.with_timeout(s)` | change one call, then `.kiq(...)` or `.call(...)` |
| `await app.send("pkg.mod:name", *args)` | send by name |
| `group = app.group()` | `await group.kiq(task, ...)` several, `await group.then(task)` runs once after the last, `group.as_completed()` yields them as they finish |
| `batch = await app.batch(id, owner=...)` | a named campaign: `kiq`, `map(task, items)`, `then`, `pause`, `resume`, `cancel`, `progress` ([guide](guides/callbacks-and-batches.md)) |

**Queues**: `app.queue("default", priorities=2, max_length=None, on_full="reject",
max_deliveries=5, dead_letter_max=10000)`. `on_full` is `"reject"` or
`"drop_oldest"`; `max_deliveries` is how many worker crashes a message survives
before it is dead-lettered. See [Queues](guides/queues.md).

## Schedules: `@app.schedule(timing, ...)` ([guide](guides/scheduling.md))

```python
@app.schedule(every("2s"), id="heartbeat", retries=0, max_tasks=1)
async def heartbeat(info: Annotated[ScheduleInfo, Depends()]) -> int: ...
```

A scheduled function is still a task: it takes every task option above and can
be sent by hand. Schedules live in Redis (never expire), are synced on every
worker start, and each fire runs **once** in the cluster. `timing` can be a
list; give each its own `id`.

**Timings**

| Timing | Fires |
|---|---|
| `every("15m")` | every interval; `anchor=` aligns it |
| `every("15m", weekday="mon-fri", between=("09:00","18:00"), tz="Europe/Berlin")` | every interval, only inside a daily window and on those dates |
| `cron("0 9 * * mon", tz=...)` | cron (5 fields, or 6 with seconds; `@hourly` macros) |
| `calendar(weekday="sun", at="09:00")` | calendar dates at times of day |
| `at("2026-12-01T09:00+06:00")` | once |
| `after("10m")` | once, this long after creation (runtime schedules only) |

`calendar(...)` takes `at` (one time or a list), `month`, `day` (negative counts
from the end: `-1` is the last day), `weekday` (`"mon"`, `"mon-fri"`), `nth`
(`1`–`5`, or `-1` for the last: "first Monday"), `week` plus `anchor` (every
N-th week), `exclude` (dates to skip) and `tz`. Every filter given must match.

Every timing also takes `id`, `args`, `kwargs` and the policies below.

**Policies**

| Option | Meaning |
|---|---|
| `misfire` | after downtime: `"once"` (default; run once now), `"all"` (every missed time, up to `max_catchup`), `"skip"` |
| `misfire_grace` | drop missed times older than this, whatever `misfire` says |
| `max_catchup` | cap for `"all"` (`[scheduler] max_catchup`, 10) |
| `overlap` | previous run still going: `"skip"` (default), `"allow"`, `"queue"` |
| `max_instances` | concurrent runs for `"queue"` (default 1) |
| `jitter` | random delay added to each fire |
| `start_at`, `end_at` | window in which it fires |
| `max_runs` | stop after N runs |
| `pause_after_failures` | pause after N failures in a row |
| `paused` | declare it paused |

Missed fires are caught up whenever any worker is back, not only at start.

**Many workers and one schedule**

- **Each fire runs once** in the whole cluster, however many workers run: one
  node claims it atomically. You don't need `max_tasks=1` to stop duplicates.
- **A run still going at the next fire** is handled by `overlap` (default
  `"skip"`: the late fire is dropped), not by `max_tasks`. If you don't want an
  extra cap, set `max_tasks=None`.
- **A crash can mean a second run**: another worker takes over the message. Make
  the work safe to repeat (a "done today" flag, a provider idempotency key, or
  `unique_for`).
- **One run uses one worker.** For a big job, make the schedule a small fan-out
  and let the pool do the work, limiting the per-item task:

```python
@app.schedule(every("1h"), timeout="1m", retries=0, max_tasks=None)
async def check_all() -> None:
    for sub_id in await load_active_subscription_ids():
        await check_one.kiq(sub_id)              # spread over every worker

@app.task(timeout="30s", retries=3, max_tasks=20, rate="50/s")   # protects the API
async def check_one(sub_id: str) -> None: ...
```

**Runtime schedules**: let your users create copies.

```python
@app.schedule(runtime=Runtime(max_per_owner=500, max_ttl="400d", min_interval="1m"),
              timeout="30s", retries=3, max_tasks=200)
async def publish_post(account: str, text: str) -> None: ...

await publish_post.schedule(at="2026-12-01T09:00+06:00", kwargs={"account": "a", "text": "Hi"},
                            owner="user:42", tags=["spring"], ttl="30d")
page = await publish_post.schedules.list(owner="user:42")
```

| `Runtime(...)` | Meaning |
|---|---|
| `max_per_owner` | copies one owner may have (1000) |
| `max_ttl` | longest life of a copy |
| `min_interval` | shortest recurrence a copy may use |
| `max_payload` | size of a copy's arguments |
| `keep_finished` | how long a finished one-shot copy is kept for the UI (7 d) |

`task.schedule(timing=None, *, args, kwargs, every, cron, tz, at, after, id, ttl,
expires_at, if_exists="error"|"replace"|"ignore", owner, tags, meta, **policies)`
creates a copy. `task.schedules` lists, updates and cancels them
([guide](guides/runtime-schedules.md)).

**CLI**: `syntasks schedules list | show | preview | pause | resume | trigger | delete | cancel | runs`.

## Channels: `@app.channel(pattern, ...)` ([guide](guides/channels.md))

For one long-running, stateful handler per name (a chat, a game room), anywhere
in the cluster.

```python
@app.channel("room:{room_id}", timeout=None, retries=5, max_tasks=1000, idle_timeout="10m")
async def room(ch: Channel, room_id: str) -> None:
    async for msg in ch.inbox():
        await ch.publish({"from": msg.sender, "text": msg.data})
```

| Option | Meaning |
|---|---|
| `pattern` | name with parameters, passed to the handler (`"room:{room_id}"`) |
| `timeout` | longest life of one run; `None` = until idle or closed |
| `retries` | restarts after a crash (within 10 minutes) before it is marked failed |
| `max_tasks` | live handlers of this pattern in the cluster; extra channels wait with their inbox |
| `idle_timeout` | stop after this long without messages and readers (10 m) |
| `authorize` | `fn(ConnectRequest) -> bool` (may be async), called in your web process for `read` and `send` |

Inside the handler: `ch.inbox()` (messages sent to it), `ch.publish(data)` (to
all readers), `ch.state` and `await ch.save_state()` (survive a takeover),
`await ch.readers()`, `await ch.sleep(s)`.

From your server: `app.channels.send(name, data)`, `.publish(...)`,
`.connect(name, identity=, meta=, last_id=, presence=True)` (an `async with`
that you iterate), `.presence(name)`, `.history(...)`, `.consume(name, group=)`,
`.close(name)`, `.restart(name)`, `.info(name)`, `.list(prefix)`.

One handler per channel name in the whole cluster; if its node dies another
starts it with the saved state and the unfinished inbox. `[channels]` sets
`max_handlers` per process, `max_message`, `retention`, `lease_ttl`, and more
([config](reference/config.md)).

**CLI**: `syntasks channels list | info | close`.

## Feeds: `@app.feed(kind, ...)` ([guide](guides/feeds.md))

For data many users watch (prices, status): the source is asked **once**, by
one node, only while somebody watches. A feed is a channel handler underneath.

```python
@app.feed("quote", every="2s", linger="30s", normalize=str.upper)
async def quote(key: str) -> dict: ...                # polled; None = nothing new

async with app.feeds.watch("user:42") as watch:       # per connection
    await watch.subscribe("watchlist", ["quote:AAPL", "quote:MSFT"])
    async for update in watch: ...
```

| Option | Meaning |
|---|---|
| `kind` | feed name; its channels are `kind:key` |
| `every` | poll interval, function `fetch(key)`; without it, `run(feed)` is one upstream stream |
| `linger` | keep running this long after the last watcher (30 s) |
| `keep_last` | how long a key's last value is kept (7 d), sent at once to new watchers |
| `shards` | handlers to spread keys over (1); `max_tasks` must be at least this |
| `concurrency` | parallel fetches per round (20) |
| `normalize` | canonical key, e.g. `str.upper` |
| `timeout`, `retries`, `max_tasks` | required, like a channel |

`Watch`: `subscribe(group, channels)`, `unsubscribe(group, channels=None)`,
`restore()`, `saved()`, `handle(json_request)`. Groups are saved per identity.

## Where each option comes from

| Where | What |
|---|---|
| Your code | every task, schedule, channel, feed and queue option above |
| `syntasks.toml` | infrastructure only: Redis URL, worker processes, node and lease timings, scheduler and channel settings ([config reference](reference/config.md)) |
| CLI flags | `--processes`, `--queues`, `--max-async-tasks`, `--profile` ([CLI](reference/cli.md)) |
