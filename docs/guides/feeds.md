# Feeds and watchlists

A feed is a shared data source: prices, a build status, a queue depth, a
vendor's order book. Thousands of users watch the same keys, but the source
should be asked **once**, by **one** machine, and **only while somebody
watches**. Two small pieces do that:

- **`@app.feed(...)`** declares the source: one function.
- **`app.feeds.watch(identity)`** is one user connection's view: groups of
  keys that the user subscribes to and drops at runtime.

Your web servers keep the sockets; SynTasks keeps the source.

## A polled feed

```python
@app.feed("quote", every="2s", normalize=str.upper)
async def quote(key: str) -> dict:
    return await vendor.get_quote(key)      # return None for "nothing new"
```

Every 2 seconds, for each key somebody watches, the cluster calls `quote(key)`
once, however many users and web servers watch it. Changed values are sent to
the watchers; unchanged ones aren't. This covers most feeds.

## A streamed feed

When the vendor pushes (a websocket carrying every symbol), declare the feed
without `every`. The function runs once for the whole kind and is told which
keys to serve:

```python
from syntasks import Feed


@app.feed("market", normalize=str.upper)
async def market(feed: Feed) -> None:
    async with vendor.connect() as socket:              # one upstream connection
        reader = asyncio.create_task(forward(socket, feed))
        try:
            while True:
                added, removed = await feed.changes()   # keys to (un)subscribe
                await socket.subscribe(added)
                await socket.unsubscribe(removed)
        finally:
            reader.cancel()


async def forward(socket, feed: Feed) -> None:
    async for frame in socket:
        await feed.publish(frame.symbol, frame.as_dict())
```

| `Feed` | |
|---|---|
| `await feed.changes()` | waits until the watched keys change; returns `(added, removed)`. The first call returns every key watched at that point |
| `feed.watched` | the keys watched now |
| `await feed.publish(key, data)` | sends a snapshot to the key's watchers and keeps it as the last value; marks the feed `live` |
| `await feed.set_state(name)` | reports your own health state (`"live"`, `"degraded"`, ...) |

If the function raises or returns, the feed is marked `degraded` and started
again after 0.2 s, doubling up to 30 s, for as long as keys are watched.

## Options

| Option | Default | Meaning |
|---|---|---|
| `every` | – | poll interval; with it the function is `fetch(key)`, without it `run(feed)` |
| `linger` | `"30s"` | how long the feed goes on after the last watcher left (navigating AAPL → MSFT → AAPL costs nothing) |
| `keep_last` | `"7d"` | how long a key's last value is kept |
| `shards` | `1` | handlers to spread the keys over (`crc32(key) % shards`); each is a cluster-wide single owner |
| `concurrency` | `20` | parallel fetches per round (polled feeds) |
| `normalize` | strip | canonical form of a key, e.g. `str.upper`; `aapl` and `AAPL` are one channel |
| `timeout`, `retries`, `max_tasks` | from the [module chain](modules.md) | required like for any channel; `max_tasks` must be at least `shards` |

Keys have at most 128 characters and no whitespace. The feed's data flows on
the channel `kind:key` (`quote:AAPL`).

## Watching

A web server opens one `Watch` per connection:

```python
async with app.feeds.watch("user:42") as watch:
    await watch.subscribe("watchlist", ["quote:AAPL", "quote:MSFT"])
    await watch.subscribe("header", ["quote:AAPL"])

    async for update in watch:              # one merged stream
        await websocket.send_json(update.to_dict())
```

- A **group** is a named list of channels (a watchlist, a header, a panel).
  A channel in two groups stays subscribed until both drop it.
- `await watch.unsubscribe("watchlist")` drops a group;
  `unsubscribe("watchlist", ["quote:MSFT"])` drops some of its channels.
- Partial success is normal: `subscribe` returns `(subscribed, rejected)`, and
  each rejection says why (`unknown_channel`, `invalid_key`, `channel_limit`).
  A connection holds at most `[channels] max_per_watch` (500) distinct channels.
- **Groups are saved per identity** (`[channels] watch_keep`, 30 days).
  A new tab starts empty; `await watch.restore()` joins its user's saved
  groups. It is explicit, because each tab shows its own panels. `await
  watch.saved()` reads them without joining.
- **A new subscriber gets the last value at once** (`update.cached` is True),
  so an illiquid symbol isn't blank until its next tick.
- **A slow reader gets the newest snapshot per channel**, not a backlog.
  Status changes come first, in order.
- The identity is whatever your login says. `Watch` doesn't authenticate;
  do it before you create one.

### An update

`Update(op, channel, data, cached)` with `op` either `"data"` (a snapshot) or
`"status"` (the feed's health changed; `data` is `{"state": "degraded"}`), so a
UI can grey a price that is no longer live. `update.to_dict()` is ready for JSON.

### Passing JSON through

`await watch.handle(request)` runs the same operations from a decoded JSON
request, so a websocket endpoint only passes messages along:

```python
@api.websocket("/ws")
async def sockets(ws: WebSocket) -> None:
    await ws.accept()
    async with app.feeds.watch(current_user(ws)) as watch:

        async def push() -> None:
            async for update in watch:
                await ws.send_json(update.to_dict())

        pusher = asyncio.create_task(push())
        try:
            while True:
                await ws.send_json(await watch.handle(await ws.receive_json()))
        except WebSocketDisconnect:
            pass
        finally:
            pusher.cancel()
```

| Request | Reply |
|---|---|
| `{"op": "subscribe", "group": "w", "channels": ["quote:AAPL"]}` | `{"op": "ack", "group": "w", "subscribed": [...], "rejected": [...]}` |
| `{"op": "unsubscribe", "group": "w"}` (`channels` optional) | `{"op": "ack", "group": "w", "unsubscribed": [...], "rejected": [...]}` |
| `{"op": "list"}` | `{"op": "list", "groups": {...}, "saved": {...}, "sources": {"quote": "live"}}`: `groups` is what this connection has joined, `saved` what the identity has saved (from any connection) |
| `{"op": "restore"}` | `{"op": "ack", "restored": {...}, "rejected": [...]}` |

An `id` in the request is echoed. A refused request gets
`{"op": "error", "error": "bad_group", "error_description": "..."}`.

## How it behaves

- **One owner, anywhere.** A feed is a channel handler (`feed:<kind>:<shard>`),
  so the cluster runs it on one worker and moves it, with its watchers
  unaffected, if that machine dies. Ticks published while it moves are lost;
  a source that can't miss any has to catch up from itself.
- **Only while watched.** Each watcher renews its keys every `linger / 3`; a
  key nobody renews lapses after `linger`, and a feed with no keys returns.
  The next `subscribe` starts it again, and a watcher's renewal restarts a
  feed that died.
- **Health.** A polled feed is `degraded` when every fetch of a round failed
  (one bad symbol doesn't count), `live` again when one works; a streamed feed
  is `degraded` while its function is failing. `await app.feeds.states()` and
  the `list` request report it per kind (`idle` before any handler ran).
- **Declare it where the web server can see it.** Watchers look up the feed's
  options (`normalize`, `linger`, shards) in the process they run in, so the
  module that declares it must be imported by the web server as well, like
  your tasks.

## Feed, channel or task?

| You need | Use |
|---|---|
| Many users watching the same keys of one source | a **feed** |
| One stateful, ordered loop per key (a chat, a game room) | a [channel handler](channels.md#handlers) |
| Work done once with retries and limits | a [task](tasks.md) |

A feed is for the latest value, not for events that must all arrive. Send
those through a task or your database, and use the feed to show them live.
