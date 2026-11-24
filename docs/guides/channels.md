# Channels

A channel is a named, ordered message stream in Redis. Any number of
programs publish to it and read from it, and a channel can have one
long-running **handler** that the cluster keeps alive.

Use them for websocket rooms (every web server sees the same messages),
stateful loops per key (a game, a device session, a market feed), and
streaming output from tasks to browsers.

## Channel or task?

Your web servers (FastAPI, Go, ...) keep the user sockets. SynTasks keeps
what must exist once, or in order, for the whole cluster, however many
web servers and users there are.

| You need | Use |
|---|---|
| Work done once, anywhere, with retries and limits | a [task](tasks.md) |
| One thing per key that stays alive: a shared feed, a session with state | a channel handler |
| Messages for one key processed in order, with saved state | a channel handler |
| Live push to many connected users | a channel (readers) |
| A result or alert that must not be lost | a task or your database; use the channel to deliver it live |

A channel keeps 10 minutes / 10 000 messages of history. A reader that was
away longer gets a `gap` message, so don't make a channel the only copy of
anything that matters.


## Publishing and reading

```python
await app.channels.publish("news", {"headline": "..."})

async with app.channels.connect("news", last_id=resume_from) as conn:
    async for message in conn:  # ChannelMessage(id, data, sender, kind)
        await websocket.send_json(message.to_dict())
```

- Readers get every message published after they connect, or after
  `last_id` to resume after a reconnect. If the messages after `last_id`
  were already trimmed, the reader first gets a message of kind `gap`.
- All readers in one process share one Redis connection per few hundred
  channels, so 10 000 websockets don't need 10 000 connections.
- A reader that falls more than `channels.reader_buffer` messages behind
  gets `ChannelOverflowError` and can reconnect with its `last_id`.

## Handlers

```python
from syntasks import Channel


@app.channel(
    "room:{room_id}", timeout=None, retries=5, max_tasks=1000, idle_timeout="10m"
)
async def room(ch: Channel, room_id: str) -> None:
    async for msg in ch.inbox():
        if msg.kind == "join":
            await ch.publish({"joined": msg.data["identity"]})
            continue
        ch.state["count"] = ch.state.get("count", 0) + 1
        await ch.publish({"from": msg.sender, "text": msg.data})
        await ch.save_state()
```

Like tasks, handlers need the three limits, from themselves or a
[module](modules.md) above them:

| Option | For a handler |
|---|---|
| `timeout` | longest life of one run; `None` for "until idle or closed" |
| `retries` | restarts after a crash (within 10 minutes) before it's marked failed |
| `max_tasks` | live handlers of this pattern across the cluster; a channel waiting for a slot keeps its inbox and starts when one frees |
| `idle_timeout` | stop after this long without messages and readers (default 10 m) |
| `authorize` | who may read and send ([below](#authorization)) |

Clients `send` to the handler; it decides what to publish:

```python
async with app.channels.connect(
    "room:42", identity="user:7", meta={"name": "Ada"}
) as conn:
    await conn.send("hi")  # goes to the handler's inbox
    async for message in conn:
        ...  # what the handler publishes
```

- **One instance per channel name** in the whole cluster. It starts on
  demand when the first message is sent, on a worker with free capacity
  (`channels.max_handlers` per process).
- **Takeover:** if its worker dies, the ownership lease expires and
  another worker starts the handler, with the state from the last
  `save_state()` and the inbox messages that weren't finished.
- A stale owner (a paused process that lost its lease) can't publish or
  save state: the writes are rejected.
- Inbox messages are acknowledged when the loop asks for the next one,
  so a crash mid-message means it is processed again.
- It stops after `idle_timeout` without messages and readers, after
  `timeout`, or on `app.channels.close(name)`. A handler that keeps
  crashing is retried with backoff `retries` times, then marked failed
  (`app.channels.restart(name)`).

## Authorization

```python
from syntasks import ConnectRequest


def members_only(request: ConnectRequest) -> bool:  # may be async
    team = request.params["team"]
    if request.action == "send":
        return is_admin(request.identity, team)
    return is_member(request.identity, team)


@app.channel(
    "team:{team}", authorize=members_only, timeout=None, retries=3, max_tasks=None
)
async def team(ch: Channel, team: str) -> None: ...
```

`authorize` runs where a reader connects (`action="read"`) and sends
(`action="send"`), in the web process, before anything touches Redis. A
refusal raises `ChannelDenied`. `ConnectRequest` has the channel `name`,
its pattern `params`, the reader's `identity` and `meta`, and the
`action`. Server code calling `app.channels.send` / `publish` is trusted
and not checked; call `await app.channels.authorize(name, identity, meta,
action)` yourself if you forward user input that way.

## Presence

```python
present = await app.channels.presence(
    "room:42"
)  # [Presence(reader_id, identity, meta)]
```

Readers connected with `presence=True` (the default) are listed while
connected, and the handler gets a `join` message when they connect and a
`leave` message when they go; both carry `{"identity": ..., "meta": ...}`
and `sender` is the reader id. `ch.readers()` counts them.

For per-user notifications, publish to a `user:<id>` channel with no
handler; every connection of that user receives it.

## Sharing work

```python
async for message in app.channels.consume("jobs", group="resizers"):
    ...  # each message goes to one consumer of the group
```

## Patterns

### One feed, many watchers

A market app where thousands of users watch the same tickers: the upstream
feed should be opened once per ticker for the whole cluster, not once per
user or per web server.

```python
@app.channel(
    "ticker:{symbol}", timeout=None, retries=5, max_tasks=500, idle_timeout="1m"
)
async def ticker(ch: Channel, symbol: str) -> None:
    feed = asyncio.create_task(pump_prices(ch, symbol))  # opens ONE upstream feed
    try:
        async for _ in ch.inbox():  # joins and leaves; ends when nobody is watching
            pass
    finally:
        feed.cancel()
```

Each web server connects its users with `app.channels.connect("ticker:AAPL")`
and forwards to their sockets. The first reader starts the handler (a
connecting reader sends a `join` to it), `idle_timeout` stops it when nobody
is left, and if its machine dies another one starts it again and the feed
reopens there. Ticks sent while it moves are not replayed, so a feed that
can't miss ticks has to catch up from its source. Ticks are published to
all readers; to give each user their own alerts, add an `authorize` and
filter in the web server, or publish to `user:<id>` channels.

Run it: `python -m shop ticker` in [test-preview](../../test-preview/README.md),
and `WS /ticker/{symbol}` in [examples/fastapi](../../examples/fastapi/README.md).

### A conversation per user (a chat bot)

A webhook (WhatsApp, Telegram, ...) answers fast and hands each message to
a channel named after the conversation. Conversations spread over the
cluster; each one is handled in order, by one handler, with its history.

```python
@api.post("/webhook")
async def webhook(payload: Incoming) -> dict[str, str]:
    await app.channels.send(f"chat:{payload.phone}", {"text": payload.text})
    return {"status": "ok"}


@app.channel(
    "chat:{phone}", timeout=None, retries=5, max_tasks=5000, idle_timeout="30m"
)
async def chat(ch: Channel, phone: str) -> None:
    async for message in ch.inbox():
        history = ch.state.setdefault("history", [])
        history.append(message.data["text"])
        del history[:-10]  # keep the state small
        reply = await answer.call(
            phone, history
        )  # slow work on the worker pool, in order
        await send_whatsapp.kiq(phone, reply)
        await ch.save_state()
```

The handler only decides and waits; the slow part (an LLM call, a lookup)
is a task, so `max_tasks` and `rate` on that task protect the provider for
the whole cluster. A message is acknowledged when the loop asks for the
next one, so after a crash the last message can run again: make `answer`
and `send_whatsapp` safe to repeat (for instance with `unique_for`). If
you don't need order or history per conversation, skip the channel and
call `answer.kiq(...)` from the webhook.

## Limits and cleanup

| Setting | Default | |
|---|---|---|
| `channels.max_message` | 64 KiB | size of one message |
| `channels.retention` / `max_length` | 10 min / 10 000 | how much history is kept |
| `channels.inbox_max` | 10 000 | `send` raises `ChannelFullError` beyond it |
| `channels.idle_expiry` | 24 h | unused channels are deleted |

Operate them with `app.channels.list()`, `info(name)`, `close(name)` or
`syntasks channels list|info|close`.
