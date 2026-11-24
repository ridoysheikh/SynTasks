# Channels

A channel is a named, ordered message stream in Redis. Any number of
programs publish to it and read from it, and a channel can have one
long-running **handler** that the cluster keeps alive.

Use them for websocket rooms (every web server sees the same messages),
stateful loops per key (a game, a device session, a market feed), and
streaming output from tasks to browsers.

## Publishing and reading

```python
await app.channels.publish("news", {"headline": "..."})

async with app.channels.connect("news", last_id=resume_from) as conn:
    async for message in conn:          # ChannelMessage(id, data, sender, kind)
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

@app.channel("room:{room_id}", idle_timeout="10m")
async def room(ch: Channel, room_id: str) -> None:
    async for msg in ch.inbox():
        if msg.kind == "join":
            await ch.publish({"joined": msg.sender})
            continue
        ch.state["count"] = ch.state.get("count", 0) + 1
        await ch.publish({"from": msg.sender, "text": msg.data})
        await ch.save_state()
```

Clients `send` to the handler; it decides what to publish:

```python
async with app.channels.connect("room:42") as conn:
    await conn.send("hi")              # goes to the handler's inbox
    async for message in conn: ...     # what the handler publishes
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
  `max_lifetime`, or on `app.channels.close(name)`. A handler that keeps
  crashing is retried with backoff, then marked failed
  (`app.channels.restart(name)`).
- Connected readers send `join` and `leave` messages to the handler, and
  `ch.readers()` counts them.

## Sharing work

```python
async for message in app.channels.consume("jobs", group="resizers"):
    ...  # each message goes to one consumer of the group
```

## Limits and cleanup

| Setting | Default | |
|---|---|---|
| `channels.max_message` | 64 KiB | size of one message |
| `channels.retention` / `max_length` | 10 min / 10 000 | how much history is kept |
| `channels.inbox_max` | 10 000 | `send` raises `ChannelFullError` beyond it |
| `channels.idle_expiry` | 24 h | unused channels are deleted |

Operate them with `app.channels.list()`, `info(name)`, `close(name)` or
`syntasks channels list|info|close`.
