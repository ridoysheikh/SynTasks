"""
Channels: named message streams in Redis.

Channels can have long-running handlers that the cluster keeps alive.

* Any program can ``publish`` to a channel; any number of readers
  ``connect`` and receive every message (with replay from a message id),
  e.g. one websocket per reader.
* A channel can have a **handler**: a long-running coroutine registered
  with ``@app.channel("room:{room_id}")``. Exactly one instance runs per
  channel name across all workers; it reads the channel's inbox (messages
  clients ``send``) and publishes to its readers. If its worker dies,
  another worker takes over and continues from the last processed inbox
  message, with the state the handler saved.

Keys (under ``syntasks:{<app>}:ch:``): ``<name>:out`` (broadcast stream),
``<name>:in`` (inbox stream, consumer group ``handler``), ``<name>:state``,
``<name>:presence``, ``<name>:meta``, ``<name>:owner`` (lease),
``active`` (name → last activity), ``activate`` (activation requests),
``handlers`` (registered handler patterns).
"""

from syntasks.channels.channel import Channel, ChannelMessage, ChannelOverflowError
from syntasks.channels.manager import ChannelConnection, ChannelInfo, ChannelManager

__all__ = [
    "Channel",
    "ChannelConnection",
    "ChannelInfo",
    "ChannelManager",
    "ChannelMessage",
    "ChannelOverflowError",
]
