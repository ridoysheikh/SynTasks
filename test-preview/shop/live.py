"""Live channels: one handler per channel for the whole cluster."""

import asyncio
import random

from syntasks import Channel, ConnectRequest, Module
from syntasks.channels.channel import ChannelFencedError

module = Module("live", timeout=None, retries=5, max_tasks=10)
STAFF = {"staff:ada", "staff:alan"}


def staff_only(request: ConnectRequest) -> bool:
    """Anyone on the staff may watch; only Ada may post announcements."""
    if request.action == "send":
        return request.identity == "staff:ada"
    return request.identity in STAFF


@module.channel("sales:live", authorize=staff_only, idle_timeout="5m")
async def live_sales(ch: Channel) -> None:
    async for message in ch.inbox():
        if message.kind in ("join", "leave"):
            await ch.publish({"event": message.kind, **(message.data or {})})
            continue
        ch.state["messages"] = ch.state.get("messages", 0) + 1
        await ch.save_state()
        await ch.publish(
            {"announcement": message.data, "n": ch.state["messages"], "by": ch.consumer}
        )


async def pump_prices(ch: Channel, symbol: str) -> None:
    """Stand-in for the one upstream market feed of a symbol."""
    price = 100.0 + random.random() * 50
    tick = 0
    try:
        while True:
            tick += 1
            price = round(price + random.uniform(-1, 1), 2)
            await ch.publish(
                {
                    "symbol": symbol,
                    "tick": tick,
                    "price": price,
                    "feed": ch.state["feeds_opened"],
                    "by": ch.consumer,
                }
            )
            await ch.sleep(0.2)
    except ChannelFencedError:
        return  # another node owns the channel now


@module.channel("ticker:{symbol}", idle_timeout="3s")
async def ticker(ch: Channel, symbol: str) -> None:
    """One feed per symbol in the whole cluster, however many users watch it."""
    ch.state["feeds_opened"] = ch.state.get("feeds_opened", 0) + 1
    await ch.save_state()
    feed = asyncio.create_task(pump_prices(ch, symbol))
    try:
        async for _ in ch.inbox():  # joins and leaves; ends when nobody watches
            pass
    finally:
        feed.cancel()
