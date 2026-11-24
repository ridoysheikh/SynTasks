"""Live data: a channel with a handler, and a feed shared by every watcher."""

import random

from syntasks import Channel, ConnectRequest, Module

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


@module.feed("quote", every="300ms", linger="2s", normalize=str.upper)
async def quote(key: str) -> dict[str, float | int | str]:
    """
    The one place that asks the "vendor" for a symbol.

    It is counted in Redis so the demo can show how often it ran: once per
    round per watched symbol, however many users watch it.
    """
    from .app import app  # noqa: PLC0415

    calls = await app.redis.incr(app.keys("demo", "vendor", key))
    base = 100 + sum(map(ord, key)) % 50
    return {
        "symbol": key,
        "price": round(base + random.uniform(-1, 1), 2),
        "call": calls,
    }
