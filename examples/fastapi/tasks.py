import asyncio
import random
from typing import Annotated

from syntasks import Channel, ConnectRequest, SynTasks, SynTasksDepends
from syntasks.channels.channel import ChannelFencedError
from syntasks.depends.progress_tracker import ProgressTracker, TaskState

app = SynTasks(timeout="5m", retries=0, max_tasks=None)


@app.task(timeout="2m", max_tasks=4)  # at most 4 reports at once, on all workers
async def build_report(
    month: str,
    progress: Annotated[ProgressTracker[str], SynTasksDepends()],
) -> dict[str, str | int]:
    for step in range(1, 6):
        await asyncio.sleep(1)  # pretend to work
        await progress.set_progress(TaskState.STARTED, meta=f"{step * 20}%")
    return {"month": month, "orders": 1234}


@app.channel("room:{room}", idle_timeout="10m")
async def room(ch: Channel, room: str) -> None:
    """One handler per room, on one worker; it moves (with its state) if that dies."""
    async for message in ch.inbox():
        if message.kind != "message":
            continue
        ch.state["count"] = ch.state.get("count", 0) + 1
        await ch.publish(
            {"room": room, "n": ch.state["count"], "text": message.data["text"]},
        )
        await ch.save_state()


def watch_only(request: ConnectRequest) -> bool:
    """Anyone may watch a ticker; nobody may send to it."""
    return request.action == "read"


async def pump_prices(ch: Channel, symbol: str) -> None:
    """Stand-in for the single upstream feed of a symbol (a vendor websocket, say)."""
    price = 100.0 + random.random() * 50
    try:
        while True:
            price = round(price + random.uniform(-1, 1), 2)
            await ch.publish({"symbol": symbol, "price": price, "by": ch.consumer})
            await ch.sleep(0.5)
    except ChannelFencedError:
        return  # another worker owns the channel now


@app.channel("ticker:{symbol}", idle_timeout="30s", authorize=watch_only)
async def ticker(ch: Channel, symbol: str) -> None:
    """One feed per symbol in the whole cluster, however many users watch it."""
    feed = asyncio.create_task(pump_prices(ch, symbol))
    try:
        async for _ in ch.inbox():  # joins and leaves; ends when nobody watches
            pass
    finally:
        feed.cancel()
