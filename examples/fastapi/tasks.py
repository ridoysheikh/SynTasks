import asyncio
import random
from typing import Annotated

from syntasks import Channel, SynTasks, SynTasksDepends
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


PRICES: dict[str, float] = {}


@app.feed("quote", every="1s", linger="30s", normalize=str.upper)
async def quote(key: str) -> dict[str, float | str]:
    """
    The one place that asks the vendor for a symbol.

    The feed calls this once a second for every symbol somebody watches, on
    one worker for the whole cluster, however many users and web servers are
    watching, and stops 30 seconds after the last one leaves. Here the
    "vendor" is a random walk.
    """
    price = PRICES.get(key) or 100.0 + (sum(map(ord, key)) % 50)
    PRICES[key] = round(price + random.uniform(-1, 1), 2)
    return {"symbol": key, "price": PRICES[key]}
