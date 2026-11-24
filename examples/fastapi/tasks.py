import asyncio
from typing import Annotated

from syntasks import Channel, SynTasks, SynTasksDepends
from syntasks.depends.progress_tracker import ProgressTracker, TaskState

app = SynTasks()


@app.task(timeout="2m", concurrency=4)  # at most 4 reports at once, across all workers
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
