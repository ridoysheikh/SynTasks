import asyncio
from typing import Annotated

from syntasks.dependencies import Depends
from syntasks.depends.progress_tracker import ProgressTracker, TaskState


async def my_task(progress: Annotated[ProgressTracker[str], Depends()]) -> None:
    for i in range(10):
        await asyncio.sleep(1)
        await progress.set_progress(TaskState.STARTED, meta=f"{(i + 1) * 10}%")
