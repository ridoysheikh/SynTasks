import asyncio
import contextlib
from datetime import UTC, datetime, timedelta

from syntasks import SynTasksScheduler
from syntasks.api import run_scheduler_task
from syntasks.schedule_sources import LabelScheduleSource
from tests.utils import AsyncQueueBroker


async def test_successful() -> None:
    broker = AsyncQueueBroker()
    scheduler = SynTasksScheduler(broker, sources=[LabelScheduleSource(broker)])
    scheduler_task = asyncio.create_task(run_scheduler_task(scheduler))

    @broker.task(schedule=[{"time": datetime.now(UTC) - timedelta(seconds=1)}])
    def _() -> None: ...

    msg = await asyncio.wait_for(broker.queue.get(), 2)
    assert msg

    scheduler_task.cancel()


async def test_cancelation() -> None:
    broker = AsyncQueueBroker()
    scheduler = SynTasksScheduler(broker, sources=[LabelScheduleSource(broker)])

    @broker.task(schedule=[{"time": datetime.now(UTC)}])
    def _() -> None: ...

    scheduler_task = asyncio.create_task(run_scheduler_task(scheduler))

    msg = await asyncio.wait_for(broker.queue.get(), 2)
    assert msg

    scheduler_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await scheduler_task

    assert scheduler_task.cancelled()
