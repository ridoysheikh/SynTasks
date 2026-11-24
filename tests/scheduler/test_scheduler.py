from syntasks.abc.schedule_source import ScheduleSource
from syntasks.brokers.inmemory_broker import InMemoryBroker
from syntasks.exceptions import ScheduledTaskCancelledError
from syntasks.scheduler.scheduled_task import ScheduledTask
from syntasks.scheduler.scheduler import SynTasksScheduler


class CancellingScheduleSource(ScheduleSource):
    async def get_schedules(self) -> list["ScheduledTask"]:
        """Return schedules list."""
        return []

    def pre_send(
        self,
        task: "ScheduledTask",
    ) -> None:
        """Raise cancelled error."""
        raise ScheduledTaskCancelledError


async def test_scheduled_task_cancelled() -> None:
    broker = InMemoryBroker()
    source = CancellingScheduleSource()
    scheduler = SynTasksScheduler(broker=broker, sources=[source])
    task = ScheduledTask(
        task_name="ping:pong",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )

    await scheduler.on_ready(source, task)  # error is caught
