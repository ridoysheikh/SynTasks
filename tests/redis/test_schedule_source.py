import uuid
from datetime import UTC, datetime, timedelta

from syntasks import RedisListBroker, RedisScheduleSource
from syntasks.redis import Redis
from syntasks.scheduler.scheduled_task import ScheduledTask


def _prefix() -> str:
    return f"sched-{uuid.uuid4().hex}"


async def test_add_get_delete(redis_url: str) -> None:
    source = RedisScheduleSource(redis_url, prefix=_prefix(), buffer_size=1)
    cron = ScheduledTask(
        task_name="t",
        labels={"a": 1},
        args=[1],
        kwargs={"k": "v"},
        cron="*/5 * * * *",
        cron_offset=timedelta(hours=2),
    )
    interval = ScheduledTask(task_name="t", labels={}, args=[], kwargs={}, interval=30)
    await source.add_schedule(cron)
    await source.add_schedule(interval)
    schedules = {s.schedule_id: s for s in await source.get_schedules()}
    assert schedules == {cron.schedule_id: cron, interval.schedule_id: interval}
    await source.delete_schedule(cron.schedule_id)
    assert [s.schedule_id for s in await source.get_schedules()] == [
        interval.schedule_id,
    ]
    await source.shutdown()


async def test_one_off_schedule_removed_after_send(redis_url: str) -> None:
    source = RedisScheduleSource(redis_url, prefix=_prefix())
    when = datetime.now(UTC) + timedelta(minutes=5)
    task = ScheduledTask(task_name="t", labels={}, args=[], kwargs={}, time=when)
    await source.add_schedule(task)
    stored = await source.get_schedules()
    assert stored[0].time == when
    await source.post_send(stored[0])
    assert await source.get_schedules() == []
    await source.shutdown()


async def test_corrupt_entries_are_skipped(redis_url: str) -> None:
    prefix = _prefix()
    source = RedisScheduleSource(redis_url, prefix=prefix)
    good = ScheduledTask(task_name="t", labels={}, args=[], kwargs={}, interval=5)
    await source.add_schedule(good)
    client = Redis.from_url(redis_url)
    await client.hset(prefix, {"broken": b"not json"})
    await client.close()
    assert await source.get_schedules() == [good]
    await source.shutdown()


async def test_schedule_through_kicker(redis_url: str) -> None:
    broker = RedisListBroker(redis_url, queue_name=_prefix())
    source = RedisScheduleSource(redis_url, prefix=_prefix())

    @broker.task
    async def job(x: int) -> None: ...

    created = await job.kicker().schedule_by_cron(source, "0 * * * *", 5)
    [stored] = await source.get_schedules()
    assert stored.schedule_id == created.schedule_id
    assert stored.cron == "0 * * * *"
    assert stored.args == [5]
    await created.unschedule()
    assert await source.get_schedules() == []
    await source.shutdown()
    await broker.shutdown()
