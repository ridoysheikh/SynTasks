"""Redis schedule source."""

import logging
from typing import Any

from syntasks.abc.schedule_source import ScheduleSource
from syntasks.abc.serializer import SynTasksSerializer
from syntasks.redis.broker import DEFAULT_URL, make_client
from syntasks.redis.client import Redis
from syntasks.scheduler.scheduled_task import ScheduledTask
from syntasks.serializers import JSONSerializer

logger = logging.getLogger(__name__)


class RedisScheduleSource(ScheduleSource):
    """
    Persistent schedules stored in Redis.

    Schedules live in one hash, ``{prefix}``, keyed by schedule id, so
    they survive restarts and can be added or removed at runtime from
    any process (``kicker.schedule_by_cron(source, ...)`` etc.).
    One-off ``time`` schedules are deleted once they've been sent.
    """

    def __init__(
        self,
        url: str = DEFAULT_URL,
        *,
        prefix: str = "syntasks:schedules",
        buffer_size: int = 500,
        serializer: SynTasksSerializer | None = None,
        client: Redis | None = None,
        max_connection_pool_size: int | None = None,
        **connection_kwargs: Any,
    ) -> None:
        """
        Create the schedule source.

        :param url: Redis URL.
        :param prefix: key of the hash holding the schedules.
        :param buffer_size: schedules read per round trip.
        :param serializer: how schedules are encoded; JSON by default.
        :param client: share an existing client.
        :param max_connection_pool_size: connections in the pool.
        :param connection_kwargs: extra connection options.
        """
        self.prefix = prefix
        self.buffer_size = buffer_size
        self.serializer = serializer or JSONSerializer()
        self.redis, self._owns_client = make_client(
            url,
            client,
            max_connection_pool_size,
            connection_kwargs,
        )

    async def shutdown(self) -> None:
        """Close the source's connections."""
        if self._owns_client:
            await self.redis.close()

    async def add_schedule(self, schedule: ScheduledTask) -> None:
        """
        Store a schedule (replacing one with the same id).

        :param schedule: schedule to store.
        """
        await self.redis.hset(
            self.prefix,
            {schedule.schedule_id: self.serializer.dumpb(schedule.to_dict())},
        )

    async def delete_schedule(self, schedule_id: str) -> None:
        """
        Remove a schedule.

        :param schedule_id: id of the schedule.
        """
        await self.redis.hdel(self.prefix, schedule_id)

    async def get_schedules(self) -> list[ScheduledTask]:
        """
        Read every stored schedule.

        Entries that can't be decoded are logged and skipped.

        :return: schedules.
        """
        schedules = []
        async for schedule_id, raw in self.redis.hscan_iter(
            self.prefix,
            count=self.buffer_size,
        ):
            try:
                schedules.append(ScheduledTask.from_dict(self.serializer.loadb(raw)))
            except Exception:
                logger.exception(
                    "Cannot decode schedule %s; skipping it.",
                    schedule_id.decode(errors="replace"),
                )
        return schedules

    async def post_send(self, task: ScheduledTask) -> None:
        """
        Delete one-off schedules after they were sent.

        :param task: schedule that was just sent.
        """
        if task.time is not None and task.cron is None and task.interval is None:
            await self.delete_schedule(task.schedule_id)
