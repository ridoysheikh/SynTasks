"""
Scheduling inside the library, stored in Redis.

Declare schedules next to tasks::

    from syntasks import SynTasks, every, cron

    app = SynTasks()

    @app.task(schedule=every("10s"))
    async def poll() -> None: ...

    @app.task(schedule=cron("0 30 9 * * mon-fri", tz="Europe/London"))
    async def report() -> None: ...

or create them at runtime::

    await app.schedules.create(
        check_price, every="1s", ttl="2h", kwargs={"symbol": "X"},
    )
"""

from syntasks.scheduler.info import ScheduleInfo
from syntasks.scheduler.manager import (
    ScheduleConflictError,
    ScheduleError,
    ScheduleManager,
    ScheduleNotFoundError,
    StaticScheduleError,
)
from syntasks.scheduler.recurrence import (
    At,
    Cron,
    Every,
    Recurrence,
    ScheduleDecl,
    after,
    at,
    cron,
    every,
)
from syntasks.scheduler.schedule import Schedule, ScheduleSpec

__all__ = [
    "At",
    "Cron",
    "Every",
    "Recurrence",
    "Schedule",
    "ScheduleConflictError",
    "ScheduleDecl",
    "ScheduleError",
    "ScheduleInfo",
    "ScheduleManager",
    "ScheduleNotFoundError",
    "ScheduleSpec",
    "StaticScheduleError",
    "after",
    "at",
    "cron",
    "every",
]
