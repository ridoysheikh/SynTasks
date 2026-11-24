"""
Scheduling inside the library, stored in Redis.

Declare schedules next to tasks::

    from syntasks import SynTasks, calendar, cron, every

    app = SynTasks()

    @app.schedule(every("10s"))
    async def poll() -> None: ...

    @app.schedule(calendar(weekday="mon-fri", at="09:30", tz="Europe/London"))
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
    ScheduleLimitError,
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
    calendar,
    cron,
    every,
)
from syntasks.scheduler.runtime import Page, Runtime, TemplateSchedules
from syntasks.scheduler.schedule import Schedule, ScheduleSpec

__all__ = [
    "At",
    "Cron",
    "Every",
    "Page",
    "Recurrence",
    "Runtime",
    "Schedule",
    "ScheduleConflictError",
    "ScheduleDecl",
    "ScheduleError",
    "ScheduleInfo",
    "ScheduleLimitError",
    "ScheduleManager",
    "ScheduleNotFoundError",
    "ScheduleSpec",
    "StaticScheduleError",
    "TemplateSchedules",
    "after",
    "at",
    "calendar",
    "cron",
    "every",
]
