"""
Runtime schedules: copies of a template schedule, created by your app's users.

A schedule declared with ``runtime=...`` is a *template*. At runtime the app
creates copies of it, each with its own timing, arguments, owner, tags and a
little metadata, stored in Redis. They replace a database table plus CRUD for
"post this at 9:00", "send this campaign on Monday"::

    @app.schedule(runtime=Runtime(max_per_owner=500, max_ttl="400d"),
                  timeout="30s", retries=3, max_tasks=200)
    async def publish_post(account_id: str, text: str) -> None: ...

    await publish_post.schedule(at="2026-12-01T09:00+06:00",
                                kwargs={"account_id": "a", "text": "Hello"},
                                owner="user:42", tags=["campaign:spring"])
    page = await publish_post.schedules.list(owner="user:42")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from syntasks.units import parse_duration_ms, parse_size

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.scheduler.manager import ScheduleManager
    from syntasks.scheduler.schedule import Schedule

__all__ = ["Page", "Runtime", "TemplateSchedules"]


@dataclass(frozen=True)
class Runtime:
    """
    Limits of the runtime copies of a template schedule.

    :param max_per_owner: copies one owner may have at once (None: no cap).
    :param max_ttl: longest lifetime of a copy; copies without ``ttl`` get it.
    :param min_interval: shortest recurrence a copy may use.
    :param max_payload: max size of a copy's arguments (default
        ``[scheduler] max_payload``).
    :param keep_finished: how long a finished one-shot copy is kept (so a UI
        can show "sent at 09:00") before it is deleted.
    """

    max_per_owner: int | None = 1000
    max_ttl: Any = None
    min_interval: Any = None
    max_payload: Any = None
    keep_finished: Any = "7d"
    max_ttl_ms: int | None = field(init=False, repr=False)
    min_interval_ms: int | None = field(init=False, repr=False)
    max_payload_bytes: int | None = field(init=False, repr=False)
    keep_finished_ms: int = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.max_per_owner is not None and self.max_per_owner < 1:
            raise ValueError("max_per_owner must be at least 1.")

        def ms(value: Any) -> int | None:
            return None if value is None else parse_duration_ms(value)

        object.__setattr__(self, "max_ttl_ms", ms(self.max_ttl))
        object.__setattr__(self, "min_interval_ms", ms(self.min_interval))
        object.__setattr__(
            self,
            "max_payload_bytes",
            None if self.max_payload is None else parse_size(self.max_payload),
        )
        object.__setattr__(self, "keep_finished_ms", ms(self.keep_finished) or 0)


@dataclass(frozen=True)
class Page:
    """
    One page of schedules.

    :param items: schedules, in creation order.
    :param cursor: pass it back for the next page; None on the last page.
    """

    items: list[Schedule]
    cursor: str | None


class TemplateSchedules:
    """The runtime copies of one template (``task.schedules``)."""

    def __init__(self, manager: ScheduleManager, template: str) -> None:
        self.manager = manager
        self.template = template

    async def list(
        self,
        *,
        owner: str | None = None,
        tag: str | None = None,
        state: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> Page:
        """
        Copies of this template, oldest first, one page at a time.

        :param owner: only this owner's.
        :param tag: only those with this tag.
        :param state: ``active``, ``paused``, ``done``, ``orphaned``, ...
        :param limit: page size.
        :param cursor: from the previous page.
        :return: page.
        """
        return await self.manager.find(
            template=self.template,
            owner=owner,
            tag=tag,
            state=state,
            limit=limit,
            cursor=cursor,
        )

    async def get(self, schedule_id: str) -> Schedule:
        """
        Read one copy.

        :param schedule_id: id.
        :return: schedule.
        """
        return await self.manager.get(schedule_id)

    async def update(self, schedule_id: str, **changes: Any) -> Schedule:
        """
        Change a copy (same arguments as creating one, plus ``version``).

        :param schedule_id: id.
        :param changes: new values.
        :return: updated schedule.
        """
        return await self.manager.update(schedule_id, **changes)

    async def cancel(self, schedule_id: str) -> None:
        """
        Delete a copy.

        :param schedule_id: id.
        """
        await self.manager.cancel(schedule_id)

    async def cancel_all(
        self, *, owner: str | None = None, tag: str | None = None
    ) -> int:
        """
        Delete this template's copies (all, or an owner's, or a tag's).

        :param owner: only this owner's.
        :param tag: only those with this tag.
        :return: copies deleted.
        """
        return await self.manager.cancel_all(
            template=self.template, owner=owner, tag=tag
        )
