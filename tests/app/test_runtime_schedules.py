import asyncio
import time
from typing import Any

import pytest

from syntasks import Runtime, ScheduleError, ScheduleLimitError, calendar
from syntasks.scheduler import ScheduleNotFoundError

from .conftest import AppFactory, running, wait_until


def fast(config: Any) -> None:
    config.scheduler.min_interval = 0.05
    config.scheduler.claim_lease = 0.5


def declare(app: Any, **runtime: Any) -> Any:
    limits = {
        "max_per_owner": 3,
        "max_ttl": "1h",
        "min_interval": "1s",
        "max_payload": "1KiB",
        **runtime,
    }

    @app.schedule(runtime=Runtime(**limits))
    async def post(account: str, text: str) -> str:
        return f"{account}: {text}"

    return post


def soon(seconds: float = 3600) -> float:
    return time.time() + seconds


async def test_runtime_requires_flag(app_factory: AppFactory) -> None:
    app = app_factory(fast)

    @app.task
    async def plain() -> None: ...

    with pytest.raises(ScheduleError, match="runtime"):
        await plain.schedule(after="1s")  # type: ignore[attr-defined]
    with pytest.raises(ScheduleError, match="runtime"):
        await app.schedules.create(plain, after="1s")


async def test_template_without_timing(app_factory: AppFactory) -> None:
    app = app_factory(fast)
    post = declare(app)
    assert app.schedules.static_specs() == {}
    created = await post.schedule(
        at=soon(),
        kwargs={"account": "acc-1", "text": "hi"},
        owner="user:1",
        tags=["campaign:spring"],
        meta={"title": "Launch"},
    )
    assert created.source == "api"
    assert created.template == post.task_name
    assert (created.owner, created.tags, created.meta) == (
        "user:1",
        ["campaign:spring"],
        {"title": "Launch"},
    )


async def test_owner_cap(app_factory: AppFactory) -> None:
    app = app_factory(fast)
    post = declare(app, max_per_owner=2)
    first = await post.schedule(
        at=soon(), kwargs={"account": "a", "text": "1"}, owner="user:1"
    )
    await post.schedule(at=soon(), kwargs={"account": "a", "text": "2"}, owner="user:1")
    with pytest.raises(ScheduleLimitError, match="user:1"):
        await post.schedule(
            at=soon(), kwargs={"account": "a", "text": "3"}, owner="user:1"
        )
    assert (await app.schedules.get(first.id)).state == "active"
    page = await post.schedules.list(owner="user:1")
    assert len(page.items) == 2
    await post.schedule(at=soon(), kwargs={"account": "b", "text": "1"}, owner="user:2")
    await app.schedules.cancel(first.id)
    await post.schedule(at=soon(), kwargs={"account": "a", "text": "4"}, owner="user:1")


async def test_list_by_owner_tag_paging(app_factory: AppFactory) -> None:
    app = app_factory(fast)
    post = declare(app, max_per_owner=10)
    mine = []
    for n in range(5):
        s = await post.schedule(
            at=soon(n + 1),
            kwargs={"account": "a", "text": str(n)},
            owner="user:1",
            tags=["even"] if n % 2 == 0 else ["odd"],
        )
        mine.append(s.id)
    for n in range(2):
        await post.schedule(
            at=soon(), kwargs={"account": "b", "text": str(n)}, owner="user:2"
        )

    seen: list[str] = []
    cursor = None
    sizes = []
    while True:
        page = await post.schedules.list(owner="user:1", limit=2, cursor=cursor)
        sizes.append(len(page.items))
        seen.extend(s.id for s in page.items)
        cursor = page.cursor
        if cursor is None:
            break
    assert sizes == [2, 2, 1]
    assert seen == mine  # creation order, no duplicates
    evens = await app.schedules.find(tag="even")
    assert sorted(s.id for s in evens.items) == sorted(mine[0::2])
    both = await app.schedules.find(owner="user:1", tag="odd")
    assert sorted(s.id for s in both.items) == sorted(mine[1::2])
    assert len((await app.schedules.find(template=post.task_name, limit=50)).items) == 7


async def test_bulk_pause_cancel_by_tag(app_factory: AppFactory) -> None:
    app = app_factory(fast)
    post = declare(app, max_per_owner=10)
    tagged = [
        (
            await post.schedule(
                at=soon(), kwargs={"account": "a", "text": str(n)}, tags=["camp"]
            )
        ).id
        for n in range(3)
    ]
    other = await post.schedule(
        at=soon(), kwargs={"account": "a", "text": "x"}, owner="user:9"
    )
    assert await app.schedules.pause_all(tag="camp") == 3
    assert {(await app.schedules.get(i)).state for i in tagged} == {"paused"}
    assert (await app.schedules.get(other.id)).state == "active"
    assert await app.schedules.resume_all(tag="camp") == 3
    assert {(await app.schedules.get(i)).state for i in tagged} == {"active"}
    assert await app.schedules.cancel_all(tag="camp") == 3
    for schedule_id in tagged:
        with pytest.raises(ScheduleNotFoundError):
            await app.schedules.get(schedule_id)
    assert (await app.schedules.find(tag="camp")).items == []
    assert await app.schedules.cancel_all(owner="user:9") == 1
    with pytest.raises(ScheduleError, match="owner, tag or template"):
        await app.schedules.cancel_all()


async def test_keep_finished_one_shot(app_factory: AppFactory) -> None:
    app = app_factory(fast)
    post = declare(app, keep_finished="300ms")
    created = await post.schedule(after="100ms", kwargs={"account": "a", "text": "go"})

    async def is_finished() -> bool:
        return await _state(app, created.id) == "done"

    async with running(app):
        await wait_until(is_finished, timeout=10)
        await wait_until(lambda: _last_status(app, created.id), timeout=10)
    assert await app.schedules.expire() == 0  # kept for a while, to show "sent"
    finished = await app.schedules.get(created.id)
    assert finished.last_status == "ok"
    await asyncio.sleep(0.5)
    assert await app.schedules.expire() == 1
    with pytest.raises(ScheduleNotFoundError):
        await app.schedules.get(created.id)


async def _last_status(app: Any, schedule_id: str) -> str | None:
    return (await app.schedules.get(schedule_id)).last_status


async def _state(app: Any, schedule_id: str) -> str | None:
    try:
        return (await app.schedules.get(schedule_id)).state
    except ScheduleNotFoundError:
        return None


async def test_orphaned_when_template_removed(app_factory: AppFactory) -> None:
    def revision(number: int) -> Any:
        def configure(config: Any) -> None:
            fast(config)
            config.app.revision = number

        return configure

    old = app_factory(revision(1))
    post = declare(old)
    created = await post.schedule(at=soon(), kwargs={"account": "a", "text": "x"})
    assert await old.schedules.sync_static() == "synced"

    without = app_factory(revision(2))  # the template was removed from code
    assert await without.schedules.sync_static() == "synced"
    orphan = await without.schedules.get(created.id)
    assert orphan.state == "orphaned"
    due = await without.redis.zrange_withscores(without.schedules.due_key(0), 0, -1)
    assert created.id not in {raw.decode() for raw, _ in due}

    back = app_factory(revision(3))
    declare(back)
    assert await back.schedules.sync_static() == "synced"
    assert (await back.schedules.get(created.id)).state == "active"


async def test_runtime_limits_enforced(app_factory: AppFactory) -> None:
    app = app_factory(fast)
    post = declare(app)
    args = {"account": "a", "text": "x"}
    with pytest.raises(ScheduleLimitError, match="min_interval"):
        await post.schedule(every="100ms", kwargs=args)
    with pytest.raises(ScheduleLimitError, match="max_ttl"):
        await post.schedule(every="1m", kwargs=args, ttl="2h")
    with pytest.raises(ScheduleError, match=r"1 KiB|1024"):
        await post.schedule(at=soon(), kwargs={"account": "a", "text": "x" * 2000})
    defaulted = await post.schedule(every="1m", kwargs=args)
    assert defaulted.expires_ms is not None
    assert defaulted.expires_ms - time.time() * 1000 == pytest.approx(
        3_600_000, abs=5_000
    )


async def test_copy_timezone_and_calendar(app_factory: AppFactory) -> None:
    app = app_factory(fast)
    post = declare(app)
    created = await post.schedule(
        calendar(weekday="mon", nth=1, at="09:00", tz="Asia/Dhaka"),
        kwargs={"account": "a", "text": "monthly"},
    )
    assert created.describe() == "first Monday at 09:00 (Asia/Dhaka)"
    cron_copy = await post.schedule(
        cron="0 9 * * *", tz="Asia/Dhaka", kwargs={"account": "a", "text": "d"}
    )
    assert "Asia/Dhaka" in cron_copy.describe()


async def test_update_with_version(app_factory: AppFactory) -> None:
    app = app_factory(fast)
    post = declare(app)
    created = await post.schedule(
        at=soon(), kwargs={"account": "a", "text": "draft"}, owner="u"
    )
    changed = await post.schedules.update(
        created.id,
        version=created.version,
        kwargs={"account": "a", "text": "final"},
        meta={"title": "Edited"},
    )
    assert changed.spec.kwargs["text"] == "final"
    assert changed.meta == {"title": "Edited"}
    assert changed.owner == "u"
    with pytest.raises(ScheduleError, match="changed"):
        await post.schedules.update(
            created.id, version=created.version, kwargs={"account": "a", "text": "x"}
        )
