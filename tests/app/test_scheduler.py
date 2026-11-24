import asyncio
from typing import Annotated, Any

import pytest

from syntasks import (
    Depends,
    Module,
    Runtime,
    ScheduleInfo,
    SynTasks,
    after,
    at,
    calendar,
    cron,
    every,
)
from syntasks.config import Config, ConfigError
from syntasks.options import MissingLimitError
from syntasks.scheduler import (
    ScheduleConflictError,
    ScheduleError,
    ScheduleNotFoundError,
    StaticScheduleError,
)
from syntasks.scheduler import scripts as sched_scripts
from syntasks.scheduler.recurrence import Every
from syntasks.scheduler.schedule import ScheduleSpec

from .conftest import AppFactory, running, wait_until


def fast(config: Config) -> None:
    config.scheduler.min_interval = 0.05
    config.scheduler.claim_lease = 0.5


def test_recurrences() -> None:
    every_ = Every(1000, anchor_ms=500)
    assert every_.next_after(0) == 500
    assert every_.next_after(500) == 1500
    decl = every("2s", id="x", kwargs={"a": 1}, misfire="skip")
    assert decl.id == "x"
    assert decl.options == {"misfire": "skip"}
    assert cron("*/5 * * * * *", tz="Europe/London").recurrence is not None
    assert at("2030-01-01T00:00:00Z").recurrence.when_ms == 1893456000000  # type: ignore[union-attr]
    assert after("10s").after_ms == 10_000
    with pytest.raises(ValueError, match="time zone"):
        cron("* * * * *", tz="Mars/Base")


async def test_misfire_plan(app_factory: AppFactory) -> None:
    def late_after_half_second(config: Config) -> None:
        config.scheduler.late_after = 0.5

    app = app_factory(late_after_half_second)
    plan = app.schedules._plan

    def spec(**options: Any) -> ScheduleSpec:
        return ScheduleSpec(id="s", task="t", recurrences=[Every(1000)], **options)

    # On time (within late_after): an ordinary fire.
    assert plan(spec(), 10_000, 10_200) == ([10_000], 0, 11_000)
    # Missed fires 10s..13s at 13.5s.
    assert plan(spec(misfire="skip"), 10_000, 13_500) == ([], 4, 14_000)
    assert plan(spec(misfire="once"), 10_000, 13_500) == ([10_000], 3, 14_000)
    assert plan(spec(misfire="all", max_catchup=2), 10_000, 13_500) == (
        [10_000, 11_000],
        2,
        14_000,
    )
    # misfire_grace drops missed fires older than it, whatever the policy.
    assert plan(spec(misfire="once", misfire_grace_ms=2_000), 10_000, 13_500) == (
        [12_000],
        3,
        14_000,
    )
    assert plan(spec(misfire="all", misfire_grace_ms=2_000), 10_000, 13_500) == (
        [12_000, 13_000],
        2,
        14_000,
    )
    assert plan(spec(misfire="once", misfire_grace_ms=100), 10_000, 13_500) == (
        [],
        4,
        14_000,
    )


async def test_static_schedule_runs_with_info(app_factory: AppFactory) -> None:
    app = app_factory(fast)
    seen: list[ScheduleInfo] = []

    @app.schedule(every("200ms"), id="tick", kwargs={"label": "x"})
    async def tick(label: str, info: ScheduleInfo = Depends()) -> None:
        assert label == "x"
        seen.append(info)

    async with running(app):
        await wait_until(lambda: len(seen) >= 3, timeout=10)
    assert all(i.schedule_id == "tick" for i in seen)
    numbers = [i.run_number for i in seen]
    assert numbers == sorted(numbers)
    schedule = await app.schedules.get("tick")
    assert schedule.source == "code"
    assert schedule.runs >= 3
    runs = await app.schedules.runs("tick")
    assert runs[0]["status"] == "ok"


async def test_static_sync_prune_and_revision(app_factory: AppFactory) -> None:
    def revision(number: int) -> Any:
        def configure(config: Config) -> None:
            config.app.revision = number

        return configure

    app = app_factory(revision(2))

    async def report() -> None: ...

    app.schedule(
        [cron("0 3 * * *", id="early"), cron("0 2 * * *", id="nightly")],
        task_name="m:report",
    )(report)
    assert await app.schedules.sync_static() == "synced"
    assert await app.schedules.sync_static() == "same"
    ids = sorted([s.id async for s in app.schedules.list()])
    assert ids == ["early", "nightly"]
    await app.schedules.pause("nightly")

    newer = app_factory(revision(3))
    newer.schedule(cron("0 2 * * *", id="nightly", kwargs={}), task_name="m:report")(
        report,
    )
    assert await newer.schedules.sync_static() == "synced"
    assert [s.id async for s in newer.schedules.list()] == ["nightly"]  # "early" pruned
    assert (await newer.schedules.get("nightly")).state == "paused"  # state kept

    stale = app_factory(revision(1))
    stale.schedule(every(60, id="legacy"), task_name="m:report")(report)
    assert await stale.schedules.sync_static() == "stale"
    with pytest.raises(ScheduleNotFoundError):
        await stale.schedules.get("legacy")
    with pytest.raises(StaticScheduleError):
        await newer.schedules.delete("nightly")


async def test_schedule_decorator_creates_task_and_schedule(
    app_factory: AppFactory,
) -> None:
    app = app_factory(fast)

    @app.schedule(
        every("1s"), misfire="all", overlap="allow", timeout="5s", max_tasks=1
    )
    async def beat() -> int:
        return 1

    assert app.find_task(beat.task_name) is beat
    specs = app.schedules.static_specs()
    spec, source, _ = specs[beat.task_name]  # one timing: the id is the task name
    assert (source, spec.misfire, spec.overlap) == ("code", "all", "allow")
    options = app.options_for(beat.task_name)
    assert (options.timeout, options.max_tasks) == (5, 1)


async def test_schedule_list_of_timings(app_factory: AppFactory) -> None:
    app = app_factory(fast)

    @app.schedule([every("1s"), calendar(weekday="mon", at="09:00")])
    async def twice() -> None: ...

    assert sorted(app.schedules.static_specs()) == [
        f"{twice.task_name}:0",
        f"{twice.task_name}:1",
    ]


async def test_schedule_requires_limits(redis_url: str) -> None:
    config = Config()
    config.redis.url = redis_url
    app = SynTasks(config, search_config=False, retries=0, max_tasks=None)
    with pytest.raises(MissingLimitError, match="timeout"):

        @app.schedule(every("1s"))
        async def job() -> None: ...

    await app.redis.close()


async def test_schedule_needs_timing(app_factory: AppFactory) -> None:
    app = app_factory()
    with pytest.raises(ConfigError, match="timing"):

        @app.schedule()
        async def job() -> None: ...


async def test_task_schedule_kwarg_removed(app_factory: AppFactory) -> None:
    app = app_factory()
    with pytest.raises(ConfigError, match="schedule"):

        @app.task(schedule=every("1s"))
        async def job() -> None: ...


async def test_schedule_on_module(app_factory: AppFactory) -> None:
    app = app_factory(fast)
    reports = Module("reports", timeout="1m")

    @reports.schedule(calendar(day=1, at="06:00", tz="Asia/Dhaka"))
    async def monthly() -> None: ...

    app.extends(reports)
    spec, _, _ = app.schedules.static_specs()[monthly.task_name]
    assert spec.describe() == "day 1 of the month at 06:00 (Asia/Dhaka)"
    assert len(spec.next_times(3)) == 3
    assert app.options_for(monthly.task_name).timeout == 60


async def test_missed_fires_once_after_downtime(app_factory: AppFactory) -> None:
    app = app_factory(fast)
    runs: list[ScheduleInfo] = []

    @app.schedule(every("1s"), id="beat")
    async def beat(info: Annotated[ScheduleInfo, Depends()]) -> None:
        runs.append(info)

    assert await app.schedules.sync_static() == "synced"
    await asyncio.sleep(3.5)  # no worker: fires are missed
    async with running(app):
        await wait_until(lambda: len(runs) >= 1, timeout=10)
        await asyncio.sleep(0.3)
    first = runs[0]
    assert first.late_by >= 2.5  # the oldest missed fire, run once
    assert sum(1 for info in runs if info.late_by >= 1.5) == 1
    history = await app.schedules.runs("beat")
    assert max(entry["late_by"] for entry in history) >= 2.5


async def test_one_shot_fires_after_downtime(app_factory: AppFactory) -> None:
    app = app_factory(fast)
    done: list[ScheduleInfo] = []

    @app.schedule(runtime=True)
    async def remind(info: Annotated[ScheduleInfo, Depends()]) -> None:
        done.append(info)

    await app.schedules.create(remind, after="500ms", id="once")
    await asyncio.sleep(2.0)  # no worker when it was due
    async with running(app):
        await wait_until(lambda: len(done) == 1, timeout=10)
    assert done[0].late_by >= 1.4
    assert done[0].scheduled_for == done[0].fire_time


async def test_dynamic_crud(app_factory: AppFactory) -> None:
    app = app_factory(fast)

    @app.schedule(runtime=True)
    async def check(symbol: str, above: float) -> None: ...

    created = await app.schedules.create(
        check,
        kwargs={"symbol": "AAPL", "above": 250},
        every="1s",
        id="alert",
        ttl="1h",
    )
    assert created.state == "active"
    assert created.source == "api"
    assert created.expires_ms is not None
    with pytest.raises(ScheduleError, match="already exists"):
        await app.schedules.create(
            check,
            kwargs={"symbol": "A", "above": 1},
            every="1s",
            id="alert",
        )
    same = await app.schedules.create(
        check,
        kwargs={"symbol": "A", "above": 1},
        every="1s",
        id="alert",
        if_exists="ignore",
    )
    assert same.spec.kwargs["symbol"] == "AAPL"
    updated = await app.schedules.update("alert", every="2s", version=created.version)
    assert updated.describe() == "every 2s"
    with pytest.raises(ScheduleConflictError):
        await app.schedules.update("alert", every="3s", version=created.version)
    assert (await app.schedules.pause("alert")).state == "paused"
    assert (await app.schedules.resume("alert")).state == "active"
    listed = [s.id async for s in app.schedules.list(task=check.task_name)]
    assert listed == ["alert"]
    await app.schedules.delete("alert")
    with pytest.raises(ScheduleNotFoundError):
        await app.schedules.get("alert")


async def test_create_validation(app_factory: AppFactory) -> None:
    def config(c: Config) -> None:
        c.scheduler.max_dynamic = 1
        c.scheduler.max_payload = 100

    app = app_factory(config)

    @app.schedule(runtime=True)
    async def job(count: int) -> None: ...

    with pytest.raises(ScheduleError, match="argument 'count'"):
        await app.schedules.create(job, kwargs={"count": "not a number"}, every="5s")
    with pytest.raises(ScheduleError, match="don't match"):
        await app.schedules.create(job, kwargs={"wrong": 1}, every="5s")
    with pytest.raises(ScheduleError, match="minimum"):
        await app.schedules.create(job, args=[1], every="100ms")
    with pytest.raises(ScheduleError, match="bytes"):
        await app.schedules.create(job, args=[1], every="5s", labels={"x": "y" * 200})
    await app.schedules.create(job, args=[1], every="5s")
    with pytest.raises(ScheduleError, match="Too many"):
        await app.schedules.create(job, args=[2], every="5s")


async def test_max_runs_and_one_shot(app_factory: AppFactory) -> None:
    app = app_factory(fast)
    runs: list[str] = []

    @app.schedule(runtime=Runtime(keep_finished="0s"))
    async def job(tag: str) -> None:
        runs.append(tag)

    async with running(app):
        await job.schedule(every="100ms", args=["limited"], max_runs=2, id="two")
        await app.schedules.create(job, args=["once"], after="200ms", id="once")
        await wait_until(lambda: runs.count("limited") == 2 and "once" in runs)
        await asyncio.sleep(0.5)
    assert runs.count("limited") == 2
    assert runs.count("once") == 1
    assert (await app.schedules.get("two")).state == "done"
    assert await app.schedules.expire() == 2
    assert [s async for s in app.schedules.list()] == []


async def test_ttl_expiry(app_factory: AppFactory) -> None:
    app = app_factory(fast)

    @app.schedule(runtime=True)
    async def job() -> None: ...

    await app.schedules.create(job, every="1s", ttl="100ms", id="short")
    await asyncio.sleep(0.2)
    assert await app.schedules.expire() == 1
    with pytest.raises(ScheduleNotFoundError):
        await app.schedules.get("short")


async def test_two_workers_never_double_fire(app_factory: AppFactory) -> None:
    fires: list[int] = []

    def build() -> SynTasks:
        app = app_factory(fast)

        @app.schedule(every("100ms"), task_name="beat", id="beat")
        async def beat(info: ScheduleInfo = Depends()) -> None:
            assert info.fire_time is not None
            fires.append(int(info.fire_time.timestamp() * 1000))

        return app

    first, second = build(), build()
    async with running(first), running(second):
        await asyncio.sleep(2.0)
    assert len(fires) >= 10
    assert len(fires) == len(set(fires))


async def test_claimed_but_not_committed_fire_is_recovered(
    app_factory: AppFactory,
) -> None:
    app = app_factory(fast)
    task_ids: list[str] = []

    @app.schedule(runtime=True, task_name="crashy")
    async def crashy(info: ScheduleInfo = Depends()) -> None:
        task_ids.append(str(info.fire_time))

    schedule = await app.schedules.create(
        crashy,
        at=app.cluster.clock.now() + 0.1,
        id="c",
    )
    await asyncio.sleep(0.2)
    # A dispatcher claims the fire and dies before committing.
    reply = await sched_scripts.CLAIM(
        app.redis,
        [app.schedules.due_key(0)],
        [10, 500, app.schedules.def_key(""), "dead-token"],
    )
    assert reply[0].decode() == schedule.id
    async with running(app):
        await wait_until(lambda: task_ids, timeout=10)
        await asyncio.sleep(0.6)
    assert len(task_ids) == 1


async def test_overlap_skip(app_factory: AppFactory) -> None:
    app = app_factory(fast)
    active = 0
    peak = 0
    done = 0

    @app.schedule(every("100ms"), id="slow", overlap="skip")
    async def slow() -> None:
        nonlocal active, peak, done
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.35)
        active -= 1
        done += 1

    async with running(app):
        await wait_until(lambda: done >= 3, timeout=10)
    assert peak == 1


async def test_trigger_and_failure_pause(app_factory: AppFactory) -> None:
    app = app_factory(fast)

    @app.schedule(runtime=True)
    async def broken() -> None:
        raise RuntimeError("bad")

    async with running(app):
        await app.schedules.create(broken, every="5s", id="b", pause_after_failures=2)
        first = await app.schedules.trigger("b")
        await first.wait_result(timeout=10)
        second = await app.schedules.trigger("b")
        await second.wait_result(timeout=10)
        await wait_until(lambda: _state(app, "b", "paused"))
    runs = await app.schedules.runs("b")
    assert [r["status"] for r in runs] == ["error", "error"]


async def _state(app: SynTasks, schedule_id: str, state: str) -> bool:
    return (await app.schedules.get(schedule_id)).state == state


async def test_invalid_schedule_marked(app_factory: AppFactory) -> None:
    app = app_factory(fast)

    @app.schedule(runtime=True, max_payload=10)
    async def job(text: str) -> None: ...

    await app.schedules.create(job, args=["x" * 50], after="50ms", id="big")
    await asyncio.sleep(0.1)
    await app.schedules.dispatch(None, 0)
    schedule = await app.schedules.get("big")
    assert schedule.state == "invalid"
    assert "max_payload" in (schedule.invalid_reason or "")


async def test_schedule_args_skip_annotated_dependencies(
    app_factory: AppFactory,
) -> None:
    app = app_factory(fast)

    @app.schedule(every("1s"), kwargs={"x": 1}, id="annotated")
    async def job(x: int, info: Annotated[ScheduleInfo, Depends()]) -> None: ...

    app.schedules.validate_declared()
