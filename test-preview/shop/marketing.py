"""Marketing: schedules, posts users schedule themselves, campaigns, fairness."""

import asyncio
import logging
from typing import Annotated, Any

from syntasks import (
    BatchOutcome,
    Depends,
    Module,
    Runtime,
    ScheduleInfo,
    calendar,
    every,
)

from .orders import node_id

module = Module("marketing", timeout="30s", retries=2, max_tasks=50)
logger = logging.getLogger("shop.marketing")


# Declared schedules: reconciled into Redis on start, one run per tick cluster-wide.
@module.schedule(every("2s"), id="heartbeat", retries=0, max_tasks=1)
async def heartbeat(info: Annotated[ScheduleInfo, Depends()]) -> dict[str, Any]:
    logger.info("heartbeat #%d (late by %.2fs)", info.run_number, info.late_by)
    return {"run": info.run_number, "node": node_id()}


@module.schedule(
    calendar(weekday="mon", nth=1, at="09:00", tz="Asia/Dhaka"), max_tasks=1
)
async def monthly_report() -> str:
    return "report.pdf"


# A runtime template: users schedule their own posts; no table needed.
@module.schedule(
    runtime=Runtime(max_per_owner=3, max_ttl="30d", min_interval="1m"),
    key="kwargs.account",
    rate_per_key="2/s",
)
async def publish_post(account: str, text: str) -> str:
    logger.info("posted to %s: %s", account, text)
    return text


# Fair share: one tenant's big send can't starve the others.
@module.task(fair=True, key="kwargs.tenant", max_tasks_per_node=1)
async def send_email(tenant: str, to: str) -> str:
    return f"{tenant} -> {to}"


# Debounce: a burst of edits to one product reindexes it once.
@module.task(debounce="500ms", key="kwargs.sku")
async def reindex(sku: str, version: int) -> int:
    logger.info("reindexed %s at version %d", sku, version)
    return version


# Campaign members and the summary that runs once at the end.
@module.task(max_tasks_per_node=1)
async def campaign_email(to: str) -> str:
    await asyncio.sleep(0.1)  # pretend to talk to the mail provider
    return to


@module.task
async def campaign_report(outcome: BatchOutcome) -> None:
    logger.info(
        "campaign %s: %d sent, %d cancelled",
        outcome.batch_id,
        outcome.done,
        outcome.cancelled,
    )
