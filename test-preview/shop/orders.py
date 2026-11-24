"""Orders: payments, shipping, a partner API, reports. Retries and limits."""

import asyncio
import logging
import time
from typing import Annotated, Any

from syntasks import Context, Depends, Emit, Module, TaskOutcome
from syntasks.depends.progress_tracker import ProgressTracker, TaskState

module = Module("orders", timeout="30s", retries=3, retry_delay="200ms", max_tasks=20)
logger = logging.getLogger("shop.orders")


def node_id() -> str:
    """The worker process running this task (the web page shows it)."""
    from .app import app  # noqa: PLC0415

    return app.cluster.node_id


@module.task(retries=0)
async def whoami(pause: float = 0.05) -> str:
    """Return the worker process that ran it; many of them show the spread."""
    await asyncio.sleep(pause)
    return node_id()


# Callback: runs once after every successful charge.
@module.task(retries=0)
async def audit(outcome: TaskOutcome) -> None:
    logger.info("audit: %s charged %s", outcome.task_id[:8], outcome.return_value)


# Retries: order ids ending in 7 fail once (a flaky payment provider).
@module.task(on_success=audit)
async def charge(
    order_id: str, cents: int, context: Annotated[Context, Depends()]
) -> dict[str, Any]:
    attempt = int(context.message.labels.get("_attempt", 0)) + 1
    if order_id.endswith("7") and attempt == 1:
        raise ConnectionError("payment provider timed out")
    return {
        "order": order_id,
        "cents": cents,
        "attempt": attempt,
        "node": node_id(),
    }


# Order per key: one customer's shipments one at a time, in order.
@module.task(key="kwargs.customer", max_tasks_per_key=1)
async def ship(customer: str, order_id: str) -> dict[str, Any]:
    started = time.time()
    await asyncio.sleep(0.3)
    return {
        "customer": customer,
        "order": order_id,
        "started": started,
        "ended": time.time(),
        "node": node_id(),
    }


# Limits across all machines: 3 at once, 10 a second.
@module.task(max_tasks=3, rate="10/s")
async def call_partner(n: int) -> dict[str, Any]:
    started = time.time()
    await asyncio.sleep(0.25)
    return {"n": n, "started": started, "ended": time.time(), "node": node_id()}


# Request / reply.
@module.task(timeout="2s", retries=0)
async def price(sku: str) -> int:
    prices = {"KB-01": 4900, "MS-02": 2900}
    if sku not in prices:
        raise KeyError(f"unknown sku {sku}")
    return prices[sku]


# Time limits: cancelled after 1s, 1s to clean up, then TaskTimeoutError.
@module.task(timeout="1s", grace="1s", retries=0)
async def export_orders() -> None:
    try:
        await asyncio.sleep(60)
    finally:
        logger.info("export: cleaned up")


# Process isolation with a memory limit.
@module.task(isolation="process", memory_limit="200MiB", timeout="20s", retries=0)
def resize_image(megabytes: int) -> int:
    buffer = bytearray(megabytes * 1024 * 1024)
    return len(buffer) // (1024 * 1024)


# Live events, progress, streamed output and captured logs.
@module.task(events=True, capture_logs=True, retries=0)
async def import_catalog(
    rows: int,
    progress: Annotated[ProgressTracker[str], Depends()],
    emit: Annotated[Emit, Depends()],
) -> int:
    for done in range(10, rows + 1, 10):
        await asyncio.sleep(0.1)
        logger.info("imported %d/%d rows", done, rows)
        await progress.set_progress(TaskState.STARTED, meta=f"{done}/{rows}")
        await emit({"sku": f"SKU-{done:03d}"})
    return rows
