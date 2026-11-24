"""
The shop's web server: FastAPI in front of the same app the workers run.

    uvicorn shop.web:api --port 8001
    WEB_NAME=web2 uvicorn shop.web:api --port 8002      # a second web server

A web server never runs tasks. It sends them with `.kiq()` / `.call()`, reads
results and events, schedules runtime copies, and keeps the sockets of users;
the workers (`syntasks worker`, one or several machines) do the work. Run two
web servers and two workers and use any of them: it is all one app on one
Redis. Open http://127.0.0.1:8001/ for a page that drives every endpoint.
"""

import asyncio
import dataclasses
import datetime as dt
import json
import os
import time
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response, WebSocket
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field
from starlette.websockets import WebSocketDisconnect

from syntasks import (
    ChannelDenied,
    ScheduleError,
    ScheduleLimitError,
    SynTasksResultTimeoutError,
    TaskTimeoutError,
)

from .app import app
from .marketing import campaign_email, campaign_report, publish_post, reindex
from .orders import (
    call_partner,
    charge,
    import_catalog,
    price,
    ship,
    whoami,
)

NAME = os.environ.get("WEB_NAME") or f"web-{os.getpid()}"
HERE = Path(__file__).resolve().parent


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
    await app.startup()
    yield
    await app.shutdown()


api = FastAPI(title=f"SynTasks shop ({NAME})", lifespan=lifespan)


@api.middleware("http")
async def served_by(request: Request, call_next: Any) -> Response:
    """Tell the caller which web server answered."""
    response: Response = await call_next(request)
    response.headers["X-Served-By"] = NAME
    return response


@api.get("/", include_in_schema=False)
async def index() -> FileResponse:
    return FileResponse(HERE / "index.html")


# -- helpers -----------------------------------------------------------------


async def machines() -> dict[str, str]:
    """Map every live worker process to its machine: ``machine 1``, ``machine 2``."""
    nodes = await app.nodes()
    started = {
        n.group: min(m.started for m in nodes if m.group == n.group) for n in nodes
    }
    label = {
        g: f"machine {i}" for i, g in enumerate(sorted(started, key=started.get), 1)
    }  # type: ignore[arg-type]
    return {n.node_id: label[n.group] for n in nodes}


def iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.UTC).isoformat(timespec="seconds")


def node_of(value: Any) -> str | None:
    """The worker process a task result says it ran on, if it says."""
    if isinstance(value, dict):
        return value.get("node")
    return value if isinstance(value, str) else None


async def outcome(task_id: str) -> dict[str, Any]:
    """State of a task: running (with progress), or its value or error, and where it ran."""
    task = app.tasks.handle(task_id)
    if not await task.is_ready():
        progress = await task.get_progress()
        return {
            "task_id": task_id,
            "done": False,
            "progress": progress.meta if progress else None,
        }
    result = await task.get_result()
    out: dict[str, Any] = {"task_id": task_id, "done": True, "ok": not result.is_err}
    if result.is_err:
        out["error"] = {
            "type": type(result.error).__name__,
            "message": str(result.error),
        }
        return out
    out["value"] = result.return_value
    node = node_of(result.return_value)
    if node:
        out["ran_on"] = {"node": node, "machine": (await machines()).get(node)}
    return out


# -- tasks -------------------------------------------------------------------


class ChargeIn(BaseModel):
    order_id: str = Field(examples=["A-1007"])
    cents: int = 1500


@api.post("/api/charges")
async def start_charge(body: ChargeIn) -> dict[str, str]:
    """A task with retries and an audit callback; ids ending in 7 fail once."""
    task = await charge.kiq(body.order_id, body.cents)
    return {"task_id": task.task_id, "served_by": NAME}


@api.get("/api/tasks/{task_id}")
async def task_state(task_id: str) -> dict[str, Any]:
    """Poll a task by id, from any web server."""
    return await outcome(task_id)


@api.get("/api/tasks/{task_id}/events")
async def task_events(task_id: str) -> StreamingResponse:
    """The live events of an `events=True` task, as server-sent events."""
    task = app.tasks.handle(task_id)

    async def stream() -> AsyncIterator[str]:
        try:
            async for event in task.events(timeout=60):
                data = {"kind": event.kind, "data": event.data}
                yield f"data: {json.dumps(data, default=str)}\n\n"
        except (TimeoutError, ValueError) as exc:
            yield f"data: {json.dumps({'kind': 'error', 'data': str(exc)})}\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream")


class ShipIn(BaseModel):
    customer: str = Field(examples=["ada"])
    order_id: str = Field(examples=["A-1"])


@api.post("/api/shipments")
async def start_shipment(body: ShipIn) -> dict[str, str]:
    """One customer's shipments run one at a time, in order; customers in parallel."""
    task = await ship.kiq(customer=body.customer, order_id=body.order_id)
    return {"task_id": task.task_id, "served_by": NAME}


@api.post("/api/prices/{sku}")
async def get_price(sku: str) -> dict[str, Any]:
    """Request / reply: wait for the task's value (or its own exception)."""
    try:
        return {"sku": sku, "cents": await price.call(sku), "served_by": NAME}
    except KeyError as exc:
        raise HTTPException(404, f"the task raised KeyError({exc})") from exc
    except TaskTimeoutError as exc:
        raise HTTPException(504, "no answer in time") from exc


class ImportIn(BaseModel):
    rows: int = Field(50, ge=10, le=500)


@api.post("/api/imports")
async def start_import(body: ImportIn) -> dict[str, str]:
    """Progress, streamed output and logs: follow it at /api/tasks/{id}/events."""
    task = await import_catalog.kiq(body.rows)
    return {"task_id": task.task_id, "served_by": NAME}


class CountIn(BaseModel):
    count: int = Field(20, ge=1, le=200)


@api.post("/api/spread")
async def spread(body: CountIn) -> dict[str, Any]:
    """Send many tiny tasks and report which worker processes ran them."""
    group = app.group()
    for _ in range(body.count):
        await group.kiq(whoami)
    try:
        results = await group.wait(timeout=30)
    except SynTasksResultTimeoutError as exc:
        raise HTTPException(
            504, "the workers didn't finish in time (are they up?)"
        ) from exc
    table = await machines()
    by_node: dict[str, int] = {}
    by_machine: dict[str, int] = {}
    for result in results:
        node = str(result.return_value)
        by_node[node] = by_node.get(node, 0) + 1
        machine = table.get(node, "gone")
        by_machine[machine] = by_machine.get(machine, 0) + 1
    return {
        "sent": body.count,
        "by_machine": by_machine,
        "by_node": by_node,
        "served_by": NAME,
    }


@api.post("/api/partner")
async def partner(body: CountIn) -> dict[str, Any]:
    """`max_tasks=3` and `rate=10/s` hold across all workers: measure them."""
    group = app.group()
    for i in range(body.count):
        await group.kiq(call_partner, i)
    spans = [r.return_value for r in await group.wait(timeout=60)]
    edges = sorted(
        [(s["started"], 1) for s in spans] + [(s["ended"], -1) for s in spans]
    )
    peak = running = 0
    for _, step in edges:
        running += step
        peak = max(peak, running)
    took = max(s["ended"] for s in spans) - min(s["started"] for s in spans)
    table = await machines()
    machines_used = sorted({table.get(s["node"], "gone") for s in spans})
    return {
        "calls": body.count,
        "peak_at_once": peak,
        "limit_at_once": 3,
        "per_second": round(body.count / took, 1),
        "limit_per_second": 10,
        "ran_on": machines_used,
    }


class ReindexIn(BaseModel):
    sku: str = Field(examples=["KB-01"])
    version: int


@api.post("/api/reindex")
async def start_reindex(body: ReindexIn) -> dict[str, str]:
    """Debounce: send it quickly several times; only the last version runs."""
    task = await reindex.kiq(sku=body.sku, version=body.version)
    return {"task_id": task.task_id, "served_by": NAME}


# -- campaigns (batches) -----------------------------------------------------


class CampaignIn(BaseModel):
    size: int = Field(40, ge=1, le=2000)


async def campaign_state(campaign_id: str) -> dict[str, Any]:
    batch = await app.batch(campaign_id)
    progress = await batch.progress()
    return {
        "id": campaign_id,
        **dataclasses.asdict(progress),
        "pending": progress.pending,
    }


@api.post("/api/campaigns")
async def start_campaign(body: CampaignIn) -> dict[str, Any]:
    """A campaign of emails as a batch; it starts paused so you can resume it."""
    batch = await app.batch(f"web-{int(time.time())}", owner=f"web:{NAME}")
    await batch.pause()
    members = ({"to": f"customer{n}@example.com"} for n in range(body.size))
    await batch.map(campaign_email, members, chunk=10)
    await batch.then(campaign_report)
    return await campaign_state(batch.id)


@api.get("/api/campaigns/{campaign_id}")
async def get_campaign(campaign_id: str) -> dict[str, Any]:
    return await campaign_state(campaign_id)


@api.post("/api/campaigns/{campaign_id}/{action}")
async def control_campaign(campaign_id: str, action: str) -> dict[str, Any]:
    if action not in ("pause", "resume", "cancel"):
        raise HTTPException(404, "use pause, resume or cancel")
    batch = await app.batch(campaign_id)
    await getattr(batch, action)()
    return await campaign_state(campaign_id)


# -- schedules ---------------------------------------------------------------


async def schedule_view(schedule: Any, *, with_result: bool = False) -> dict[str, Any]:
    out = {
        "id": schedule.id,
        "task": schedule.task,
        "source": schedule.source,
        "state": schedule.state,
        "when": schedule.describe(),
        "next": iso(schedule.next_ms),
        "runs": schedule.runs,
        "last_status": schedule.last_status,
        "owner": schedule.owner,
        "meta": schedule.meta,
    }
    if with_result and schedule.last_task_id:
        last = await outcome(schedule.last_task_id)
        out["last_ran_on"] = last.get("ran_on")
    return out


@api.get("/api/schedules")
async def list_schedules() -> list[dict[str, Any]]:
    """Declared schedules (`code`) and users' posts (`api`), with where the last run ran."""
    return [
        await schedule_view(s, with_result=True) async for s in app.schedules.list()
    ]


@api.get("/api/schedules/{schedule_id}/runs")
async def schedule_runs(schedule_id: str) -> list[dict[str, Any]]:
    return await app.schedules.runs(schedule_id, limit=10)


class PostIn(BaseModel):
    owner: str = Field("user:ada", examples=["user:ada"])
    account: str = "ada-insta"
    text: str
    after: str = Field("1m", description="delay, e.g. 10s, 5m, 2h")


@api.post("/api/posts")
async def schedule_post(body: PostIn) -> dict[str, Any]:
    """A user schedules a post for later: a runtime schedule, stored in Redis."""
    try:
        created = await publish_post.schedule(
            after=body.after,
            kwargs={"account": body.account, "text": body.text},
            owner=body.owner,
            tags=["web"],
            meta={"title": body.text},
        )
    except ScheduleLimitError as exc:
        raise HTTPException(429, str(exc)) from exc
    except ScheduleError as exc:
        raise HTTPException(400, str(exc)) from exc
    return await schedule_view(created)


@api.get("/api/posts")
async def list_posts(owner: str | None = None) -> list[dict[str, Any]]:
    page = (
        await publish_post.schedules.list(owner=owner)
        if owner
        else await publish_post.schedules.list()
    )
    return [await schedule_view(s) for s in page.items]


@api.delete("/api/posts/{schedule_id}")
async def cancel_post(schedule_id: str) -> dict[str, str]:
    await app.schedules.cancel(schedule_id)
    return {"cancelled": schedule_id}


# -- the cluster -------------------------------------------------------------


@api.get("/api/cluster")
async def cluster() -> dict[str, Any]:
    """Who is running: worker processes grouped by machine, queues, channels, feeds."""
    table = await machines()
    nodes = await app.nodes()
    return {
        "served_by": NAME,
        "nodes": [
            {
                "node": n.node_id,
                "machine": table.get(n.node_id),
                "alive": n.alive,
                "running": f"{n.running}/{n.capacity}",
                "channels": n.channels,
            }
            for n in sorted(nodes, key=lambda n: (table.get(n.node_id, ""), n.node_id))
        ],
        "queues": [q.to_dict() for q in await app.queues.list()],
        "channels": [
            {**dataclasses.asdict(c), "owner_machine": table.get(c.owner or "")}
            for c in await app.channels.list()
        ],
        "feeds": await app.feeds.states(),
    }


@api.get("/api/vendor-calls")
async def vendor_calls(symbols: str = "AAPL,MSFT,NVDA") -> dict[str, int]:
    """How often the `quote` feed asked the 'vendor': once per round per watched symbol."""
    names = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    keys = [app.keys("demo", "vendor", s) for s in names]
    values = await app.redis.mget(keys) if keys else []
    return {s: int(v or 0) for s, v in zip(names, values, strict=True)}


# -- sockets -----------------------------------------------------------------


@api.websocket("/ws/live")
async def live_socket(
    ws: WebSocket, identity: str = "guest:anon", name: str = ""
) -> None:
    """
    The `sales:live` channel.

    The handler runs once in the cluster; this server only relays.
    `staff:ada` and `staff:alan` may read, only `staff:ada` may send.
    """
    await ws.accept()
    try:
        async with app.channels.connect(
            "sales:live", identity=identity, meta={"name": name or identity}
        ) as conn:
            await ws.send_json(
                {"op": "connected", "identity": identity, "served_by": NAME}
            )

            async def forward() -> None:
                async for message in conn:
                    await ws.send_json(
                        {"op": "message", "kind": message.kind, "data": message.data}
                    )

            forwarding = asyncio.create_task(forward())
            try:
                while True:
                    text = await ws.receive_text()
                    try:
                        await conn.send(text)
                    except ChannelDenied as exc:
                        await ws.send_json({"op": "denied", "error": str(exc)})
            except WebSocketDisconnect:
                pass
            finally:
                forwarding.cancel()
    except ChannelDenied as exc:
        await ws.send_json({"op": "denied", "error": str(exc)})
        await ws.close()


@api.websocket("/ws/watch")
async def watch_socket(ws: WebSocket, user: str = "demo") -> None:
    """
    Watchlists over the `quote` feed.

    JSON requests from the page (`subscribe`, `unsubscribe`, `list`, `restore`)
    are answered on the socket, and so is every update.
    """
    await ws.accept()
    async with app.feeds.watch(f"user:{user}") as watch:

        async def push() -> None:
            async for update in watch:
                await ws.send_json(update.to_dict())

        pusher = asyncio.create_task(push())
        try:
            while True:
                reply = await watch.handle(await ws.receive_json())
                await ws.send_json({**reply, "served_by": NAME})
        except WebSocketDisconnect:
            pass
        finally:
            pusher.cancel()
