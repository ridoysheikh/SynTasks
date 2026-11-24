import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from tasks import app, build_report


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
    await app.startup()
    yield
    await app.shutdown()


api = FastAPI(lifespan=lifespan)


@api.post("/reports/{month}")
async def start_report(month: str) -> dict[str, str]:
    task = await build_report.kiq(month)
    return {"task_id": task.task_id}


@api.get("/reports/{task_id}")
async def report_status(task_id: str) -> dict[str, Any]:
    task = app.tasks.handle(task_id)
    if not await task.is_ready():
        progress = await task.get_progress()
        return {"done": False, "progress": progress.meta if progress else None}
    result = await task.get_result()
    if result.is_err:
        return {"done": True, "error": repr(result.error)}
    return {"done": True, "report": result.return_value}


@api.websocket("/rooms/{room}")
async def room_socket(ws: WebSocket, room: str) -> None:
    await ws.accept()
    async with app.channels.connect(f"room:{room}") as conn:

        async def forward() -> None:
            async for message in conn:
                await ws.send_json(message.data)

        forwarding = asyncio.create_task(forward())
        try:
            while True:
                await conn.send({"text": await ws.receive_text()})
        except WebSocketDisconnect:
            pass
        finally:
            forwarding.cancel()


@api.websocket("/ws")
async def watchlists(ws: WebSocket) -> None:
    """
    One socket, many watchlists, driven by JSON requests from the browser.

    Requests::

        {"op": "subscribe", "group": "watchlist", "channels": ["quote:AAPL"]}
        {"op": "unsubscribe", "group": "watchlist"}
        {"op": "list"}
        {"op": "restore"}

    Replies come back on the same socket, and so does every update, as
    ``{"op": "data", "channel": "quote:AAPL", "data": {...}, "cached": false}``.
    """
    await ws.accept()
    user = ws.query_params.get("user", "demo")  # your login check goes here
    async with app.feeds.watch(f"user:{user}") as watch:

        async def push() -> None:
            async for update in watch:
                await ws.send_json(update.to_dict())

        pusher = asyncio.create_task(push())
        try:
            while True:
                await ws.send_json(await watch.handle(await ws.receive_json()))
        except WebSocketDisconnect:
            pass
        finally:
            pusher.cancel()
