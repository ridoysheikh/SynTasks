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


@api.websocket("/ticker/{symbol}")
async def ticker_socket(ws: WebSocket, symbol: str) -> None:
    """Every uvicorn process and every user shares the one feed of this symbol."""
    await ws.accept()
    async with app.channels.connect(f"ticker:{symbol}", identity="anonymous") as conn:
        try:
            async for message in conn:
                if message.kind == "message":
                    await ws.send_json(message.data)
        except WebSocketDisconnect:
            pass
