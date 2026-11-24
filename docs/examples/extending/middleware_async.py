from asyncio import sleep
from typing import Any

from syntasks import SynTasksMessage, SynTasksMiddleware, SynTasksResult


class MyMiddleware(SynTasksMiddleware):
    async def startup(self) -> None:
        print("RUN STARTUP")
        await sleep(1)

    async def shutdown(self) -> None:
        print("RUN SHUTDOWN")
        await sleep(1)

    async def pre_execute(self, message: "SynTasksMessage") -> SynTasksMessage:
        await sleep(1)
        return message

    async def post_save(
        self,
        message: "SynTasksMessage",
        result: "SynTasksResult[Any]",
    ) -> None:
        await sleep(1)
        print("Post save")
