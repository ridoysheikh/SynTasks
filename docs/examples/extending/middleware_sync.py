from time import sleep
from typing import Any

from syntasks import SynTasksMessage, SynTasksMiddleware, SynTasksResult


class MyMiddleware(SynTasksMiddleware):
    def startup(self) -> None:
        print("RUN STARTUP")
        sleep(1)

    def shutdown(self) -> None:
        print("RUN SHUTDOWN")
        sleep(1)

    def pre_execute(self, message: "SynTasksMessage") -> SynTasksMessage:
        sleep(1)
        return message

    def post_save(
        self,
        message: "SynTasksMessage",
        result: "SynTasksResult[Any]",
    ) -> None:
        sleep(1)
        print("Post save")
