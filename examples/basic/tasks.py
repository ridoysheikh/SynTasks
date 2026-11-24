import logging

from syntasks import SynTasks, every

app = SynTasks()  # reads syntasks.toml
logger = logging.getLogger(__name__)


@app.task(timeout="30s", retries=3)
async def add(a: int, b: int) -> int:
    return a + b


@app.task(schedule=every("10s"))
async def heartbeat() -> None:
    logger.info("still here")
