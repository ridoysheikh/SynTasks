import asyncio

from syntasks import (
    Context,
    Redis,
    SynTasks,
    SynTasksDepends,
    SynTasksEvents,
    SynTasksState,
)

app = SynTasks()


@app.on_event(SynTasksEvents.WORKER_STARTUP)
async def startup(state: SynTasksState) -> None:
    # Open a client once per worker process and keep it in the state.
    state.cache = Redis.from_url("redis://localhost/1")


@app.on_event(SynTasksEvents.WORKER_SHUTDOWN)
async def shutdown(state: SynTasksState) -> None:
    await state.cache.close()


@app.task
async def get_val(key: str, context: Context = SynTasksDepends()) -> str | None:
    value = await context.state.cache.get(key)
    return None if value is None else value.decode()


@app.task
async def set_val(key: str, value: str, context: Context = SynTasksDepends()) -> None:
    await context.state.cache.set(key, value)


async def main() -> None:
    await app.startup()
    await (await set_val.kiq("key", "value")).wait_result(timeout=10)
    result = await (await get_val.kiq("key")).wait_result(timeout=10)
    print(f"Got value: {result.return_value}")
    await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
