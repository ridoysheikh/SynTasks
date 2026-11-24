import asyncio

from tasks import add, app


async def main() -> None:
    task = await add.kiq(1, 2)
    result = await task.wait_result(timeout=10)
    print(f"1 + 2 = {result.return_value}")
    await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
