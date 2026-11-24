import uuid
from unittest.mock import AsyncMock

import pytest

from syntasks.formatters.json_formatter import JSONFormatter
from syntasks.message import SynTasksMessage
from syntasks.middlewares.simple_retry_middleware import SimpleRetryMiddleware
from syntasks.result import SynTasksResult


@pytest.fixture
def broker() -> AsyncMock:
    mocked_broker = AsyncMock()
    mocked_broker.id_generator = lambda: uuid.uuid4().hex
    mocked_broker.formatter = JSONFormatter()
    mocked_broker.find_task = lambda task_name: None
    return mocked_broker


async def test_successful_retry(broker: AsyncMock) -> None:
    middleware = SimpleRetryMiddleware()
    middleware.set_broker(broker)
    await middleware.on_error(
        SynTasksMessage(
            task_id="test_id",
            task_name="meme",
            labels={
                "retry_on_error": "True",
            },
            args=[],
            kwargs={},
        ),
        SynTasksResult(is_err=True, return_value=None, execution_time=0.0),
        Exception(),
    )
    resend: SynTasksMessage = broker.kick.await_args.args[0]
    assert resend.task_name == "meme"
    assert resend.labels["_retries"] == "1"


async def test_no_retry(broker: AsyncMock) -> None:
    middleware = SimpleRetryMiddleware()
    middleware.set_broker(broker)
    await middleware.on_error(
        SynTasksMessage(
            task_id="test_id",
            task_name="meme",
            labels={},
            args=[],
            kwargs={},
        ),
        SynTasksResult(is_err=True, return_value=None, execution_time=0.0),
        Exception(),
    )
    broker.kick.assert_not_called()


async def test_max_retries(broker: AsyncMock) -> None:
    middleware = SimpleRetryMiddleware(default_retry_count=3)
    middleware.set_broker(broker)
    await middleware.on_error(
        SynTasksMessage(
            task_id="test_id",
            task_name="meme",
            labels={
                "retry_on_error": "True",
                "_retries": "2",
            },
            args=[],
            kwargs={},
        ),
        SynTasksResult(is_err=True, return_value=None, execution_time=0.0),
        Exception(),
    )
    broker.kick.assert_not_called()
