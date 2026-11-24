from collections.abc import AsyncGenerator
from copy import copy

import pytest

from syntasks.abc.broker import AsyncBroker
from syntasks.decor import AsyncSynTasksDecoratedTask
from syntasks.events import SynTasksEvents
from syntasks.message import BrokerMessage
from syntasks.state import SynTasksState


class _TestBroker(AsyncBroker):
    """Broker for testing purpose."""

    async def kick(self, message: BrokerMessage) -> None:
        """
        This method is used to send messages.

        But in this case it just throws messages away.

        :param message: message to lost.
        """

    async def listen(self) -> AsyncGenerator[BrokerMessage, None]:  # type: ignore
        """
        This method is not implemented.

        :param callback: callback that is never called.
        """


def test_decorator_success() -> None:
    """Test that decoration without parameters works."""
    tbrok = _TestBroker()

    @tbrok.task
    async def test_func() -> None:
        """Some test function."""

    assert isinstance(test_func, AsyncSynTasksDecoratedTask)


def test_decorator_with_name_success() -> None:
    """Test that task_name is successfully set."""
    tbrok = _TestBroker()

    @tbrok.task(task_name="my_task")
    async def test_func() -> None:
        """Some test function."""

    assert isinstance(test_func, AsyncSynTasksDecoratedTask)
    assert test_func.task_name == "my_task"


def test_decorator_with_labels_success() -> None:
    """Tests that labels are assigned for task as is."""
    tbrok = _TestBroker()

    @tbrok.task(label1=1, label2=2)
    async def test_func() -> None:
        """Some test function."""

    assert isinstance(test_func, AsyncSynTasksDecoratedTask)
    assert test_func.labels == {
        "label1": 1,
        "label2": 2,
    }


def test_kicker_labels_modification() -> None:
    """Test that using kicker.with_labels doesn't modify task's labels globally."""
    broker = _TestBroker()

    @broker.task(test_lb="one")
    async def test_task() -> None: ...

    old_labels = copy(test_task.labels)
    test_kicker = test_task.kicker().with_labels(another_label="test")
    assert "another_label" in test_kicker.labels

    assert test_task.labels == old_labels


@pytest.mark.parametrize(
    ("is_worker_process", "startup", "shutdown"),
    [
        (True, SynTasksEvents.WORKER_STARTUP, SynTasksEvents.WORKER_SHUTDOWN),
        (False, SynTasksEvents.CLIENT_STARTUP, SynTasksEvents.CLIENT_SHUTDOWN),
    ],
)
async def test_async_context_manager_enter(
    *,
    is_worker_process: bool,
    startup: SynTasksEvents,
    shutdown: SynTasksEvents,
) -> None:
    """Test that `__aenter__` and `__aexit__` calls work."""
    broker = _TestBroker()
    broker.is_worker_process = is_worker_process
    startup_called = False
    shutdown_called = False

    @broker.on_event(startup)
    async def track_startup(state: SynTasksState) -> None:
        nonlocal startup_called
        startup_called = True

    @broker.on_event(shutdown)
    async def track_shutdown(state: SynTasksState) -> None:
        nonlocal shutdown_called
        shutdown_called = True

    async with broker as ctx:
        assert ctx is None
        assert startup_called is True
        assert shutdown_called is False

    assert shutdown_called is True


@pytest.mark.parametrize(
    ("is_worker_process", "startup", "shutdown"),
    [
        (True, SynTasksEvents.WORKER_STARTUP, SynTasksEvents.WORKER_SHUTDOWN),
        (False, SynTasksEvents.CLIENT_STARTUP, SynTasksEvents.CLIENT_SHUTDOWN),
    ],
)
async def test_async_context_manager_exit_on_exception(
    *,
    is_worker_process: bool,
    startup: SynTasksEvents,
    shutdown: SynTasksEvents,
) -> None:
    """Test that __aexit__ calls shutdown even if exception is raised."""
    broker = _TestBroker()
    broker.is_worker_process = is_worker_process
    startup_called = False
    shutdown_called = False

    @broker.on_event(startup)
    async def track_startup(state: SynTasksState) -> None:
        nonlocal startup_called
        startup_called = True

    @broker.on_event(shutdown)
    async def track_shutdown(state: SynTasksState) -> None:
        nonlocal shutdown_called
        shutdown_called = True

    with pytest.raises(ValueError, match="Test exception"):
        async with broker:
            assert startup_called is True
            assert shutdown_called is False
            raise ValueError("Test exception")

    assert shutdown_called is True
