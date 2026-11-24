import pytest

from syntasks import Module, ModuleNotAttachedError, QueueFullError, SynTasks
from syntasks.config import Config, ConfigError
from syntasks.options import MissingLimitError

from .conftest import AppFactory


def make_app(**root: object) -> SynTasks:
    config = Config()
    config.app.name = "t-modules"
    return SynTasks(config, search_config=False, **root)  # type: ignore[arg-type]


def test_required_limits_error_names_chain() -> None:
    app = make_app(retries=0, max_tasks=None)
    billing = Module("billing")

    @billing.task
    async def charge() -> None: ...

    with pytest.raises(MissingLimitError) as exc:
        app.extends(billing)
    text = str(exc.value)
    assert "timeout" in text
    assert "charge" in text
    assert 'Module("billing")' in text
    assert "SynTasks()" in text


def test_required_limits_on_app_task() -> None:
    app = make_app(timeout="1s")
    with pytest.raises(MissingLimitError, match="retries"):

        @app.task
        async def job() -> None: ...


def test_explicit_none_is_unbounded() -> None:
    app = make_app(timeout=None, retries=0, max_tasks=None)

    @app.task
    async def job() -> None: ...

    options = app.options_for(job.task_name)
    assert options.timeout is None
    assert options.max_tasks is None
    assert options.retries == 0


def test_nearest_layer_wins() -> None:
    app = make_app(timeout="300s", retries=0, max_tasks=None)
    billing = Module("billing", timeout="30s", max_tasks=5)

    @billing.task(timeout="2m")
    async def slow() -> None: ...

    @billing.task
    async def quick() -> None: ...

    app.extends(billing)
    assert app.options_for(slow.task_name).timeout == 120
    assert app.options_for(quick.task_name).timeout == 30
    assert app.options_for(quick.task_name).max_tasks == 5


def test_retries_none_rejected() -> None:
    with pytest.raises(ConfigError, match="retries"):
        make_app(timeout=None, retries=None, max_tasks=None)


def test_unknown_option_rejected() -> None:
    app = make_app(timeout=None, retries=0, max_tasks=None)
    with pytest.raises(ConfigError, match="concurrency"):

        @app.task(concurrency=2)
        async def job() -> None: ...


def test_labels_keyword() -> None:
    app = make_app(timeout=None, retries=0, max_tasks=None)

    @app.task(labels={"team": "billing"})
    async def job() -> None: ...

    assert job.labels["team"] == "billing"


def test_extends_registers_nested() -> None:
    app = make_app(timeout=None, retries=0, max_tasks=None)
    outer = Module("outer", timeout="10s")
    inner = Module("outer.inner", max_tasks=3)
    outer.extends(inner)

    @inner.task
    async def deep() -> None: ...

    app.extends(outer)
    assert app.find_task(deep.task_name) is deep
    assert deep.broker is app
    options = app.options_for(deep.task_name)
    assert (options.timeout, options.max_tasks) == (10, 3)


async def test_kiq_before_attach_raises() -> None:
    lonely = Module("lonely", timeout=None, retries=0, max_tasks=None)

    @lonely.task
    async def job() -> None: ...

    with pytest.raises(ModuleNotAttachedError, match="lonely"):
        await job.kiq()


def test_attach_twice_noop() -> None:
    app = make_app(timeout=None, retries=0, max_tasks=None)
    module = Module("m")

    @module.task
    async def job() -> None: ...

    app.extends(module)
    app.extends(module)
    assert app.find_task(job.task_name) is job


def test_module_on_two_apps_rejected() -> None:
    module = Module("m", timeout=None, retries=0, max_tasks=None)
    make_app().extends(module)
    with pytest.raises(ConfigError, match="already attached"):
        make_app().extends(module)


def test_duplicate_task_names_rejected() -> None:
    app = make_app(timeout=None, retries=0, max_tasks=None)
    first, second = Module("first"), Module("second")

    @first.task("shared:name")
    async def one() -> None: ...

    @second.task("shared:name")
    async def two() -> None: ...

    app.extends(first)
    with pytest.raises(ConfigError, match="shared:name"):
        app.extends(second)


def test_extends_by_path() -> None:
    app = make_app()
    app.extends("tests.app.modules_pkg:module")
    assert app.find_task("tests.app.modules_pkg:from_path") is not None


def test_declared_after_attach_registers() -> None:
    app = make_app(timeout=None, retries=0, max_tasks=None)
    module = Module("late")
    app.extends(module)

    @module.task
    async def later() -> None: ...

    assert app.find_task(later.task_name) is later


async def test_queue_declared_in_code(app_factory: AppFactory) -> None:
    app = app_factory()
    small = Module("small")
    small.queue("tiny", max_length=2)

    @small.task(queue="tiny")
    async def job() -> None: ...

    app.extends(small)
    assert app.queue_options("tiny").max_length == 2
    await job.kiq()
    await job.kiq()
    with pytest.raises(QueueFullError):
        await job.kiq()
