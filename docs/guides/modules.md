# Modules and required limits

Big apps declare tasks in many files and folders. A **module** groups the
declarations of one file or folder and sets the limits everything in it
starts from, the way a router does in a web framework.

## Declaring a module

```python
# billing/payments.py
from syntasks import Module

module = Module("billing.payments", timeout="30s", retries=3, max_tasks=50)


@module.task(timeout="2m")                 # overrides the module's timeout
async def charge(customer_id: str, cents: int) -> None: ...


@module.task(key="kwargs.customer_id", max_tasks_per_key=1)
async def refund(customer_id: str, cents: int) -> None: ...
```

A module has the same decorators as the app: `task`, `schedule`, `channel`
and `queue`.

## Attaching modules

```python
# billing/__init__.py
from syntasks import Module
from . import invoices, payments

module = Module("billing", queue="billing")
module.extends(invoices.module, payments.module)

# app.py
from syntasks import SynTasks
import billing

app = SynTasks(timeout="5m", retries=0, max_tasks=None)   # the root module
app.extends(billing.module)
```

- `extends` also takes a path, `"billing.payments:module"`, to avoid
  import cycles. Nothing is scanned: you list the modules.
- Modules nest. A module can be attached to one app; attaching it again
  does nothing.
- A module's task is a normal task object, but sending it before its
  module is attached raises `ModuleNotAttachedError`.
- Task names stay `python.module:function`; two tasks with the same name
  are an error.

## Inheritance

Options pass down the chain `SynTasks(...)` → `Module(...)` → nested
module → declaration, and the **nearest link wins**. A module's options
are defaults for each member, not a budget the module shares:
`max_tasks=50` on a module means each of its tasks may run 50 at once.

```python
app = SynTasks(timeout="5m", retries=0, max_tasks=None)
billing = Module("billing", timeout="30s", max_tasks=5)

@billing.task(timeout="2m")
async def slow() -> None: ...     # timeout 2m, retries 0, max_tasks 5

@billing.task
async def quick() -> None: ...    # timeout 30s, retries 0, max_tasks 5
```

## Required limits

Every task, schedule and channel must end up with **`timeout`**,
**`retries`** and **`max_tasks`** (runs at once across the cluster), from
itself or a link above it. A missing one is an error when the code is
imported:

```text
billing.payments:charge has no timeout; set it on the task, Module("billing.payments"),
Module("billing") or SynTasks().
```

`None` means "unbounded on purpose" (`timeout=None`, `max_tasks=None`).
For no retries, use `retries=0`.

For channels, `timeout` is the handler's longest life, `retries` the
restarts after a crash, and `max_tasks` the number of live handlers of
its pattern. See [Channels](channels.md).

## Queues

Queues are declared in code too:

```python
app.queue("emails", max_length=1_000_000, on_full="reject", max_deliveries=5)
```

A queue no one declared gets the defaults. See [Queues](queues.md).

## What stays in `syntasks.toml`

Only infrastructure: `[app]`, `[redis]`, `[worker]`, `[node]`,
`[scheduler]`, `[channels]`, `[results]`, `[logging]` and
`[profiles.*]`. Putting `[tasks]`, `[defaults]`, `[queues]` or
`[schedules]` in the file is an error that tells you where they go in
code. See [Configuration](configuration.md).

## Every machine enforces the same limits

On start, each worker publishes the effective options of every task and
queue its code declares, with `[app] revision`. A worker running older
code (a rolling deploy) uses the stored numbers for the tasks it also
has, so two machines never enforce different limits. Set `revision` to
your build number in CI.
