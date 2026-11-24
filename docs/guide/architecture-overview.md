---
order: 3
---

# Architecture overview

SynTasks has a very simple structure.
On the client side all messages are sent by `kickers` using `brokers`.
On the worker side all messages are received by the `broker` and results are stored in result backends.

On the sequence diagram it looks like this:

::: info Cool tip!
If you use dark theme and cannot see words on diagram,
try switching to light theme and back to dark.
:::

```sequence
rect rgb(191, 223, 255)
note right of Your code: Client side.
Your code ->> Kicker: assemble message
Kicker ->> Broker: Send message
end
rect rgb(191, 223, 255)
note right of Broker: Worker side.
Broker ->> External Queue: Send message
External Queue ->> Broker: Receive
Broker ->> Broker: Execute task
Broker ->> Result backend: Save the result
end
Result backend ->> Your code: Receive the result
```

Let's discuss every component.

## Broker

Brokers are the most critical element of syntasks. Every broker **must** implement the `AsyncBroker` abstract class from [syntasks.abc.broker](https://github.com/synlor/syntasks/blob/main/syntasks/abc/broker.py) to make things work.

`AsyncBroker` class has two main methods to implement:

- kick
- listen

The `kick` method puts the message in the external system.
For example, it may call the `PUB` command in Redis.

The `listen` is a method with an infinite loop that reads messages from the external system and creates a task for processing messages. For example, it subscribes to the Redis channel and waits for new messages.

## Kicker

Kicker is an object that is used to form a message for a broker. This class isn't extendable.
To form a message kicker uses labels, task name and arguments.

When you call the `task.kiq` on a task, it generates a Kicker instance and is a shortening for the `task.kicker().kiq(...)`. You can use kicker to change broker, add labels, or even change task_id.

```python
import asyncio

from syntasks.brokers.inmemory_broker import InMemoryBroker

broker = InMemoryBroker()
second_broker = InMemoryBroker()


@broker.task
async def my_async_task() -> None:
    """My lovely task."""
    await asyncio.sleep(1)
    print("Hello")


async def main():
    # This task was initially assigned to broker,
    # but this time it is going to be sent using
    # the second broker with additional label `delay=1`.
    task = await my_async_task.kicker().with_broker(second_broker).with_labels(delay=1).kiq()
    print(await task.get_result())


asyncio.run(main())

```

## Messages

Every message has labels. You can define labels
using `task` decorator, or you can add them using kicker.

For example:

```python

@broker.task(my_label=1, label2="something")
async def my_async_task() -> None:
    """My lovely task."""
    await asyncio.sleep(1)
    print("Hello")

async def main():
    await my_async_task.kiq()
```

It's equivalent to this

```python

@broker.task
async def my_async_task() -> None:
    """My lovely task."""
    await asyncio.sleep(1)
    print("Hello")

async def main():
    await my_async_task.kicker().with_labels(
        my_label=1,
        label2="something",
    ).kiq()
```

Also you can assign custom task names using decorator.
This is useful to be sure that task names are unique and resolved correctly.
Also it may be useful to balance message routing in some brokers.

For example:

```python
@broker.task(task_name="my_tasks.add_one", label1=1)
async def my_async_task() -> None:
    """My lovely task."""
    await asyncio.sleep(1)
    print("Hello")

```

## Result backend

Result backend is used to store and get results of the execution.
Results have type `SynTasksResult` from [syntasks.result](https://github.com/synlor/syntasks/blob/main/syntasks/result.py).

Every ResultBackend must implement `AsyncResultBackend` from [syntasks.abc.result_backend](https://github.com/synlor/syntasks/blob/main/syntasks/abc/result_backend.py). By default, brokers use `DummyResultBackend`. It doesn't do anything and cannot be used
in real-world scenarios. But some brokers can override it. For example `InMemoryBroker` by default uses `InMemoryResultBackend` and returns correct results.

## Workers

SynTasks has a command line interface to run workers.
It's simple to get it to work.

You have to provide a path to your broker. As an example, if you want to start listening to new tasks
with a broker that is stored in a variable `mybroker` in the module `my_project.broker` run this in your terminal:

```
syntasks worker my_project.broker:mybroker
```

syntasks can discover task modules to import automatically,
if you add the `-fsd` (file system discover) option.

Let's assume we have project with the following structure:

```
test_project
├── broker.py
├── submodule
│   └── tasks.py
└── utils
    └── tasks.py
```

You can specify all tasks modules to import manually.

```bash
syntasks worker test_project.broker:broker test_project.submodule.tasks test_project.utils.tasks
```

Or you can let syntasks find all python modules named tasks in current directory recursively.

```bash
syntasks worker test_project.broker:broker -fsd
```

If you have uvloop installed, syntasks will automatically install new policies to the event loop.
You can get more info about the CLI in the [CLI](./cli.md) section.

::: info Cool info

By default we start two processes, if you want to change this value, please take a look at `--help`.

:::

## Middlewares

Middlewares are used to modify a message, or take
some actions before or after the task is complete.

You can write your own middlewares by subclassing
the `syntasks.abc.middleware.SynTasksMiddleware`.

Every hook can be sync or async. SynTasks will execute it.

For example, this is a valid middleware.

```python
import asyncio

from syntasks.abc.middleware import SynTasksMiddleware
from syntasks.message import SynTasksMessage


class MyMiddleware(SynTasksMiddleware):
    async def pre_send(self, message: "SynTasksMessage") -> SynTasksMessage:
        await asyncio.sleep(1)
        message.labels["my_label"] = "my_value"
        return message

    def post_send(self, message: "SynTasksMessage") -> None:
        print(f"Message {message} was sent.")

```

Here are methods you can implement in the order they are executed:

- `pre_send` - executed on the client side before the message is sent. Here you can modify the message.
- `post_send` - executed right after the message was sent.
- `pre_execute` - executed on the worker side after the message was received by a worker and before its execution.
- `on_error` - executed after the task was executed if an exception was found.
- `post_execute` - executed after the message was executed.
- `post_save` - executed after the result was saved in the result backend.

You can use sync or async hooks without changing anything, but adding async to the hook signature.

::: warning important note

If an exception happens in middlewares it won't be caught. Please ensure that you have try/except for all edge cases of your middleware.

:::

Middlewares can store information in `message.labels` for
later use. For example `SimpleRetryMiddleware` uses labels
to remember number of failed attempts.

## Context

Context is a useful class with some additional functions.
You can use context to get the broker that runs this task, from inside the task.

Or it has the ability to control the flow of execution. Here's an example of how to get
the context.

::: tabs

@tab Annotated 3.10+


```python
from syntasks import Context, SynTasksDepends, ZeroMQBroker

broker = ZeroMQBroker()


@broker.task
async def my_task(context: Annotated[Context, SynTasksDepends()]):
    ...
```

@tab default values

```python
from syntasks import Context, SynTasksDepends, ZeroMQBroker

broker = ZeroMQBroker()


@broker.task
async def my_task(context: Context = SynTasksDepends()):
    ...
```

:::

Also through contexts you can reject or requeue a task. It's easy as this:


::: tabs

@tab Annotated 3.10+

```python
from typing import Annotated

@broker.task
async def my_task(context: Annotated[Context, SynTasksDepends()]):
   await context.requeue()
```

@tab default values

```python
@broker.task
async def my_task(context: Context = SynTasksDepends()):
   await context.requeue()
```

:::

Calling `requeue` or `reject` stops task execution and either drops the message,
or puts it back to the queue.

Also, with context you'll be able to get current message that was received by the broker
or even an instance of a broker that received a message. This may be useful for lib developers.
