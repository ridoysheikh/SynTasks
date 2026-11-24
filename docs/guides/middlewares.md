# Middlewares

Middlewares hook into sending and running tasks.

```python
import time

from syntasks import SynTasksMiddleware


class Timing(SynTasksMiddleware):
    async def pre_execute(self, message):
        message.labels["started"] = time.time()
        return message

    async def post_execute(self, message, result):
        print(message.task_name, result.execution_time)


app.add_middlewares(Timing())
```

Hooks: `startup`, `shutdown`, `pre_send`, `post_send`, `pre_execute`,
`on_error`, `post_execute`, `post_save`.

Included:

| Middleware | |
|---|---|
| `PrometheusMiddleware` | metrics (`syntasks[metrics]`) |
| `OpenTelemetryMiddleware` | traces and metrics (`syntasks[opentelemetry]`); or `SynTasksInstrumentor().instrument()` |
| `SimpleRetryMiddleware`, `SmartRetryMiddleware` | retries for plain brokers; SynTasks apps have retries built in (`retries=`) |
