# Install

SynTasks needs Python 3.11+ and a Redis 6.2+ server. It has no required
Python dependencies.

```bash
pip install syntasks
```

Until it's on PyPI, install a release from GitHub:

```bash
pip install "syntasks @ https://github.com/ridoysheikh/syntasks/releases/download/v0.1.0/syntasks-0.1.0-py3-none-any.whl"
```

## Optional extras

| Extra | Installs | For |
| --- | --- | --- |
| `metrics` | prometheus_client | `PrometheusMiddleware` |
| `opentelemetry` | OpenTelemetry API and instrumentation, psutil | traces and metrics ([Middlewares](../guides/middlewares.md)) |
| `reload` | watchdog, gitignore-parser | `syntasks worker --reload` while developing |
| `orjson`, `msgpack`, `cbor` | the serializer | faster or smaller messages |
| `uv` | uvloop | a faster event loop for workers |

```bash
pip install "syntasks[metrics,uv]"
```

## Redis

Any Redis 6.2 or newer: a single server, or several behind Sentinel. Redis
Cluster isn't supported yet. For a local server:

```bash
docker run -d --name redis -p 127.0.0.1:6379:6379 redis:7-alpine
```

Installing SynTasks gives you the `syntasks` command (`python -m syntasks`
works too); see the [CLI reference](../reference/cli.md).

Next: [Quickstart](quickstart.md).
