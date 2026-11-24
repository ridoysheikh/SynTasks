# Third-party notices and credits

SynTasks is MIT licensed (see `LICENSE`). It has **no runtime dependencies**:
the core uses only the Python standard library, and the Redis client is part
of SynTasks.

## Code SynTasks is derived from

| Project | License | What came from it |
|---|---|---|
| [taskiq](https://github.com/taskiq-python/taskiq) | MIT | SynTasks started as a fork of taskiq. The broker, task, message, middleware, result backend and receiver abstractions, the serializers and formatters, and parts of the CLI are copied from or based on taskiq. |
| [taskiq-dependencies](https://github.com/taskiq-python/taskiq-dependencies) | MIT | Vendored as `syntasks.dependencies` (dependency injection for tasks), kept close to upstream. |

Both are Copyright (c) Pavel Kirilin and the taskiq contributors. Their
copyright notices are kept in `LICENSE`, as the MIT License requires.

## Optional dependencies

Installed only when you ask for them through an extra. Each keeps its own
license, which ships inside its own distribution.

| Extra | Project | License |
|---|---|---|
| `metrics` | [prometheus_client](https://github.com/prometheus/client_python) | Apache-2.0 |
| `opentelemetry` | [OpenTelemetry Python](https://github.com/open-telemetry/opentelemetry-python), [psutil](https://github.com/giampaolo/psutil) | Apache-2.0, BSD-3-Clause |
| `reload` | [watchdog](https://github.com/gorakhargosh/watchdog), [gitignore-parser](https://github.com/mherrmann/gitignore_parser) | Apache-2.0, MIT |
| `orjson` | [orjson](https://github.com/ijl/orjson) | Apache-2.0 or MIT |
| `msgpack` | [msgpack](https://github.com/msgpack/msgpack-python) | Apache-2.0 |
| `cbor` | [cbor2](https://github.com/agronholm/cbor2) | MIT |
| `uv` | [uvloop](https://github.com/MagicStack/uvloop) | Apache-2.0 or MIT |

## Design inspiration (no code copied)

| Project | License | Idea we drew on |
|---|---|---|
| [Redis](https://redis.io) documentation and patterns | – | GCRA rate limiting, sorted-set semaphores, streams with consumer groups |
| Vixie cron | ISC | Cron field syntax and the day-of-month / day-of-week rule |

Thank you to the authors and maintainers of all of these projects.
