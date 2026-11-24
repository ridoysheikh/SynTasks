---
title: Distributed async tasks on Redis
home: true
heroImage: logo.svg
heroAlt: SynTasks logo
heroText: SynTasks
tagline: Distributed async task queue for Python, with Redis built in.
actions:
  - text: Get started
    link: /guide/getting-started.html
    type: primary
  - text: Introduction
    link: /guide/
    type: secondary
highlights:
    - features:
        - title: Zero dependencies
          details: The library uses only the Python standard library. Redis client, brokers, results, schedules and channels are built in.
        - title: Made for many machines
          details: Every machine runs the same worker. Cluster-wide state lives in Redis; roles that need one owner move by lease when a machine dies.
        - title: Scheduling in the library
          details: Second-level schedules declared next to tasks or created at runtime, stored in Redis, fired exactly once across the cluster.
        - title: Limits that hold
          details: Concurrency per machine, per task across the cluster and per key; rate limits; soft and hard timeouts with grace; memory limits.
        - title: Channels
          details: Broadcast streams with replay, and long-running handlers kept alive once per channel, for websocket rooms and stateful loops.
        - title: One config file
          details: syntasks.toml for every setting, with environment overrides, profiles and a generated reference.

copyright: false
footer: MIT Licensed | Copyright © 2026-present <a href="https://synlor.com">Synlor</a> | Derived from <a href="https://github.com/taskiq-python/taskiq">taskiq</a> by Pavel Kirilin
---

```python
from syntasks import SynTasks, every

app = SynTasks()  # reads syntasks.toml


@app.task(timeout="30s", retries=3)
async def charge(customer_id: str, cents: int) -> None: ...


@app.task(schedule=every("10s"))
async def sync_rates() -> None: ...
```

```bash
syntasks worker
```
