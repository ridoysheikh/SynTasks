---
title: Task manager for asyncio
home: true
heroImage: logo.svg
heroAlt: logo
heroText: SynTasks python
actions:
  - text: Get Started
    link: /guide/getting-started.html
    type: primary
  - text: Introduction
    link: /guide/
    type: secondary
highlights:
    - features:
        - title: Built on proven foundations
          details: SynTasks started from the battle-tested taskiq codebase and is evolving from there.
        - title: Fully asynchronous
          details: SynTasks can run both sync and async functions. You don't have to worry about it.
        - title: Easily extensible
          details: SynTasks has a lot of replaceable components. It's super easy to implement
            your own broker or middleware.
        - title: Strongly typed
          details: SynTasks provides correct autocompletion for most of its functionality.

copyright: false
footer: MIT Licensed | Copyright© 2026-present <a href="https://synlor.com">Synlor</a> | Based on <a href="https://github.com/taskiq-python/taskiq">taskiq</a> by Pavel Kirilin
---

## What is syntasks in a nutshell

Consider syntasks as an asyncio celery implementation. It uses almost the same patterns, but it's more modern
and flexible.

It's not a drop-in replacement for any other task manager. It has a different ecosystem of libraries and a different set of features.
Also, it doesn't work for synchronous projects. You won't be able to send tasks synchronously.


## Installation

You can install syntasks with pip or your favorite dependency manager:

```bash:no-line-numbers
pip install syntasks
```
