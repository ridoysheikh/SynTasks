---
order: 1
dir:
  order: 1
---

# Introduction

## What is syntasks

SynTasks is a library that helps you send and process python functions in a distributed manner.
For example, you have many heavy to calculate functions you want to execute on another server.
You can implement interservice communication by yourself, or you can use SynTasks to make the job done easily.

The core library doesn't have much functionality. It provides two built-in brokers, CLI, basic functionality for creating distributed tasks, and abstractions to extend syntasks. The main idea of syntasks is to make it modular and easy to extend. We have libraries for many
possible use cases, but if you lack something, you can adapt syntasks to fit your needs.

## Why not use existing libraries?

We created this project because we couldn't find any project that can execute and send async functions using distributed queues like RabbitMQ.

You might have seen projects built on top of asyncio that solve a similar problem, but here's a comparison table of the syntasks and other projects.

|                Feature name | SynTasks |  Arq  | AioTasks | streaQ |
| --------------------------: | :----: | :---: | :------: | :----: |
|         Actively maintained |   ✅    |   ❌   |    ❌     |   ✅   |
|    Multiple broker backends |   ✅    |   ❌   |    ✅     |   ❌   |
|    Multiple result backends |   ✅    |   ❌   |    ❌     |   ❌   |
|      Rich documentation     |   ✅    |   ❌   |    ❌     |   ✅   |
|   Startup & Shutdown events |   ✅    |   ✅   |    ❌     |   ✅   |
|      Ability to abort tasks |   ❌    |   ✅   |    ❌     |   ✅   |
|          Custom serializers |   ✅    |   ✅   |    ❌     |   ✅   |
|        Dependency injection |   ✅    |   ❌   |    ❌     |   ❌   |
|              Task pipelines |   ✅    |   ✅   |    ❌     |   ✅   |
|              Task schedules |   ✅    |   ✅   |    ❌     |   ✅   |
|          Global middlewares |   ✅    |   ❌   |    ❌     |   ✅   |

If you have a fully synchronous project, consider using celery or dramatiq instead.
