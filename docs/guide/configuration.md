---
order: 3
---

# Configuration

All settings live in one TOML file. The [reference](../reference/config.md)
lists every setting with its type, default and scope.

## Where it's read from

1. `--config PATH` (CLI) or `SynTasks("path.toml")` / `SynTasks(Config(...))` (code)
2. `$SYNTASKS_CONFIG`
3. `./syntasks.toml`
4. `[tool.syntasks]` in `./pyproject.toml`
5. built-in defaults

## Precedence

From lowest to highest:

1. library defaults
2. `[defaults]` (options every task starts from)
3. arguments in code, e.g. `@app.task(timeout="30s")`
4. `[tasks."module:function"]`
5. the selected profile
6. environment variables
7. CLI flags

Operations settings beat code: a timeout in the file overrides the one in
the decorator, so you can tune a deployment without changing code.

## Environment variables

Any setting: `SYNTASKS__<SECTION>__<KEY>`, values parsed as TOML
(`SYNTASKS__WORKER__PROCESSES=8`, `SYNTASKS__WORKER__QUEUES='["a","b"]'`).

Inside the file, strings can reference variables, so secrets stay out of it:

```toml
[redis]
url = "${REDIS_URL}"                       # required
# url = "${REDIS_URL:-redis://localhost}"  # with a default
```

## Profiles

```toml
[worker]
processes = 1

[profiles.production.worker]
processes = 8
max_async_tasks = 500
```

Select with `--profile production` or `SYNTASKS_PROFILE=production`.

## Units

| Kind | Examples |
|---|---|
| duration | `"250ms"`, `"30s"`, `"5m"`, `"1h30m"`, `"2d"`, or a number of seconds |
| size | `"512"`, `"64KiB"`, `"10MB"`, `"2GiB"` |
| rate | `"100/s"`, `"5/m"`, `"10/30s"` |

Unknown keys are errors with a suggestion
(`worker.max_async_task: unknown setting. Did you mean 'max_async_tasks'?`).

## Node scope and cluster scope

With many machines, some settings describe **this machine** (node scope:
`[worker]`, memory limits, `channels.max_handlers`), and some must be
**the same everywhere** (cluster scope: `[defaults]`, `[tasks.*]`,
`[queues.*]`, `[scheduler]`).

On start, every worker publishes its cluster-scope settings to Redis with
`[app] revision`. A worker with an older revision (an old deploy that
restarted late) adopts the stored settings instead and logs a warning, so
two machines never enforce different global limits. Set `revision` to
your build number in CI; without it, the last machine to start wins.

## Inspecting

```bash
syntasks config check            # validate the file and the app's tasks
syntasks config show --sources   # effective config, with where each value came from
```
