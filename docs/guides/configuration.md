# Configuration

`syntasks.toml` holds **infrastructure only**: where Redis is, how many
worker processes a machine runs, timings of nodes, the scheduler and
channels, results and logging. Everything about tasks, schedules, channels
and queues is declared in code ([Modules](modules.md)). The
[reference](../reference/config.md) lists every setting with its type,
default and scope.

```toml
[app]
name = "billing"           # namespace in Redis
app = "billing.app:app"    # where workers find the app
revision = 42              # your build number (see Cluster)

[redis]
url = "${REDIS_URL:-redis://localhost:6379/0}"

[worker]
processes = 4
max_async_tasks = 200      # per machine, split across its processes
```

## Where it's read from

1. `--config PATH` (CLI) or `SynTasks("path.toml")` / `SynTasks(Config(...))` (code)
2. `$SYNTASKS_CONFIG`
3. `./syntasks.toml`
4. `[tool.syntasks]` in `./pyproject.toml`
5. built-in defaults

## Precedence

From lowest to highest: library defaults, the file, the selected
profile, environment variables, CLI flags.

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

The same units work in code: `@app.task(timeout="30s", max_payload="64KiB", rate="100/s")`.

Unknown keys are errors with a suggestion
(`worker.max_async_task: unknown setting. Did you mean 'max_async_tasks'?`),
and so are the sections that belong in code:

```text
[tasks] isn't a config section; task options are set in code: SynTasks(...), Module(...) or @app.task(...).
```

## Node scope and cluster scope

Some settings describe **this machine** (node scope: `[worker]`,
`channels.max_handlers`), and some must be **the same everywhere**
(cluster scope: most of `[scheduler]`). On start, every worker publishes
its cluster-scope settings and the limits its code declares, with
`[app] revision`; a worker with an older revision adopts the stored ones
and logs a warning. See [Modules › Every machine enforces the same
limits](modules.md#every-machine-enforces-the-same-limits).

## Inspecting

```bash
syntasks config check            # validate the file, import the app, check every declaration
syntasks config show --sources   # effective config, with where each value came from
```
