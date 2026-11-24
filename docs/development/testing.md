# Testing

```bash
uv sync --all-extras                      # dev environment, with every extra
uv run ruff check src tests examples test-preview && uv run ruff format --check src tests examples test-preview
uv run mypy src tests
uv run pytest -q -n auto
```

## Redis

Tests that need Redis start their own throwaway `redis-server` on a free
port, with no persistence, so **your own Redis is never touched**. It must
be on `PATH`:

```bash
sudo apt-get install redis-server          # Debian / Ubuntu
brew install redis                         # macOS
```

Or point them at a disposable server; every test flushes its database:

```bash
SYNTASKS_TEST_REDIS_URL=redis://127.0.0.1:6390/0 uv run pytest -q
```

With neither, the Redis tests are skipped and the rest still run.

## What runs where

| Tests | Cover |
| --- | --- |
| `tests/cluster/` | Lua primitives: leases, semaphores, rate limits, node registry, clock |
| `tests/app/test_queues.py` | routing, priorities, delays, retries, dead letters, dedupe, every kind of limit, timeouts, cancelling, isolation, memory, takeover of abandoned messages, log capture |
| `tests/app/test_scheduler.py` | static sync, dynamic CRUD, TTLs, exactly-once fires across workers, recovery of half-done fires, misfire and overlap |
| `tests/scheduler/` | cron (with `L` / `#`) and calendar rules, checked against a brute-force walk of two years in five time zones, DST included |
| `tests/app/test_modules.py`, `test_code_config.py` | modules, `extends`, required limits, code-only config, the limits manifest |
| `tests/app/test_runtime_schedules.py` | templates, owners, tags, paging, caps, bulk operations, orphans |
| `tests/app/test_flow_control.py` | `fair`, `debounce`, `throttle`, `rate_per_key` |
| `tests/app/test_completion.py` | callbacks, group fan-in, `call`, batches |
| `tests/app/test_events_and_presence.py` | task events and `Emit`; channel limits, `authorize`, presence |
| `tests/app/test_feeds.py`, `test_watch.py` | feeds (shared fetching, linger, last value, health, shards, streams) and watches (groups, saved groups, conflation, the JSON protocol) |
| `tests/app/test_channels.py` | broadcast, handlers, takeover after a crash, shared consumers |
| `tests/app/test_results.py` | push-based results, groups, progress |
| `tests/cli/test_cli_e2e.py` | a real `syntasks worker` with two processes, driven through the CLI |
| `tests/test_docs.py` | `docs/reference/config.md` matches the config schema |
| everything else | units: config, units, payload limits, cron, codec, dependencies, middlewares, receiver |

After changing a setting in `src/syntasks/config/schema.py`, regenerate the
reference:

```bash
uv run python -c "from syntasks.config.dump import config_reference; open('docs/reference/config.md', 'w').write(config_reference())"
```

There is no CI service: releases are cut from a developer machine. `scripts/release.sh`
runs ruff, `ruff format --check`, mypy and the suite (skip with `--skip-tests`)
before it builds, and you can run the same four commands at any time. The suite
runs on the Python you have; to cover other versions, run it under each
(`uv run --python 3.11 pytest -q`).

Before a release, also run `test-preview/walkthrough.py`: it drives every
feature on two real worker supervisors and kills one of them.

## Contributing

Open an issue or a draft pull request with your question. Keep the core on
the standard library only: anything that needs a third-party package goes
behind an extra.
