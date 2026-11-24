# SynTasks test preview: a shop on two machines

A small, realistic app that runs every part of SynTasks for real: two
`syntasks worker` "machines" with two processes each, one Redis, and a
walkthrough that drives them step by step, then kills one machine.

## Run it

From the repository root, install the dev environment once:

```bash
uv sync
```

Then run the walkthrough. If `redis-server` is on your `PATH` it starts a
throwaway one on port 55460 and stops it at the end; nothing else is needed:

```bash
cd test-preview
../.venv/bin/python walkthrough.py            # add --pause to go step by step
```

Without `redis-server`, use Docker:

```bash
docker compose up -d --wait
SHOP_REDIS_URL=redis://127.0.0.1:55460/0 ../.venv/bin/python walkthrough.py
docker compose down -v                        # when you're done
```

`SHOP_REDIS_URL` can point at any **disposable** Redis.

## What the walkthrough does

Each step prints the exact command it runs, so you can repeat any of them
by hand (with workers running: `syntasks worker` in this folder).

1. **`syntasks config check`**: the app imports and every declaration has its required limits.
2. **`syntasks nodes`, `schedules list`**: four worker processes; declared schedules synced into Redis.
3. **`python -m shop orders`**: a group of charges; one fails once and is retried; each success sends an audit callback exactly once.
4. **`ship`**: a customer's shipments run one at a time, in order; customers run side by side.
5. **`partner`**: `max_tasks=3` and `rate="10/s"` hold across all machines.
6. **`call`**: request / reply, with the task's own exception.
7. **`export`, `resize`**: a timeout, a run in its own process, and one over its memory limit.
8. **`stream`**: live events, progress and streamed output of an import (`events=True`, `Emit`).
9. **`fair`**: three small shops aren't stuck behind a tenant's 100 emails (`fair=True`).
10. **`debounce`**: 20 quick edits, one reindex with the last version.
11. **`schedules preview`**: a calendar rule, "first Monday at 09:00 in Asia/Dhaka".
12. **`posts`**: users schedule their own posts (a runtime template): owners, tags, metadata, the per-owner cap, an edit with a version, bulk pause and cancel.
13. **`campaign`**: 40 emails as one batch: paused, resumed, cancelled, with one summary at the end.
14. **`live`**: a channel whose handler runs once in the cluster: authorization, presence, join messages.
15. **`watchlists`**: the `quote` feed: a user's groups of symbols subscribed at runtime, one vendor call per symbol however many watch it, linger after the last watcher leaves, saved groups restored in a new tab with the cached last value.
16. **A machine dies**: machine A is killed with `SIGKILL`. Its nodes go away, its work moves to machine B, the channel handler keeps its state and the schedule keeps firing.

Worker logs (audit callbacks, posts, the campaign summary) are in the
folder the walkthrough prints.

## Files

| File | What it shows |
|---|---|
| `syntasks.toml` | infrastructure only: Redis, workers, short node and lease timings for the demo |
| `shop/app.py` | the root `SynTasks(...)` with the default limits, one queue, and `app.extends(...)` of the three modules |
| `shop/orders.py` | `Module("orders", ...)`: retries, a callback, order per key, cluster limits, `call`, timeouts, isolation, events |
| `shop/marketing.py` | `Module("marketing", ...)`: `every` and `calendar` schedules, a runtime template, `fair`, `debounce`, campaign tasks |
| `shop/live.py` | `Module("live", ...)`: a channel handler with `authorize`, and the `quote` feed |
| `shop/__main__.py` | `python -m shop <command>`: one command per feature |
| `walkthrough.py` | runs the story above |
| `docker-compose.yml` | Redis on port 55460, if you'd rather not install `redis-server` |
