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

## Web servers: try it from a browser

`shop/web.py` is a FastAPI app in front of the same `app` the workers run:
the web server only sends tasks, reads results and events, schedules posts
and keeps sockets; the workers do the work. Run it the way you would in
production, as separate processes that share only Redis.

Install the web dependencies once (`fastapi`, `uvicorn`, `websockets`):

```bash
uv sync --all-extras --group preview        # from the repository root
```

Then start five things, each in its own terminal, from `test-preview/`.
Redis listens on 55460, which is what `syntasks.toml` expects:

```bash
redis-server --port 55460 --save "" --appendonly no      # or: docker compose up -d --wait

../.venv/bin/syntasks worker                              # worker1 (one "machine", 2 processes)
../.venv/bin/syntasks worker                              # worker2 (another "machine")

WEB_NAME=web1 ../.venv/bin/uvicorn shop.web:api --port 8001
WEB_NAME=web2 ../.venv/bin/uvicorn shop.web:api --port 8002
```

Open <http://127.0.0.1:8001/> and <http://127.0.0.1:8002/>. Each page shows
which web server served it, and the worker processes grouped by machine.
Every card is a feature; the cards say what to expect. Things worth doing:

- **Send on one server, read on the other.** Start a charge on web1, then look it up on web2: `GET /api/tasks/<id>`. The task ran on whichever worker had room.
- **Spread over the workers.** Send 40 tiny tasks: both machines and all four processes get a share.
- **Kill a worker** (`Ctrl-C` worker1, or `kill -9` its supervisor) while the cluster card refreshes. Its processes disappear, the other machine carries on, the `heartbeat` schedule keeps firing, and a channel it hosted moves.
- **Two tabs, two web servers.** Connect `staff:ada` on web1 and `staff:alan` on web2 to the live channel: one handler serves both. Alan can read but is refused when he posts; `guest:eve` can't connect.
- **Watchlists in two tabs.** Open the same user on both servers, subscribe a group of symbols, then use *Restore* in the second tab: it joins the first tab's groups and gets the last values at once. *Vendor calls* shows the vendor is asked once per symbol, and it stops about two seconds after the last tab leaves.
- **Posts.** Schedule four posts for one owner: the fourth is refused (`max_per_owner = 3`). A post set 20 s ahead runs on a worker and shows `done / ok`.

The endpoints are plain JSON, so `curl` works too, and
<http://127.0.0.1:8001/docs> lists them all. The `XX-99` price lookup
raises on the worker on purpose, so it appears under `syntasks queues dlq default`.

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
| `shop/web.py` | the FastAPI web server: sends tasks, reads results and events, schedules posts, relays the live channel and the watchlist sockets |
| `shop/index.html` | the page it serves at `/`: one card per feature |
| `walkthrough.py` | runs the story above |
| `docker-compose.yml` | Redis on port 55460, if you'd rather not install `redis-server` |
