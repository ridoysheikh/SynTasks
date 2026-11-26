# FastAPI example

A FastAPI app that hands slow work to SynTasks workers and bridges a
websocket to a channel. `syntasks.toml` holds the Redis URL and worker
settings; the web app and the workers share `tasks.py`.

```bash
pip install syntasks fastapi uvicorn
syntasks run                    # one terminal (or several machines)
uvicorn main:api                # another
```

| Endpoint | Shows |
| --- | --- |
| `POST /reports/{month}` | sends `build_report` and returns its task id at once |
| `GET /reports/{task_id}` | progress while it runs (`ProgressTracker`), then the result |
| `WS /ws?user=ada` | watchlists: subscribe and unsubscribe groups of `quote:SYMBOL` channels while connected; saved per user; one feed for the whole cluster |
| `WS /rooms/{room}` | a chat room: what you send goes to the room's handler, which numbers it and broadcasts it to everyone connected |

```bash
id=$(curl -s -X POST localhost:8000/reports/2026-11 | python -c "import json,sys; print(json.load(sys.stdin)['task_id'])")
curl -s localhost:8000/reports/$id          # {"done": false, "progress": "40%"}
```

The room handler runs on one worker at a time, wherever it was started.
Stop that worker and the next message starts it on another one, with the
count it saved. Every uvicorn process can serve the same room; their readers
share one Redis connection per process.

Watchlists (the `quote` feed in `tasks.py`, the socket in `main.py`):

```text
websocat 'ws://localhost:8000/ws?user=ada'
{"op": "subscribe", "group": "watchlist", "channels": ["quote:aapl", "quote:MSFT"]}
{"op": "subscribe", "group": "header", "channels": ["quote:AAPL"]}
{"op": "unsubscribe", "group": "watchlist"}      # AAPL stays: the header holds it
```

Open the socket from several terminals and users: `quote()` still runs once
a second per symbol in total, on one worker, and stops 30 seconds after the
last watcher leaves. A new tab sends `{"op": "restore"}` to get back the
groups saved for its user, and a new subscriber gets the last price at once.

See [Channels](../../docs/guides/channels.md) and
[Results and logs](../../docs/guides/results-and-logs.md).
