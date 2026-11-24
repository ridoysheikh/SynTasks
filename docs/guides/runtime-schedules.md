# Runtime schedules

Apps that schedule work for their users ("post this at 9:00", "send this
campaign on Monday", "remind me every Friday") usually need a table and a
CRUD layer for it. With SynTasks the schedules themselves live in Redis
and can be listed by owner, so the table isn't needed.

## A template

A schedule declared with `runtime=` is a **template**: the app creates
copies of it at runtime. It may have no timing of its own.

```python
from syntasks import Runtime


@app.schedule(
    runtime=Runtime(max_per_owner=500, max_ttl="400d", min_interval="1m", max_payload="16KiB"),
    timeout="30s",
    retries=3,
    max_tasks=200,
    key="kwargs.account_id",
    rate_per_key="1/s",          # one post per second per social account
)
async def publish_post(account_id: str, post_id: str, text: str) -> None: ...
```

Only templates accept runtime copies; other tasks use `with_delay(...)`
for a one-off later run.

| `Runtime(...)` | Default | Meaning |
|---|---|---|
| `max_per_owner` | `1000` | copies one owner may have at once (checked atomically) |
| `max_ttl` | none | longest lifetime of a copy; copies without `ttl` get it |
| `min_interval` | none | shortest recurrence a copy may use |
| `max_payload` | `[scheduler] max_payload` | size of a copy's arguments |
| `keep_finished` | `"7d"` | how long a finished one-shot copy is kept, so a UI can show "sent at 09:02" |

## Creating copies

```python
s = await publish_post.schedule(
    at="2026-12-01T09:00+06:00",
    kwargs={"account_id": "acc-9", "post_id": "p-1", "text": "Hello"},
    owner="user:42",
    tags=["campaign:spring"],
    meta={"title": "Launch post"},
)

await publish_post.schedule(
    calendar(weekday="fri", at="17:00", tz="Asia/Dhaka"),    # each copy its own time zone
    kwargs={...},
    owner="user:42",
)
```

A copy takes any timing (`at=`, `after=`, `every=`, `cron=` with `tz=`, or
a `calendar(...)` / `every(...)` / `at(...)` positional), the policies of
[Scheduling](scheduling.md#policies), and:

| Argument | Meaning |
|---|---|
| `owner` | who it belongs to, e.g. `"user:42"`: listing and the cap |
| `tags` | up to 20 labels to find it by |
| `meta` | up to 4 KiB of JSON shown in listings |
| `ttl`, `expires_at` | delete it then (at most `max_ttl`) |
| `max_runs` | stop after N runs |
| `id`, `if_exists` | your own id; `"error"`, `"replace"` or `"ignore"` if it exists |

Arguments are JSON, checked against the task's signature, and bounded by
`max_payload`. Over a limit, you get `ScheduleLimitError`.

## Listing, editing, cancelling

```python
page = await publish_post.schedules.list(owner="user:42", state="active", limit=20)
for s in page.items:
    print(s.id, s.describe(), s.next_ms, s.meta, s.last_status)
next_page = await publish_post.schedules.list(owner="user:42", cursor=page.cursor)

page = await app.schedules.find(tag="campaign:spring")          # across templates

s = await publish_post.schedules.update(
    s.id,
    version=s.version,            # ScheduleConflictError if someone changed it first
    at="2026-12-01T10:00+06:00",
    meta={"title": "Launch post (moved)"},
)
await publish_post.schedules.cancel(s.id)

await app.schedules.pause_all(tag="campaign:spring")            # bulk, by owner, tag or template
await app.schedules.resume_all(tag="campaign:spring")
await app.schedules.cancel_all(owner="user:42")
```

Pages come in creation order and the cursor is exact (copies are numbered
when created), so nothing is skipped or repeated while you page.

From the shell: `syntasks schedules list --owner user:42`,
`--tag campaign:spring`, `--template billing.tasks:publish_post`, and
`syntasks schedules cancel --owner user:42`.

## When the template leaves the code

If a deploy removes a template, its copies become **orphaned** at the
next sync: they don't fire, they're listed with that state, and a warning
says how to delete them (`cancel_all(template=...)`). If the template
comes back, they're active again.

## Missed fires

Copies follow the same rules as declared schedules: a fire missed while
the cluster was down runs as soon as a dispatcher sees it (`misfire`,
`misfire_grace`). See [Scheduling › Missed fires](scheduling.md#missed-fires-run-as-soon-as-possible).
