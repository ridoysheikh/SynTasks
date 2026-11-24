"""
The shop's command line: each command sends work to the running workers.

    python -m shop orders [N]      N charges as a group; ids ending in 7 are retried
    python -m shop ship            shipments in order per customer, customers side by side
    python -m shop partner [N]     N partner calls under a cluster-wide limit and rate
    python -m shop call            request / reply: wait for a price, or its error
    python -m shop export          a run that hits its timeout
    python -m shop resize MB       a run in its own process, under a memory limit
    python -m shop stream          live events and streamed output of an import
    python -m shop fair            a tenant with 100 emails doesn't starve the others
    python -m shop debounce        20 quick edits, one reindex
    python -m shop posts           users schedule their own posts (runtime schedules)
    python -m shop campaign        a campaign: pause, resume, progress, summary
    python -m shop live            the live channel: authorization, presence, takeover
    python -m shop watchlists      feeds: groups, runtime subscribe, shared fetching
"""

import argparse
import asyncio
import itertools
import time
from typing import Any

from syntasks import (
    ChannelDenied,
    ScheduleLimitError,
    SynTasksResult,
    TaskSupersededError,
)

from .app import app
from .marketing import (
    campaign_email,
    campaign_report,
    publish_post,
    reindex,
    send_email,
)
from .orders import (
    call_partner,
    charge,
    export_orders,
    import_catalog,
    price,
    resize_image,
    ship,
)


def show(result: SynTasksResult[Any]) -> str:
    if result.is_err:
        return f"failed: {type(result.error).__name__}: {result.error}"
    return f"ok: {result.return_value}"


async def orders(n: int) -> None:
    group = app.group()
    for i in range(1, n + 1):
        await group.kiq(charge, f"A-{1000 + i}", 1500 + i * 100)
    print(f"  sent {n} charges as group {group.id[:8]}; results as they finish:")
    async for task, result in group.as_completed(timeout=30):
        print(f"    {task.task_id[:8]}  {show(result)}")
    print("  each success also ran the audit callback once (see the worker log)")


async def ship_all() -> None:
    plan = [
        ("ada", "A-1"),
        ("alan", "B-1"),
        ("ada", "A-2"),
        ("alan", "B-2"),
        ("ada", "A-3"),
        ("alan", "B-3"),
    ]
    group = app.group()
    for customer, order in plan:
        await group.kiq(ship, customer=customer, order_id=order)
    results = [r.return_value for r in await group.wait(timeout=30)]
    t0 = min(r["started"] for r in results)
    for customer in ("ada", "alan"):
        mine = sorted(
            (r for r in results if r["customer"] == customer),
            key=lambda r: r["started"],
        )
        timeline = "  ".join(
            f"{r['order']} {r['started'] - t0:.1f}-{r['ended'] - t0:.1f}s" for r in mine
        )
        overlap = any(b["started"] < a["ended"] for a, b in itertools.pairwise(mine))
        print(f"  {customer:5} {timeline}   overlapping: {overlap}")


async def partner(n: int) -> None:
    group = app.group()
    for i in range(n):
        await group.kiq(call_partner, i)
    spans = [r.return_value for r in await group.wait(timeout=60)]
    edges = sorted(
        [(s["started"], 1) for s in spans] + [(s["ended"], -1) for s in spans]
    )
    peak = running = 0
    for _, step in edges:
        running += step
        peak = max(peak, running)
    took = max(s["ended"] for s in spans) - min(s["started"] for s in spans)
    print(
        f"  {n} calls on all workers: at most {peak} at once (limit 3), {n / took:.1f}/s (limit 10/s)"
    )


async def call() -> None:
    print(f"  price('KB-01') = {await price.call('KB-01')}")
    try:
        await price.call("XX-99")
    except KeyError as exc:
        print(f"  price('XX-99') raised the task's own error: KeyError({exc})")


async def export() -> None:
    started = time.monotonic()
    result = await (await export_orders.kiq()).wait_result(timeout=30)
    print(f"  {show(result)}  (after {time.monotonic() - started:.1f}s)")


async def resize(megabytes: int) -> None:
    result = await (await resize_image.kiq(megabytes)).wait_result(timeout=60)
    print(f"  {megabytes} MiB in an isolated process: {show(result)}")


async def stream() -> None:
    task = await import_catalog.kiq(50)
    async for event in task.events(timeout=30):
        print(f"  {event.kind:9} {event.data if event.data is not None else ''}")
    print(f"  captured logs: syntasks logs {task.task_id}")


async def fair() -> None:
    group = app.group()
    for n in range(100):
        await group.kiq(send_email, tenant="big-corp", to=f"user{n}@big.example")
    for tenant in ("cafe", "bakery", "florist"):
        await group.kiq(send_email, tenant=tenant, to=f"owner@{tenant}.example")
    print("  sent 100 emails for big-corp, then 1 each for 3 small shops")
    position = 0
    async for _, result in group.as_completed(timeout=120):
        position += 1
        tenant = str(result.return_value).split(" ")[0]
        if tenant != "big-corp":
            print(f"  {tenant}'s email finished at position {position} of 103")
    print(
        "  with fair=True each tenant gets its turn; without it they'd wait for all 100"
    )


async def debounce() -> None:
    handles = [await reindex.kiq(sku="KB-01", version=v) for v in range(20)]
    last = await handles[-1].wait_result(timeout=10)
    replaced = 0
    for handle in handles[:-1]:
        if isinstance(
            (await handle.wait_result(timeout=10)).error, TaskSupersededError
        ):
            replaced += 1
    print(
        f"  20 edits: reindexed once, at version {last.return_value}; {replaced} calls were replaced"
    )


async def posts() -> None:
    await publish_post.schedules.cancel_all()  # start clean when run twice
    for owner, account, text, minutes in (
        ("user:ada", "ada-insta", "New keyboards are in!", 5),
        ("user:ada", "ada-insta", "Weekend sale", 60),
        ("user:alan", "alan-x", "Hello from the shop", 1),
    ):
        s = await publish_post.schedule(
            after=f"{minutes}m",
            kwargs={"account": account, "text": text},
            owner=owner,
            tags=["campaign:spring"],
            meta={"title": text},
        )
        print(f"  {owner} scheduled {s.meta['title']!r}: {s.describe()}")
    page = await publish_post.schedules.list(owner="user:ada")
    print(
        f"  user:ada has {len(page.items)} posts: {[s.meta['title'] for s in page.items]}"
    )
    try:
        for _ in range(2):
            await publish_post.schedule(
                after="2h",
                kwargs={"account": "ada-insta", "text": "more"},
                owner="user:ada",
            )
    except ScheduleLimitError as exc:
        print(f"  the 4th post is refused: {exc}")
    first = page.items[0]
    moved = await publish_post.schedules.update(
        first.id, version=first.version, after="10m"
    )
    print(
        f"  moved {first.meta['title']!r} to {moved.describe()} (version {moved.version})"
    )
    soon = await publish_post.schedule(
        after="1s", kwargs={"account": "alan-x", "text": "right now"}, owner="user:alan"
    )
    while (current := await app.schedules.get(soon.id)).last_status is None:
        await asyncio.sleep(0.2)
    print(
        f"  a post due in 1s ran: state={current.state}, status={current.last_status} (kept to show as sent)"
    )
    print(
        f"  paused the spring campaign: {await app.schedules.pause_all(tag='campaign:spring')} posts"
    )
    print(
        f"  cancelled everything of user:ada: {await app.schedules.cancel_all(owner='user:ada')} posts"
    )


async def campaign() -> None:
    batch = await app.batch(f"spring-{int(time.time())}", owner="team:marketing")
    await batch.pause()
    await batch.map(
        campaign_email,
        ({"to": f"customer{n}@example.com"} for n in range(40)),
        chunk=10,
    )
    await batch.then(campaign_report)
    await asyncio.sleep(1)
    print(f"  40 emails queued while paused: {await batch.progress()}")
    await batch.resume()
    await asyncio.sleep(0.4)
    print(f"  resumed for a moment: {await batch.progress()}")
    await batch.cancel()
    while (progress := await batch.progress()).pending != 0:
        await asyncio.sleep(0.3)
    print(f"  cancelled the rest: {progress}")
    print("  campaign_report ran once with the summary (see the worker log)")


async def live() -> None:
    try:
        async with app.channels.connect("sales:live", identity="guest:eve"):
            pass
    except ChannelDenied as exc:
        print(f"  guest:eve: {exc}")
    async with (
        app.channels.connect(
            "sales:live", identity="staff:alan", meta={"name": "Alan"}
        ) as alan,
        app.channels.connect(
            "sales:live", identity="staff:ada", meta={"name": "Ada"}
        ) as ada,
    ):
        present = await app.channels.presence("sales:live")
        print(f"  present: {sorted(p.identity for p in present)}")
        try:
            await alan.send("I'm not allowed to post")
        except ChannelDenied as exc:
            print(f"  staff:alan sends: {exc}")
        await ada.send("Flash sale until 18:00!")
        for _ in range(4):
            message = await alan.receive(timeout=10)
            if message is None:
                break
            print(f"  alan sees: {message.data}")
            if "announcement" in (message.data or {}):
                break


async def vendor_calls(*symbols: str) -> dict[str, int]:
    keys = [app.keys("demo", "vendor", s) for s in symbols]
    return {
        s: int(v or 0) for s, v in zip(symbols, await app.redis.mget(keys), strict=True)
    }


async def collect(watch: Any, seconds: float) -> dict[str, dict[str, Any]]:
    """The newest update of each channel that arrives in ``seconds``."""
    newest: dict[str, dict[str, Any]] = {}
    end = time.monotonic() + seconds
    while (left := end - time.monotonic()) > 0:
        update = await watch.receive(timeout=left)
        if update is not None and update.op == "data":
            newest[update.channel] = update.to_dict()
    return newest


async def watchlists() -> None:
    await app.redis.delete(
        *(app.keys("demo", "vendor", s) for s in ("AAPL", "MSFT", "NVDA"))
    )
    crowd = [app.feeds.watch(f"user:{n}") for n in range(4)]
    for watch in crowd:
        await watch.subscribe("main", ["quote:AAPL"])
    async with app.feeds.watch("user:ada") as tab1:
        ok, rejected = await tab1.subscribe(
            "watchlist", ["quote: aapl ", "quote:MSFT", "quote:NVDA", "bonds:US10Y"]
        )
        await tab1.subscribe("header", ["quote:AAPL"])
        print(f"  ada's watchlist: {ok}; rejected: {rejected}")
        seen = await collect(tab1, 2.0)
        prices = {c: u["data"]["price"] for c, u in sorted(seen.items())}
        print(f"  after 2s she has prices: {prices}")
        calls = await vendor_calls("AAPL", "MSFT", "NVDA")
        print(f"  vendor calls: {calls}  (AAPL has 5 watchers, MSFT and NVDA one each)")

        await tab1.unsubscribe("watchlist")
        print(f"  dropped her watchlist; still joined: {tab1.groups()}")
        await asyncio.sleep(3)  # the feed lingers for 2s after the last watcher
        before = await vendor_calls("AAPL", "MSFT", "NVDA")
        await asyncio.sleep(1.5)
        after = await vendor_calls("AAPL", "MSFT", "NVDA")
        grew = {s: after[s] - before[s] for s in after}
        print(f"  1.5s after the linger, vendor calls grew by {grew}")
        print(
            "  the header still holds AAPL; nobody watches MSFT and NVDA, so they stopped"
        )

    async with app.feeds.watch("user:ada") as tab2:
        print(f"  a new tab of ada: joined {tab2.groups()}, saved {await tab2.saved()}")
        restored, _ = await tab2.restore()
        update = await tab2.receive(timeout=2)
        assert update is not None
        print(
            f"  restore() joined {restored}; first update is the last value "
            f"(cached={update.cached}): {update.data}"
        )
    print(f"  feed health: {await app.feeds.states()}")
    for watch in crowd:
        await watch.close()


COMMANDS: dict[str, Any] = {
    "orders": lambda a: orders(a.n or 8),
    "ship": lambda a: ship_all(),
    "partner": lambda a: partner(a.n or 12),
    "call": lambda a: call(),
    "export": lambda a: export(),
    "resize": lambda a: resize(a.n or 50),
    "stream": lambda a: stream(),
    "fair": lambda a: fair(),
    "debounce": lambda a: debounce(),
    "posts": lambda a: posts(),
    "campaign": lambda a: campaign(),
    "live": lambda a: live(),
    "watchlists": lambda a: watchlists(),
}


async def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m shop",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("command", choices=sorted(COMMANDS))
    parser.add_argument("n", nargs="?", type=int)
    args = parser.parse_args()
    await app.startup()
    try:
        await COMMANDS[args.command](args)
    finally:
        await app.shutdown()


asyncio.run(main())
