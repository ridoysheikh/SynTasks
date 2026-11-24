"""Live sales channel: one handler for the whole cluster, with authorization."""

from syntasks import Channel, ConnectRequest, Module

module = Module("live", timeout=None, retries=5, max_tasks=10)
STAFF = {"staff:ada", "staff:alan"}


def staff_only(request: ConnectRequest) -> bool:
    """Anyone on the staff may watch; only Ada may post announcements."""
    if request.action == "send":
        return request.identity == "staff:ada"
    return request.identity in STAFF


@module.channel("sales:live", authorize=staff_only, idle_timeout="5m")
async def live_sales(ch: Channel) -> None:
    async for message in ch.inbox():
        if message.kind in ("join", "leave"):
            await ch.publish({"event": message.kind, **(message.data or {})})
            continue
        ch.state["messages"] = ch.state.get("messages", 0) + 1
        await ch.save_state()
        await ch.publish(
            {"announcement": message.data, "n": ch.state["messages"], "by": ch.consumer}
        )
