"""
Cluster agreement: every machine enforces the same limits.

On startup each worker publishes a manifest: the effective options of every
task declared in its code, its declared queues and its ``[scheduler]``
settings, with ``[app] revision``. A worker whose revision is older than the
stored one (an old deploy restarting late) adopts the stored numbers for the
tasks and queues it also has, so machines never enforce different global
limits. Without revisions the last writer wins.
"""

import dataclasses
import json
import logging
from typing import TYPE_CHECKING, Any

from syntasks.cluster.scripts import Script
from syntasks.config.loader import build
from syntasks.config.schema import QueueConfig, SchedulerConfig
from syntasks.units import Rate

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.app import SynTasks
    from syntasks.options import TaskOptions

__all__ = ["manifest", "publish_cluster_config"]

logger = logging.getLogger("syntasks.config")

_PUBLISH = Script(
    "config_publish",
    """
-- KEYS[1] config hash; ARGV[2] revision|'', ARGV[3] json, ARGV[4] node
local stored = redis.call('HGET', KEYS[1], 'revision')
if stored and stored ~= '' and (ARGV[2] == '' or tonumber(stored) > tonumber(ARGV[2])) then
  return {'stale', stored, redis.call('HGET', KEYS[1], 'json')}
end
local previous = redis.call('HGET', KEYS[1], 'json') or ''
redis.call('HSET', KEYS[1], 'revision', ARGV[2], 'json', ARGV[3], 'node', ARGV[4], 'at', tostring(now_ms()))
return {'ok', ARGV[2], previous}
""",  # noqa: E501
)


def _options_data(options: "TaskOptions") -> dict[str, Any]:
    data = dataclasses.asdict(options)
    data["tags"] = list(options.tags)
    return data


def _options_from(data: dict[str, Any]) -> "TaskOptions":
    from syntasks.options import TaskOptions  # noqa: PLC0415

    values = dict(data)
    values["tags"] = tuple(values.get("tags") or ())
    rate = values.get("rate")
    values["rate"] = Rate(**rate) if rate else None
    known = {f.name for f in dataclasses.fields(TaskOptions)}
    return TaskOptions(**{k: v for k, v in values.items() if k in known})


def manifest(app: "SynTasks") -> dict[str, Any]:
    """
    What this node's code declares, as plain data.

    :param app: the app.
    :return: tasks' effective options, queues and scheduler settings.
    """
    return {
        "tasks": {
            name: _options_data(app.options_for(name))
            for name in sorted(app.declared_tasks)
        },
        "queues": {
            name: dataclasses.asdict(options)
            for name, options in sorted(app.declared_queues.items())
        },
        "scheduler": dataclasses.asdict(app.config.scheduler),
    }


def _adopt(app: "SynTasks", data: dict[str, Any]) -> None:
    for name, options in data.get("tasks", {}).items():
        if name in app.declared_tasks:
            app.adopted_options[name] = _options_from(options)
    for name, options in data.get("queues", {}).items():
        if name in app.declared_queues:
            app.declared_queues[name] = build(QueueConfig, options, f"queue {name}")
    local_enabled = app.config.scheduler.enabled
    app.config.scheduler = build(SchedulerConfig, data["scheduler"], "scheduler")
    app.config.scheduler.enabled = local_enabled


async def publish_cluster_config(app: "SynTasks") -> bool:
    """
    Publish this node's manifest, or adopt a newer stored one.

    :param app: the app (updated in place when adopting).
    :return: True if the stored manifest was adopted.
    """
    mine = manifest(app)
    encoded = json.dumps(mine, sort_keys=True, separators=(",", ":"))
    revision = "" if app.config.app.revision is None else str(app.config.app.revision)
    reply = await _PUBLISH(
        app.redis,
        [app.keys("cfg", "cluster")],
        [revision, encoded, app.cluster.node_id],
    )
    status = reply[0].decode()
    if status == "stale":
        stored = json.loads(reply[2])
        if stored != mine:
            logger.warning(
                "This node's code (revision %s) is older than the cluster's "
                "(revision %s); using the cluster's task, queue and scheduler "
                "limits.",
                revision or "none",
                reply[1].decode(),
            )
        _adopt(app, stored)
        return True
    previous = reply[2]
    if previous and json.loads(previous) != mine and not revision:
        logger.warning(
            "Task, queue or scheduler limits changed with this node's start, and "
            "no [app] revision is set. During rolling deploys set a revision so "
            "old nodes don't override new limits.",
        )
    return False
