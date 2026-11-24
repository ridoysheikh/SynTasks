"""
Cluster-scope settings: the same values on every machine.

On startup each worker publishes its cluster-scope settings (``[defaults]``,
``[tasks.*]``, ``[queues.*]``, ``[scheduler]``) to Redis together with
``[app] revision``. A worker whose revision is older than the stored one
(an old deploy restarting late) adopts the stored settings instead, so
machines never enforce different global limits. Without revisions the
last writer wins.
"""

import dataclasses
import json
import logging
from typing import Any

from syntasks.cluster.scripts import Script
from syntasks.config.loader import build
from syntasks.config.schema import (
    Config,
    QueueConfig,
    SchedulerConfig,
    TaskConfig,
    TaskDefaults,
)
from syntasks.redis.client import Redis

__all__ = ["cluster_settings", "publish_cluster_config"]

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


def cluster_settings(config: Config) -> dict[str, Any]:
    """
    The cluster-scope part of a config as plain data.

    :param config: config.
    :return: plain data.
    """
    return {
        "defaults": dataclasses.asdict(config.defaults),
        "tasks": {
            name: dataclasses.asdict(task) for name, task in config.tasks.items()
        },
        "queues": {
            name: dataclasses.asdict(queue) for name, queue in config.queues.items()
        },
        "scheduler": dataclasses.asdict(config.scheduler),
    }


def _apply(config: Config, data: dict[str, Any]) -> None:
    config.defaults = build(TaskDefaults, data["defaults"], "defaults")
    config.tasks = {
        name: build(TaskConfig, value, f"tasks.{name}")
        for name, value in data["tasks"].items()
    }
    config.queues = {
        name: build(QueueConfig, value, f"queues.{name}")
        for name, value in data["queues"].items()
    }
    local_enabled = config.scheduler.enabled
    config.scheduler = build(SchedulerConfig, data["scheduler"], "scheduler")
    config.scheduler.enabled = local_enabled


async def publish_cluster_config(
    redis: Redis,
    key: str,
    config: Config,
    node_id: str,
) -> bool:
    """
    Publish this node's cluster settings, or adopt newer stored ones.

    :param redis: client.
    :param key: config hash key.
    :param config: this node's config (updated in place when adopting).
    :param node_id: this node's id.
    :return: True if the stored settings were adopted.
    """
    mine = cluster_settings(config)
    encoded = json.dumps(mine, sort_keys=True, separators=(",", ":"))
    revision = "" if config.app.revision is None else str(config.app.revision)
    reply = await _PUBLISH(redis, [key], [revision, encoded, node_id])
    status = reply[0].decode()
    if status == "stale":
        stored = json.loads(reply[2])
        if stored != mine:
            logger.warning(
                "This node's config (revision %s) is older than the cluster's "
                "(revision %s); using the cluster's task, queue and scheduler "
                "settings.",
                revision or "none",
                reply[1].decode(),
            )
        _apply(config, stored)
        return True
    previous = reply[2]
    if previous and json.loads(previous) != mine and not revision:
        logger.warning(
            "Cluster settings changed with this node's start, and no [app] revision "
            "is set. During rolling deploys set a revision so old nodes don't "
            "override new settings.",
        )
    return False
