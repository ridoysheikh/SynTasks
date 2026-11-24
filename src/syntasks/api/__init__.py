"""
Run a receiver programmatically, for generic brokers.

SynTasks apps use ``await app.run_worker(stop_event)`` instead.
"""

from syntasks.api.receiver import run_receiver_task

__all__ = ["run_receiver_task"]
