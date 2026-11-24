"""
Package to run some syntasks functions programmatically.

This package is useful for managing dynamically created brokers and
scheduler instances.
"""

from syntasks.api.receiver import run_receiver_task
from syntasks.api.scheduler import run_scheduler_task

__all__ = ["run_receiver_task", "run_scheduler_task"]
