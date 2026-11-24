"""Schedule specification (what to run, when, and how) and its stored state."""

import hashlib
import json
import random
from dataclasses import dataclass, field
from typing import Any, Literal

from syntasks.scheduler.recurrence import (
    Recurrence,
    _iso,
    recurrence_from_dict,
)

__all__ = ["Schedule", "ScheduleSpec"]

Misfire = Literal["skip", "once", "all"]
Overlap = Literal["skip", "allow", "queue"]
_MISFIRE = ("skip", "once", "all")
_OVERLAP = ("skip", "allow", "queue")


@dataclass(kw_only=True)
class ScheduleSpec:
    """Everything that defines a schedule (not its runtime state)."""

    id: str
    task: str
    recurrences: list[Recurrence]
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    start_at_ms: int | None = None
    end_at_ms: int | None = None
    jitter_ms: int = 0
    misfire: Misfire = "once"
    misfire_grace_ms: int | None = None
    max_catchup: int | None = None
    overlap: Overlap = "skip"
    max_instances: int = 1
    timeout: float | None = None
    retries: int | None = None
    queue: str | None = None
    priority: int | None = None
    labels: dict[str, Any] = field(default_factory=dict)
    max_runs: int | None = None
    pause_after_failures: int | None = None

    def __post_init__(self) -> None:
        if not self.recurrences:
            raise ValueError(f"Schedule {self.id!r} has no recurrence.")
        if self.misfire not in _MISFIRE:
            raise ValueError(f"misfire must be one of {_MISFIRE}.")
        if self.overlap not in _OVERLAP:
            raise ValueError(f"overlap must be one of {_OVERLAP}.")
        if self.max_instances < 1:
            raise ValueError("max_instances must be at least 1.")
        if self.max_runs is not None and self.max_runs < 1:
            raise ValueError("max_runs must be at least 1.")
        if self.jitter_ms < 0:
            raise ValueError("jitter must not be negative.")

    def next_after(self, after_ms: int) -> int | None:
        """
        Next nominal fire time strictly after ``after_ms``.

        :param after_ms: Unix milliseconds.
        :return: Unix milliseconds, or None if it never fires again.
        """
        if self.start_at_ms is not None and after_ms < self.start_at_ms:
            after_ms = self.start_at_ms - 1
        candidates = [
            t
            for t in (r.next_after(after_ms) for r in self.recurrences)
            if t is not None
        ]
        if not candidates:
            return None
        found = min(candidates)
        if self.end_at_ms is not None and found > self.end_at_ms:
            return None
        return found

    def due_for(self, nominal_ms: int) -> int:
        """
        When a nominal fire time is actually dispatched (adds jitter).

        :param nominal_ms: nominal fire time.
        :return: dispatch time.
        """
        if not self.jitter_ms:
            return nominal_ms
        return nominal_ms + random.randint(0, self.jitter_ms)  # noqa: S311

    def describe(self) -> str:
        """Human description of when it fires."""
        return ", ".join(r.describe() for r in self.recurrences)

    def to_dict(self) -> dict[str, Any]:
        """Plain data (stored in Redis)."""
        return {
            "id": self.id,
            "task": self.task,
            "recurrences": [r.to_dict() for r in self.recurrences],
            "args": self.args,
            "kwargs": self.kwargs,
            "start_at_ms": self.start_at_ms,
            "end_at_ms": self.end_at_ms,
            "jitter_ms": self.jitter_ms,
            "misfire": self.misfire,
            "misfire_grace_ms": self.misfire_grace_ms,
            "max_catchup": self.max_catchup,
            "overlap": self.overlap,
            "max_instances": self.max_instances,
            "timeout": self.timeout,
            "retries": self.retries,
            "queue": self.queue,
            "priority": self.priority,
            "labels": self.labels,
            "max_runs": self.max_runs,
            "pause_after_failures": self.pause_after_failures,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ScheduleSpec":
        """
        Rebuild from :meth:`to_dict` data.

        :param data: plain data.
        :return: spec.
        """
        values = dict(data)
        values["recurrences"] = [recurrence_from_dict(r) for r in data["recurrences"]]
        known = cls.__dataclass_fields__
        return cls(**{k: v for k, v in values.items() if k in known})

    def to_json(self) -> str:
        """Canonical JSON."""
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    def fingerprint(self) -> str:
        """Hash of the definition (changes when anything in it changes)."""
        return hashlib.sha1(self.to_json().encode()).hexdigest()  # noqa: S324

    def timing_fingerprint(self) -> str:
        """Hash of only the timing (recurrence, bounds)."""
        timing = {
            "recurrences": [r.to_dict() for r in self.recurrences],
            "start_at_ms": self.start_at_ms,
            "end_at_ms": self.end_at_ms,
        }
        return hashlib.sha1(  # noqa: S324
            json.dumps(timing, sort_keys=True).encode(),
        ).hexdigest()


def _int(raw: bytes | None) -> int | None:
    if raw in (None, b""):
        return None
    return int(float(raw))  # type: ignore[arg-type]


def _text(raw: bytes | None) -> str | None:
    if raw in (None, b""):
        return None
    return raw.decode(errors="replace")  # type: ignore[union-attr]


@dataclass(kw_only=True)
class Schedule:
    """A schedule as stored in Redis: spec plus runtime state."""

    spec: ScheduleSpec
    source: str
    state: str
    version: int = 0
    next_ms: int | None = None
    last_fire_ms: int | None = None
    last_task_id: str | None = None
    last_status: str | None = None
    last_error: str | None = None
    runs: int = 0
    failures: int = 0
    created_ms: int | None = None
    updated_ms: int | None = None
    expires_ms: int | None = None
    invalid_reason: str | None = None

    @property
    def id(self) -> str:
        """Schedule id."""
        return self.spec.id

    @property
    def task(self) -> str:
        """Task name."""
        return self.spec.task

    def describe(self) -> str:
        """Human description of when it fires."""
        return self.spec.describe()

    @classmethod
    def from_hash(cls, data: dict[bytes, bytes]) -> "Schedule":
        """
        Build from a ``def:<id>`` hash.

        :param data: HGETALL reply.
        :return: schedule.
        """
        return cls(
            spec=ScheduleSpec.from_dict(json.loads(data[b"spec"])),
            source=_text(data.get(b"source")) or "api",
            state=_text(data.get(b"state")) or "active",
            version=_int(data.get(b"version")) or 0,
            next_ms=_int(data.get(b"next")),
            last_fire_ms=_int(data.get(b"last_fire")),
            last_task_id=_text(data.get(b"last_task_id")),
            last_status=_text(data.get(b"last_status")),
            last_error=_text(data.get(b"last_error")),
            runs=_int(data.get(b"runs")) or 0,
            failures=_int(data.get(b"failures")) or 0,
            created_ms=_int(data.get(b"created_at")),
            updated_ms=_int(data.get(b"updated_at")),
            expires_ms=_int(data.get(b"expires_at")),
            invalid_reason=_text(data.get(b"invalid_reason")),
        )

    def to_dict(self) -> dict[str, Any]:
        """Plain data for display."""
        return {
            "id": self.id,
            "task": self.task,
            "source": self.source,
            "state": self.state,
            "when": self.describe(),
            "version": self.version,
            "next": _iso(self.next_ms) if self.next_ms else None,
            "last_fire": _iso(self.last_fire_ms) if self.last_fire_ms else None,
            "last_task_id": self.last_task_id,
            "last_status": self.last_status,
            "last_error": self.last_error,
            "runs": self.runs,
            "failures": self.failures,
            "expires": _iso(self.expires_ms) if self.expires_ms else None,
            "invalid_reason": self.invalid_reason,
            "spec": self.spec.to_dict(),
        }
