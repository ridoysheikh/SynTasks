import contextlib
import enum
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from syntasks.codec import convert, to_jsonable
from syntasks.context import Context
from syntasks.dependencies import Depends

_ProgressType = TypeVar("_ProgressType")


class TaskState(enum.StrEnum):
    """State of task execution."""

    STARTED = "STARTED"
    FAILURE = "FAILURE"
    SUCCESS = "SUCCESS"
    RETRY = "RETRY"


@dataclass(kw_only=True)
class TaskProgress(Generic[_ProgressType]):
    """Progress of task execution."""

    state: TaskState | str
    meta: _ProgressType | None

    def __post_init__(self) -> None:
        if isinstance(self.state, TaskState):
            return
        if not isinstance(self.state, str):
            raise ValueError(
                f"Progress state must be a string, got {type(self.state).__name__}.",
            )
        with contextlib.suppress(ValueError):
            self.state = TaskState(self.state)

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to plain, JSON-compatible data.

        :return: progress as a dict.
        """
        return {"state": to_jsonable(self.state), "meta": to_jsonable(self.meta)}

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        meta_type: Any = None,
    ) -> "TaskProgress[Any]":
        """
        Build from plain data.

        :param data: progress as a dict.
        :param meta_type: optional type to convert ``meta`` to.
        :raises ValueError: if the data is not a valid progress.
        :return: progress.
        """
        if not isinstance(data, Mapping) or "state" not in data:
            raise ValueError("Progress must be a mapping with a 'state' key.")
        meta = data.get("meta")
        if meta_type is not None and meta is not None:
            meta = convert(meta, meta_type)
        return cls(state=data["state"], meta=meta)


class ProgressTracker(Generic[_ProgressType]):
    """Task's dependency to set progress."""

    def __init__(
        self,
        context: Context = Depends(),
    ) -> None:
        self.context = context

    async def set_progress(
        self,
        state: TaskState | str,
        meta: _ProgressType | None = None,
    ) -> None:
        """Set progress.

        :param state: TaskState or str
        :param meta: progress data
        """
        if meta is None:
            progress = await self.get_progress()
            meta = progress.meta if progress else None

        progress = TaskProgress(
            state=state,
            meta=meta,
        )

        await self.context.broker.result_backend.set_progress(
            self.context.message.task_id,
            progress,
        )

    async def get_progress(self) -> TaskProgress[_ProgressType] | None:
        """Get progress."""
        return await self.context.broker.result_backend.get_progress(
            self.context.message.task_id,
        )
