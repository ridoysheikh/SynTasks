import json
import pickle
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Generic, Self, TypeVar

from syntasks.codec import convert, to_jsonable
from syntasks.serialization import (
    ExceptionRepr,
    exception_to_python,
    prepare_exception,
)

_ReturnType = TypeVar("_ReturnType")


@dataclass(kw_only=True)
class SynTasksResult(Generic[_ReturnType]):
    """Result of a remote task invocation."""

    is_err: bool
    # Log is a deprecated field. It would be removed in future releases of not,
    # if we find a way to capture logs in async environment.
    log: str | None = None
    return_value: _ReturnType
    execution_time: float
    labels: dict[str, Any] = field(default_factory=dict)

    error: BaseException | None = None

    def __post_init__(self) -> None:
        if self.error is not None and not isinstance(self.error, BaseException):
            self.error = exception_to_python(self.error)

    def raise_for_error(self) -> Self:
        """Raise exception if `error`.

        :raises error: task execution exception
        :returns: SynTasksResult
        """
        if self.error is not None:
            raise self.error
        return self

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the result to plain, JSON-compatible data.

        The error is encoded with :func:`prepare_exception`,
        so it can be restored by :meth:`from_dict`.

        :return: result as a dict.
        """
        error: Any = None
        if self.error is not None:
            prepared = prepare_exception(self.error, json)
            error = prepared.to_dict() if isinstance(prepared, ExceptionRepr) else None
        return {
            "is_err": self.is_err,
            "log": self.log,
            "return_value": to_jsonable(self.return_value),
            "execution_time": self.execution_time,
            "labels": to_jsonable(self.labels),
            "error": error,
        }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        return_type: Any = None,
    ) -> "SynTasksResult[Any]":
        """
        Build a result from plain data.

        :param data: decoded result.
        :param return_type: optional type to convert the return value to.
        :raises ValueError: if the data is not a valid result.
        :return: parsed result.
        """
        if not isinstance(data, Mapping):
            raise ValueError(f"Result must be a mapping, got {type(data).__name__}.")
        try:
            return_value = data["return_value"]
            if return_type is not None:
                return_value = convert(return_value, return_type)
            return cls(
                is_err=bool(data["is_err"]),
                log=data.get("log"),
                return_value=return_value,
                execution_time=float(data["execution_time"]),
                labels=dict(data.get("labels") or {}),
                error=exception_to_python(data.get("error")),
            )
        except KeyError as exc:
            raise ValueError(f"Result is missing the required field {exc}.") from exc
        except TypeError as exc:
            raise ValueError(f"Invalid result data: {exc}") from exc

    def __getstate__(self) -> dict[str, Any]:
        state = dict(self.__dict__)
        if state.get("error") is not None:
            state["error"] = prepare_exception(state["error"], pickle)
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        error = state.get("error")
        if error is not None:
            state = {**state, "error": exception_to_python(error)}
        self.__dict__.update(state)
