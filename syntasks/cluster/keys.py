"""Redis key naming for one SynTasks app."""

import re

__all__ = ["Keys", "validate_name"]

_NAME = re.compile(r"^[A-Za-z0-9_.\-]+$")


def validate_name(name: str, what: str = "name") -> str:
    """
    Check that a name is safe to use inside Redis keys.

    :param name: app, queue or task-group name.
    :param what: what the name is, for error messages.
    :raises ValueError: if the name has characters outside ``[A-Za-z0-9_.-]``.
    :return: the name.
    """
    if not _NAME.match(name):
        raise ValueError(
            f"Invalid {what} {name!r}: use letters, digits, '_', '.' and '-'.",
        )
    return name


class Keys:
    """
    Builds the keys of one app: ``syntasks:{<app>}:<part>:<part>…``.

    The app name is wrapped in a hash tag, so all keys of one app map to
    the same slot if the app ever runs on Redis Cluster.
    """

    def __init__(self, app: str) -> None:
        self.app = validate_name(app, "app name")
        self.base = f"syntasks:{{{app}}}"

    def __call__(self, *parts: str) -> str:
        """
        Join key parts under the app prefix.

        :param parts: key parts.
        :return: full key.
        """
        return ":".join((self.base, *parts))

    def __repr__(self) -> str:
        return f"Keys({self.app!r})"
