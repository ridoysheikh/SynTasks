"""Errors raised by the built-in Redis client."""


class RedisError(Exception):
    """Base class for all Redis client errors."""


class RedisConnectionError(RedisError, ConnectionError):
    """The connection to Redis failed or was lost."""


class RedisTimeoutError(RedisError, TimeoutError):
    """Redis did not answer in time."""


class ProtocolError(RedisError):
    """Redis sent data that doesn't follow the RESP protocol."""


class ResponseError(RedisError):
    """Redis answered a command with an error reply."""

    @property
    def code(self) -> str:
        """First word of the error message, e.g. ``WRONGTYPE`` or ``BUSYGROUP``."""
        return str(self).split(" ", 1)[0]


class AuthenticationError(ResponseError):
    """Redis rejected the credentials."""


class NoScriptError(ResponseError):
    """EVALSHA was called with an unknown script."""


class ReadOnlyError(ResponseError):
    """A write was sent to a read-only replica."""


class MasterNotFoundError(RedisConnectionError):
    """None of the Sentinels knows the requested master."""


_ERROR_CLASSES: dict[str, type[ResponseError]] = {
    "WRONGPASS": AuthenticationError,
    "NOAUTH": AuthenticationError,
    "NOPERM": AuthenticationError,
    "NOSCRIPT": NoScriptError,
    "READONLY": ReadOnlyError,
}


def make_response_error(message: str) -> ResponseError:
    """
    Build the right error class for an error reply.

    :param message: error message sent by Redis.
    :return: error instance.
    """
    code = message.split(" ", 1)[0]
    if code == "ERR" and "invalid password" in message:
        return AuthenticationError(message)
    return _ERROR_CLASSES.get(code, ResponseError)(message)
