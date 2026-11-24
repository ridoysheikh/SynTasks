import enum


@enum.unique
class SynTasksEvents(enum.Enum):
    """List of syntasks broker lifetime events."""

    # Worker events.

    # Called on worker startup.
    WORKER_STARTUP = "WORKER_STARTUP"
    # Called o worker shutdown.
    WORKER_SHUTDOWN = "WORKER_SHUTDOWN"

    # Client events.

    # Called when startup function is called from the client's code.
    CLIENT_STARTUP = "CLIENT_STARTUP"
    # Called if shutdown function was called from the client's code.
    CLIENT_SHUTDOWN = "CLIENT_SHUTDOWN"
