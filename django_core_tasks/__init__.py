from django.utils.connection import BaseConnectionHandler, ConnectionProxy
from django.utils.module_loading import import_string
from django.core import signals
from .backends.base import BaseTaskBackend
from .task import TaskStatus
from .exceptions import InvalidTaskBackendError
from .utils import task_function

__all__ = [
    "tasks",
    "DEFAULT_TASK_BACKEND_ALIAS",
    "BaseTaskBackend",
    "TaskStatus",
    "task_function",
]

DEFAULT_TASK_BACKEND_ALIAS = "default"


class TasksHandler(BaseConnectionHandler):
    settings_name = "TASKS"
    exception_class = InvalidTaskBackendError

    def create_connection(self, alias):
        params = self.settings[alias].copy()

        backend = params.get("BACKEND")

        if backend is None:
            raise InvalidTaskBackendError(f"No backend specified for {alias}")

        options = params.get("OPTIONS", {})

        try:
            backend_cls = import_string(backend)
        except ImportError as e:
            raise InvalidTaskBackendError(
                f"Could not find backend '{backend}': {e}"
            ) from e

        return backend_cls(options)


tasks = TasksHandler()

default_task_backend = ConnectionProxy(tasks, DEFAULT_TASK_BACKEND_ALIAS)


def close_task_backends(**kwargs):
    tasks.close_all()


signals.request_finished.connect(close_task_backends)


async def aenqueue(func, *, priority=None, args=None, kwargs=None):
    """
    Queue up a task function (or coroutine) to be executed
    """
    return await default_task_backend.aenqueue(
        func, priority=priority, args=args, kwargs=kwargs
    )


async def adefer(func, *, when, priority=None, args=None, kwargs=None):
    """
    Add a task function (or coroutine) to be completed at a specific time
    """
    return await default_task_backend.adefer(
        func, priority=priority, when=when, args=args, kwargs=kwargs
    )


def enqueue(func, *, priority=None, args=None, kwargs=None):
    """
    Queue up a task function (or coroutine) to be executed
    """
    return default_task_backend.defer(func, priority=priority, args=args, kwargs=kwargs)


def defer(func, *, when, priority=None, args=None, kwargs=None):
    """
    Add a task function (or coroutine) to be completed at a specific time
    """
    return default_task_backend.defer(
        func, priority=priority, when=when, args=args, kwargs=kwargs
    )
