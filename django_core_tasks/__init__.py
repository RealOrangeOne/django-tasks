from django.utils.connection import BaseConnectionHandler, ConnectionProxy
from django.utils.module_loading import import_string
from django.core import signals
from .backends.base import BaseTaskBackend

__all__ = ["tasks", "DEFAULT_TASK_BACKEND_ALIAS", "BaseTaskBackend"]

DEFAULT_TASK_BACKEND_ALIAS = "default"


class TasksHandler(BaseConnectionHandler):
    settings_name = "TASKS"

    def create_connection(self, alias):
        params = self.settings[alias]
        backend = params["BACKEND"]
        options = params.get("OPTIONS", {})
        backend_cls = import_string(backend)
        return backend_cls(options)


tasks = TasksHandler()

default_task_backend = ConnectionProxy(tasks, DEFAULT_TASK_BACKEND_ALIAS)


def close_task_backends(**kwargs):
    tasks.close_all()


signals.request_finished.connect(close_task_backends)


async def aenqueue(self, func, *, priority, args, kwargs):
    """
    Queue up a task function (or coroutine) to be executed
    """
    return await default_task_backend.aenqueue(func, priority, args, kwargs)


async def adefer(self, func, *, priority, when, args, kwargs):
    """
    Add a task function (or coroutine) to be completed at a specific time
    """
    return await default_task_backend.adefer(func, priority, when, args, kwargs)


def enqueue(self, func, *, priority, args=None, kwargs=None):
    """
    Queue up a task function to be executed
    """
    return default_task_backend.defer(func, priority, args, kwargs)


def defer(self, func, *, priority, when, args=None, kwargs=None):
    """
    Add a task to be completed at a specific time
    """
    return default_task_backend.defer(func, priority, when, args, kwargs)
