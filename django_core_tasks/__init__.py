from django.core import signals
from django.utils.connection import BaseConnectionHandler, ConnectionProxy
from django.utils.module_loading import import_string

from .backends.base import BaseTaskBackend
from .exceptions import InvalidTaskBackendError
from .task import TaskStatus, task

__all__ = [
    "tasks",
    "DEFAULT_TASK_BACKEND_ALIAS",
    "BaseTaskBackend",
    "task",
    "TaskStatus",
]

DEFAULT_TASK_BACKEND_ALIAS = "default"


class TasksHandler(BaseConnectionHandler):
    settings_name = "TASKS"
    exception_class = InvalidTaskBackendError

    def create_connection(self, alias):
        params = self.settings[alias].copy()

        # Added back to allow a backend to self-identify
        params["ALIAS"] = alias

        backend = params.get("BACKEND")

        if backend is None:
            raise InvalidTaskBackendError(f"No backend specified for {alias}")

        try:
            backend_cls = import_string(backend)
        except ImportError as e:
            raise InvalidTaskBackendError(
                f"Could not find backend '{backend}': {e}"
            ) from e

        return backend_cls(params)


tasks = TasksHandler()

default_task_backend = ConnectionProxy(tasks, DEFAULT_TASK_BACKEND_ALIAS)


def close_task_backends(**kwargs):
    tasks.close_all()


signals.request_finished.connect(close_task_backends)
