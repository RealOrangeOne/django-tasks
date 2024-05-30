from typing import Mapping, Optional, cast

from django.core import signals
from django.utils.connection import BaseConnectionHandler, ConnectionProxy
from django.utils.module_loading import import_string

from .backends.base import BaseTaskBackend
from .exceptions import InvalidTaskBackendError
from .task import DEFAULT_QUEUE_NAME, ResultStatus, Task, task

__version__ = "0.0.0"

__all__ = [
    "tasks",
    "DEFAULT_TASK_BACKEND_ALIAS",
    "DEFAULT_QUEUE_NAME",
    "task",
    "ResultStatus",
    "Task",
]

DEFAULT_TASK_BACKEND_ALIAS = "default"


class TasksHandler(BaseConnectionHandler[BaseTaskBackend]):
    settings_name = "TASKS"
    exception_class = InvalidTaskBackendError

    def configure_settings(self, settings: Optional[dict]) -> dict:
        try:
            return super().configure_settings(settings)
        except AttributeError:
            # HACK: Force a default task backend.
            # Can be replaced with `django.conf.global_settings` once vendored.
            return {
                "default": {
                    "BACKEND": "django_tasks.backends.immediate.ImmediateBackend"
                }
            }

    def create_connection(self, alias: str) -> BaseTaskBackend:
        params = self.settings[alias].copy()

        # Added back to allow a backend to self-identify
        params["ALIAS"] = alias

        backend = params["BACKEND"]

        try:
            backend_cls = import_string(backend)
        except ImportError as e:
            raise InvalidTaskBackendError(
                f"Could not find backend '{backend}': {e}"
            ) from e

        return backend_cls(params)  # type:ignore[no-any-return]


tasks = TasksHandler()

default_task_backend = ConnectionProxy(cast(Mapping, tasks), DEFAULT_TASK_BACKEND_ALIAS)


def close_task_backends(**kwargs: dict) -> None:
    tasks.close_all()


signals.request_finished.connect(close_task_backends)
