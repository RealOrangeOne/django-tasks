# ruff: noqa: E402
import django_stubs_ext

django_stubs_ext.monkeypatch()

import importlib.metadata
from typing import Optional

from django.utils.connection import BaseConnectionHandler, ConnectionProxy
from django.utils.module_loading import import_string

from .backends.base import BaseTaskBackend
from .exceptions import InvalidTaskBackendError
from .task import (
    DEFAULT_QUEUE_NAME,
    DEFAULT_TASK_BACKEND_ALIAS,
    ResultStatus,
    Task,
    TaskContext,
    TaskResult,
    task,
)

__version__ = importlib.metadata.version(__name__)

__all__ = [
    "tasks",
    "default_task_backend",
    "DEFAULT_TASK_BACKEND_ALIAS",
    "DEFAULT_QUEUE_NAME",
    "task",
    "ResultStatus",
    "Task",
    "TaskResult",
    "TaskContext",
]


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
                DEFAULT_TASK_BACKEND_ALIAS: {
                    "BACKEND": "django_tasks.backends.immediate.ImmediateBackend"
                }
            }

    def create_connection(self, alias: str) -> BaseTaskBackend:
        params = self.settings[alias]

        backend = params["BACKEND"]

        try:
            backend_cls = import_string(backend)
        except ImportError as e:
            raise InvalidTaskBackendError(
                f"Could not find backend '{backend}': {e}"
            ) from e

        return backend_cls(alias=alias, params=params)  # type:ignore[no-any-return]


tasks = TasksHandler()

default_task_backend: BaseTaskBackend = ConnectionProxy(  # type:ignore[assignment]
    tasks, DEFAULT_TASK_BACKEND_ALIAS
)
