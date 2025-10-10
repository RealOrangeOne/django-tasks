# ruff: noqa: E402
import django_stubs_ext

django_stubs_ext.monkeypatch()

import importlib.metadata

from django.utils.connection import BaseConnectionHandler, ConnectionProxy
from django.utils.module_loading import import_string

from .backends.base import BaseTaskBackend
from .base import (
    DEFAULT_TASK_BACKEND_ALIAS,
    DEFAULT_TASK_QUEUE_NAME,
    TaskContext,
    TaskResult,
    TaskResultStatus,
    task,
)
from .exceptions import InvalidTaskBackendError

__version__ = importlib.metadata.version(__name__)

__all__ = [
    "task_backends",
    "default_task_backend",
    "DEFAULT_TASK_BACKEND_ALIAS",
    "DEFAULT_TASK_QUEUE_NAME",
    "TaskResultStatus",
    "TaskResult",
    "TaskContext",
    "task",
]


class TaskBackendHandler(BaseConnectionHandler[BaseTaskBackend]):
    settings_name = "TASKS"
    exception_class = InvalidTaskBackendError

    def configure_settings(self, settings: dict | None) -> dict:
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


task_backends = TaskBackendHandler()

default_task_backend: BaseTaskBackend = ConnectionProxy(  # type:ignore[assignment]
    task_backends, DEFAULT_TASK_BACKEND_ALIAS
)
