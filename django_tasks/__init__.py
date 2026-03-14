# ruff: noqa: E402
from typing import TYPE_CHECKING

_USE_DJANGO_TASKS = False
if not TYPE_CHECKING:
    try:
        from django.tasks import (  # noqa: F401
            DEFAULT_TASK_BACKEND_ALIAS,
            DEFAULT_TASK_QUEUE_NAME,
            TaskContext,
            TaskResult,
            TaskResultStatus,
            default_task_backend,
            task,
            task_backends,
        )
        from django.tasks.backends.base import BaseTaskBackend  # noqa: F401
        from django.tasks.base import (  # noqa: F401
            TASK_MAX_PRIORITY,
            TASK_MIN_PRIORITY,
            Task,
        )

        _USE_DJANGO_TASKS = True
    except ImportError:
        pass

if not _USE_DJANGO_TASKS:
    import django_stubs_ext

    django_stubs_ext.monkeypatch()

    import importlib.metadata

    from django.conf import global_settings
    from django.utils.connection import BaseConnectionHandler, ConnectionProxy
    from django.utils.module_loading import import_string

    from .backends.base import BaseTaskBackend
    from .base import (
        DEFAULT_TASK_BACKEND_ALIAS,
        DEFAULT_TASK_QUEUE_NAME,
        TASK_MAX_PRIORITY,
        TASK_MIN_PRIORITY,
        Task,
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
        "TASK_MAX_PRIORITY",
        "TASK_MIN_PRIORITY",
        "Task",
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
                task_settings = super().configure_settings(settings)
            except AttributeError:
                # HACK: Force a default task backend.
                # Can be replaced with `django.conf.global_settings` once vendored.
                task_settings = None

            if task_settings is None or task_settings is getattr(
                global_settings, self.settings_name, None
            ):
                task_settings = {
                    DEFAULT_TASK_BACKEND_ALIAS: {
                        "BACKEND": "django_tasks.backends.immediate.ImmediateBackend"
                    }
                }

            return task_settings

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
