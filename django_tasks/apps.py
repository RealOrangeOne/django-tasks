from django.apps import AppConfig
from django.core import checks


class TasksAppConfig(AppConfig):
    name = "django_tasks"

    def ready(self) -> None:
        from . import signal_handlers  # noqa
        from .checks import check_tasks

        checks.register(check_tasks)
