from django.apps import AppConfig
from django.core import checks

from django_tasks.checks import check_tasks


class TasksAppConfig(AppConfig):
    name = "django_tasks"

    def ready(self) -> None:
        from . import signal_handlers  # noqa

        checks.register(check_tasks)
