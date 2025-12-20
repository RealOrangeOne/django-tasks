from django.apps import AppConfig
from django.utils.translation import pgettext_lazy


class TasksAppConfig(AppConfig):
    name = "django_tasks.backends.database"
    label = "django_tasks_database"
    verbose_name = pgettext_lazy("Database Backend", "Tasks Database Backend")

    def ready(self) -> None:
        from . import signal_handlers  # noqa
