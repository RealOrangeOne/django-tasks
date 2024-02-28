from django.apps import AppConfig


class TasksAppConfig(AppConfig):
    name = "django_core_tasks"

    def ready(self):
        from . import signal_handlers  # noqa
