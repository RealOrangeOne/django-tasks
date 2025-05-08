from django.apps import AppConfig


class TasksAppConfig(AppConfig):
    name = "django_tasks.backends.database"
    label = "django_tasks_database"
    verbose_name = "Tasks Database Backend"
