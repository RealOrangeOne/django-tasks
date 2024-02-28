from django.dispatch import receiver
from django.core.signals import setting_changed
from asgiref.local import Local


@receiver(setting_changed)
def clear_tasks_handlers(*, setting, **kwargs):
    """
    Reset the connection handler whenever the settings change
    """
    if setting == "TASKS":
        from django_core_tasks import tasks, close_task_backends

        close_task_backends()
        tasks._settings = tasks.settings = tasks.configure_settings(None)
        tasks._connections = Local()
