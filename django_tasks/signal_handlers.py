import logging
from typing import Type

from asgiref.local import Local
from django.core.signals import setting_changed
from django.dispatch import receiver

from django_tasks import BaseTaskBackend, ResultStatus, TaskResult

from .signals import task_enqueued, task_finished

logger = logging.getLogger("django_tasks")


@receiver(setting_changed)
def clear_tasks_handlers(*, setting: str, **kwargs: dict) -> None:
    """
    Reset the connection handler whenever the settings change
    """
    if setting == "TASKS":
        from django_tasks import tasks

        tasks._settings = tasks.settings = tasks.configure_settings(None)  # type:ignore[attr-defined]
        tasks._connections = Local()  # type:ignore[attr-defined]


@receiver(task_enqueued)
def log_task_enqueued(
    sender: Type[BaseTaskBackend], task_result: TaskResult, **kwargs: dict
) -> None:
    logger.debug(
        "Task id=%s path=%s enqueued backend=%s",
        task_result.id,
        task_result.task.module_path,
        task_result.backend,
    )


@receiver(task_finished)
def log_task_finished(
    sender: Type[BaseTaskBackend], task_result: TaskResult, **kwargs: dict
) -> None:
    if task_result.status == ResultStatus.FAILED:
        # Use `.exception` to integrate with error monitoring tools (eg Sentry)
        log_method = logger.exception
    else:
        log_method = logger.info

    log_method(
        "Task id=%s path=%s state=%s",
        task_result.id,
        task_result.task.module_path,
        task_result.status,
    )
