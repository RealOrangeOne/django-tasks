from functools import partial
from typing import Any, Iterable, TypeVar

from celery import shared_task
from celery.app import default_app
from celery.local import Proxy as CeleryTaskProxy
from django.apps import apps
from django.core.checks import ERROR, CheckMessage
from django.db import transaction
from django.utils import timezone
from kombu.utils.uuid import uuid
from typing_extensions import ParamSpec

from django_tasks.backends.base import BaseTaskBackend
from django_tasks.task import MAX_PRIORITY, MIN_PRIORITY, ResultStatus, TaskResult
from django_tasks.task import Task as BaseTask

if not default_app:
    from django_tasks.backends.celery.app import app as celery_app

    celery_app.set_default()


T = TypeVar("T")
P = ParamSpec("P")


CELERY_MIN_PRIORITY = 0
CELERY_MAX_PRIORITY = 9


def _map_priority(value: int) -> int:
    # linear scale value to the range 0 to 9
    scaled_value = (value + abs(MIN_PRIORITY)) / (
        (MAX_PRIORITY - MIN_PRIORITY) / (CELERY_MAX_PRIORITY - CELERY_MIN_PRIORITY)
    )
    mapped_value = int(scaled_value)

    # ensure the mapped value is within the range 0 to 9
    if mapped_value < CELERY_MIN_PRIORITY:
        mapped_value = CELERY_MIN_PRIORITY
    elif mapped_value > CELERY_MAX_PRIORITY:
        mapped_value = CELERY_MAX_PRIORITY

    return mapped_value


class Task(BaseTask[P, T]):
    celery_task: CeleryTaskProxy = None
    """Celery proxy to the task in the current celery app task registry."""

    def __post_init__(self) -> None:
        celery_task = shared_task()(self.func)
        self.celery_task = celery_task
        return super().__post_init__()


class CeleryBackend(BaseTaskBackend):
    task_class = Task
    supports_defer = True

    def enqueue(
        self,
        task: Task[P, T],  # type: ignore[override]
        args: P.args,
        kwargs: P.kwargs,
    ) -> TaskResult[T]:
        self.validate_task(task)

        apply_async_kwargs: P.kwargs = {
            "eta": task.run_after,
        }
        if task.queue_name:
            apply_async_kwargs["queue"] = task.queue_name
        if task.priority is not None:
            # map priority to the range 0 to 9
            priority = _map_priority(task.priority)
            apply_async_kwargs["priority"] = priority

        task_id = uuid()
        apply_async_kwargs["task_id"] = task_id

        if self._get_enqueue_on_commit_for_task(task):
            transaction.on_commit(
                partial(
                    task.celery_task.apply_async,
                    args,
                    kwargs=kwargs,
                    **apply_async_kwargs,
                )
            )
        else:
            task.celery_task.apply_async(args, kwargs=kwargs, **apply_async_kwargs)

        # TODO: send task_enqueued signal
        # TODO: link a task to trigger the task_finished signal?
        # TODO: consider using DBTaskResult for results?

        # TODO: a Celery result backend is required to get additional information
        task_result = TaskResult[T](
            task=task,
            id=task_id,
            status=ResultStatus.NEW,
            enqueued_at=timezone.now(),
            started_at=None,
            finished_at=None,
            args=args,
            kwargs=kwargs,
            backend=self.alias,
        )
        return task_result

    def check(self, **kwargs: Any) -> Iterable[CheckMessage]:
        backend_name = self.__class__.__name__

        if not apps.is_installed("django_tasks.backends.celery"):
            yield CheckMessage(
                ERROR,
                f"{backend_name} configured as django_tasks backend, but celery app not installed",
                "Insert 'django_tasks.backends.celery' in INSTALLED_APPS",
            )
