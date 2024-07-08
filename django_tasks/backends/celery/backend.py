from dataclasses import dataclass
from typing import TypeVar

from celery import shared_task
from celery.app import default_app
from celery.local import Proxy as CeleryTaskProxy
from django.utils import timezone
from typing_extensions import ParamSpec

from django_tasks.backends.base import BaseTaskBackend
from django_tasks.task import ResultStatus, TaskResult
from django_tasks.task import Task as BaseTask
from django_tasks.utils import json_normalize

if not default_app:
    from django_tasks.backends.celery.app import app as celery_app

    celery_app.set_default()


T = TypeVar("T")
P = ParamSpec("P")


@dataclass
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
        if task.priority:
            apply_async_kwargs["priority"] = task.priority

        # TODO: a Celery result backend is required to get additional information
        async_result = task.celery_task.apply_async(
            args, kwargs=kwargs, **apply_async_kwargs
        )
        task_result = TaskResult[T](
            task=task,
            id=async_result.id,
            status=ResultStatus.NEW,
            enqueued_at=timezone.now(),
            finished_at=None,
            args=json_normalize(args),
            kwargs=json_normalize(kwargs),
            backend=self.alias,
        )
        return task_result
