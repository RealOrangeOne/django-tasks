from typing import TypeVar

from celery import shared_task
from celery.app import default_app
from celery.local import Proxy as CeleryTaskProxy
from typing_extensions import ParamSpec

from django_tasks.backends.base import BaseTaskBackend
from django_tasks.task import Task, TaskResult

if not default_app:
    from django_tasks.backends.celery.app import app as celery_app
    celery_app.set_default()


T = TypeVar("T")
P = ParamSpec("P")


class CeleryTask(Task):

    celery_task: CeleryTaskProxy
    """Celery proxy to the task in the current celery app task registry."""

    def __post_init__(self) -> None:
        # TODO: allow passing extra celery specific parameters?
        celery_task = shared_task()(self.func)
        self.celery_task = celery_task
        return super().__post_init__()


class CeleryBackend(BaseTaskBackend):
    task_class = CeleryTask
    supports_defer = True

    def enqueue(
        self, task: Task[P, T], args: P.args, kwargs: P.kwargs
    ) -> TaskResult[T]:
        self.validate_task(task)

        apply_async_kwargs = {
            "eta": task.run_after,
        }
        if task.queue_name:
            apply_async_kwargs["queue"] = task.queue_name
        if task.priority:
            apply_async_kwargs["priority"] = task.priority
        task.celery_task.apply_async(args, kwargs=kwargs, **apply_async_kwargs)
