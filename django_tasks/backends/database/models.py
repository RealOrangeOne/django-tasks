import uuid
from typing import TYPE_CHECKING, Generic, TypeVar

from django.db import models
from django.utils.module_loading import import_string
from typing_extensions import ParamSpec

from django_tasks.task import ResultStatus, Task

T = TypeVar("T")
P = ParamSpec("P")

if TYPE_CHECKING:
    from .backend import TaskResult


class DBTaskResult(Generic[P, T], models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    status = models.CharField(
        choices=ResultStatus.choices,
        default=ResultStatus.NEW,
        max_length=max(len(value) for value in ResultStatus.values),
    )

    args_kwargs = models.JSONField()

    priority = models.PositiveSmallIntegerField(null=True)

    task_path = models.TextField()

    queue_name = models.TextField()
    backend_name = models.TextField()

    run_after = models.DateTimeField(null=True)

    result = models.JSONField(default=None, null=True)

    @property
    def task(self) -> Task[P, T]:
        task = import_string(self.task_path)

        assert isinstance(task, Task)

        return task.using(
            priority=self.priority,
            queue_name=self.queue_name,
            run_after=self.run_after,
            backend=self.backend_name,
        )

    @property
    def task_result(self) -> "TaskResult[T]":
        from .backend import TaskResult

        result = TaskResult[T](
            db_result=self,
            task=self.task,
            id=str(self.id),
            status=ResultStatus[self.status],
            args=self.args_kwargs["args"],
            kwargs=self.args_kwargs["kwargs"],
            backend=self.backend_name,
        )

        result._result = self.result

        return result
