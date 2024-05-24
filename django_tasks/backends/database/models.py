import uuid
from typing import TYPE_CHECKING, Any

from django.db import models
from django.utils.functional import cached_property
from django.utils.module_loading import import_string

from django_tasks.task import ResultStatus, Task

if TYPE_CHECKING:
    from .backend import TaskResult


class DBTaskResult(models.Model):
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

    @cached_property
    def task(self) -> Task:
        task = import_string(self.task_path)

        assert isinstance(task, Task)

        return task.using(
            priority=self.priority,
            queue_name=self.queue_name,
            run_after=self.run_after,
            backend=self.backend_name,
        )

    def get_task_result(self) -> "TaskResult":
        from .backend import TaskResult

        result = TaskResult[Any](
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
