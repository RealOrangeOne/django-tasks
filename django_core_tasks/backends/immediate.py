from inspect import iscoroutinefunction
from uuid import uuid4

from asgiref.sync import async_to_sync

from django_core_tasks.exceptions import InvalidTaskError
from django_core_tasks.task import Task, TaskResult, TaskStatus

from .base import BaseTaskBackend


class ImmediateBackend(BaseTaskBackend):
    def validate_task(self, task: Task):
        super().validate_task(task)

        if task.run_after is not None:
            raise InvalidTaskError("Immediate backend does not support run_after")

    def enqueue(self, task: Task, args: list, kwargs: dict) -> TaskResult:
        self.validate_task(task)

        calling_task_func = (
            async_to_sync(task.func) if iscoroutinefunction(task.func) else task.func
        )

        try:
            result = calling_task_func(*args, **kwargs)
            status = TaskStatus.COMPLETE
        except Exception as e:
            result = e
            status = TaskStatus.FAILED

        task = TaskResult(
            task=task,
            id=uuid4(),
            status=status,
            args=args,
            kwargs=kwargs,
            backend=self.alias,
        )

        task._result = result

        return task
