from inspect import iscoroutinefunction
from typing import TypeVar
from uuid import uuid4

from asgiref.sync import async_to_sync
from typing_extensions import ParamSpec

from django_tasks.task import ResultStatus, Task, TaskResult

from .base import BaseTaskBackend

T = TypeVar("T")
P = ParamSpec("P")


class ImmediateBackend(BaseTaskBackend):
    supports_async_task = True

    def enqueue(
        self, task: Task[P, T], args: P.args, kwargs: P.kwargs
    ) -> TaskResult[T]:
        self.validate_task(task)

        calling_task_func = (
            async_to_sync(task.func) if iscoroutinefunction(task.func) else task.func
        )

        try:
            result = calling_task_func(*args, **kwargs)
            status = ResultStatus.COMPLETE
        except Exception as e:
            result = e
            status = ResultStatus.FAILED

        task_result = TaskResult[T](
            task=task,
            id=str(uuid4()),
            status=status,
            args=args,
            kwargs=kwargs,
            backend=self.alias,
        )

        task_result._result = result

        return task_result
