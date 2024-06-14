from copy import deepcopy
from typing import List, TypeVar
from uuid import uuid4

from django.utils import timezone
from typing_extensions import ParamSpec

from django_tasks.exceptions import ResultDoesNotExist
from django_tasks.task import ResultStatus, Task, TaskResult
from django_tasks.utils import json_normalize

from .base import BaseTaskBackend

T = TypeVar("T")
P = ParamSpec("P")


class DummyBackend(BaseTaskBackend):
    supports_defer = True
    supports_async_task = True
    results: List[TaskResult]

    def __init__(self, options: dict) -> None:
        super().__init__(options)

        self.results = []

    def enqueue(
        self, task: Task[P, T], args: P.args, kwargs: P.kwargs
    ) -> TaskResult[T]:
        self.validate_task(task)

        result = TaskResult[T](
            task=task,
            id=str(uuid4()),
            status=ResultStatus.NEW,
            enqueued_at=timezone.now(),
            finished_at=None,
            args=json_normalize(args),
            kwargs=json_normalize(kwargs),
            backend=self.alias,
        )

        # Copy the task to prevent mutation issues
        self.results.append(deepcopy(result))

        return result

    # We don't set `supports_get_result` as the results are scoped to the current thread
    def get_result(self, result_id: str) -> TaskResult:
        try:
            return next(result for result in self.results if result.id == result_id)
        except StopIteration:
            raise ResultDoesNotExist(result_id) from None

    def clear(self) -> None:
        self.results.clear()
