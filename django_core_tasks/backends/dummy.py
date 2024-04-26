from typing import ParamSpec, TypeVar
from uuid import uuid4

from django_core_tasks.exceptions import ResultDoesNotExist
from django_core_tasks.task import Task, TaskResult, TaskStatus

from .base import BaseTaskBackend

T = TypeVar("T")
P = ParamSpec("P")


class DummyBackend(BaseTaskBackend):
    results: list[TaskResult]

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
            status=TaskStatus.NEW,
            args=args,
            kwargs=kwargs,
            backend=self.alias,
        )

        self.results.append(result)

        return result

    def get_result(self, result_id: str) -> TaskResult:
        try:
            return next(result for result in self.results if result.id == result_id)
        except StopIteration:
            raise ResultDoesNotExist(result_id) from None

    def clear(self) -> None:
        self.results.clear()
