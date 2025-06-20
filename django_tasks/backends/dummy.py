from copy import deepcopy
from functools import partial
from typing import TypeVar

from django.db import transaction
from django.utils import timezone
from typing_extensions import ParamSpec

from django_tasks.exceptions import ResultDoesNotExist
from django_tasks.signals import task_enqueued
from django_tasks.task import ResultStatus, Task, TaskResult
from django_tasks.utils import get_random_id

from .base import BaseTaskBackend

T = TypeVar("T")
P = ParamSpec("P")


class DummyBackend(BaseTaskBackend):
    supports_defer = True
    supports_async_task = True
    results: list[TaskResult]

    def __init__(self, alias: str, params: dict) -> None:
        super().__init__(alias, params)

        self.results = []

    def _store_result(self, result: TaskResult) -> None:
        object.__setattr__(result, "enqueued_at", timezone.now())
        self.results.append(result)
        task_enqueued.send(type(self), task_result=result)

    def enqueue(
        self,
        task: Task[P, T],
        args: P.args,  # type:ignore[valid-type]
        kwargs: P.kwargs,  # type:ignore[valid-type]
    ) -> TaskResult[T]:
        self.validate_task(task)

        result = TaskResult[T](
            task=task,
            id=get_random_id(),
            status=ResultStatus.READY,
            enqueued_at=None,
            started_at=None,
            last_attempted_at=None,
            finished_at=None,
            args=args,
            kwargs=kwargs,
            backend=self.alias,
            errors=[],
            worker_ids=[],
        )

        if self._get_enqueue_on_commit_for_task(task) is not False:
            transaction.on_commit(partial(self._store_result, result))
        else:
            self._store_result(result)

        # Copy the task to prevent mutation issues
        return deepcopy(result)

    # We don't set `supports_get_result` as the results are scoped to the current thread
    def get_result(self, result_id: str) -> TaskResult:
        try:
            return next(result for result in self.results if result.id == result_id)
        except StopIteration:
            raise ResultDoesNotExist(result_id) from None

    def clear(self) -> None:
        self.results.clear()
