from copy import deepcopy
from functools import partial
from typing import List, TypeVar
from uuid import uuid4

from django.db import transaction
from django.utils import timezone
from typing_extensions import ParamSpec

from django_tasks.exceptions import TaskRunDoesNotExist
from django_tasks.task import Task, TaskRun, TaskRunStatus
from django_tasks.utils import json_normalize

from .base import BaseTaskBackend

T = TypeVar("T")
P = ParamSpec("P")


class DummyBackend(BaseTaskBackend):
    supports_defer = True
    supports_async_task = True
    results: List[TaskRun]

    def __init__(self, options: dict) -> None:
        super().__init__(options)

        self.results = []

    def enqueue(self, task: Task[P, T], args: P.args, kwargs: P.kwargs) -> TaskRun[T]:
        self.validate_task(task)

        result = TaskRun[T](
            task=task,
            id=str(uuid4()),
            status=TaskRunStatus.NEW,
            enqueued_at=timezone.now(),
            started_at=None,
            finished_at=None,
            args=json_normalize(args),
            kwargs=json_normalize(kwargs),
            backend=self.alias,
        )

        if self._get_enqueue_on_commit_for_task(task) is not False:
            # Copy the task to prevent mutation issues
            transaction.on_commit(partial(self.results.append, deepcopy(result)))
        else:
            self.results.append(deepcopy(result))

        return result

    # We don't set `supports_get_task_run` as the results are scoped to the current thread
    def get_task_run(self, result_id: str) -> TaskRun:
        try:
            return next(result for result in self.results if result.id == result_id)
        except StopIteration:
            raise TaskRunDoesNotExist(result_id) from None

    def clear(self) -> None:
        self.results.clear()
