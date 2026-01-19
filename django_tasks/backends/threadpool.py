import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

from django.db import close_old_connections
from django.utils.functional import cached_property
from typing_extensions import ParamSpec

from django_tasks.base import Task, TaskResult, TaskResultStatus
from django_tasks.utils import (
    get_random_id,
)

from .base import BaseTaskBackend
from .immediate import _execute_task as _immediate_execute_task

logger = logging.getLogger(__name__)


T = TypeVar("T")
P = ParamSpec("P")


def _execute_task(
    backend_class: type[BaseTaskBackend], task_result: TaskResult
) -> None:
    _immediate_execute_task(backend_class, task_result, str(threading.get_native_id()))
    close_old_connections()


class ThreadPoolBackend(BaseTaskBackend):
    supports_async_task = True
    supports_priority = True

    def __init__(self, alias: str, params: dict):
        super().__init__(alias, params)

        self.max_workers = self.options.get("MAX_WORKERS")

    @cached_property
    def pool(self) -> ThreadPoolExecutor:
        return ThreadPoolExecutor(max_workers=self.max_workers)

    def enqueue(
        self,
        task: Task[P, T],
        args: P.args,  # type:ignore[valid-type]
        kwargs: P.kwargs,  # type:ignore[valid-type]
    ) -> TaskResult[T]:
        self.validate_task(task)

        task_result: TaskResult[T] = TaskResult(
            task=task,
            id=get_random_id(),
            status=TaskResultStatus.READY,
            enqueued_at=None,
            started_at=None,
            last_attempted_at=None,
            finished_at=None,
            args=args,
            kwargs=kwargs,
            backend=self.alias,
            errors=[],
            worker_ids=[],
            metadata={},
        )

        self.pool.submit(
            _execute_task,
            type(self),
            task_result,
        )

        return task_result

    def save_metadata(self, result_id: str, metadata: dict[str, Any]) -> None:
        pass

    async def asave_metadata(self, result_id: str, metadata: dict[str, Any]) -> None:
        pass
