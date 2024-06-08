from abc import ABCMeta, abstractmethod
from inspect import iscoroutinefunction
from typing import Any, List, TypeVar

from asgiref.sync import sync_to_async
from django.core.checks.messages import CheckMessage
from django.utils import timezone
from typing_extensions import ParamSpec

from django_tasks.exceptions import InvalidTaskError
from django_tasks.task import Task, TaskResult
from django_tasks.utils import is_global_function

T = TypeVar("T")
P = ParamSpec("P")


class BaseTaskBackend(metaclass=ABCMeta):
    task_class = Task

    supports_defer = False
    """Can tasks be enqueued with the run_after attribute"""

    supports_async_task = False
    """Can coroutines be enqueued"""

    supports_get_result = False
    """Can results be retrieved after the fact (from **any** thread / process)"""

    def __init__(self, options: dict) -> None:
        self.alias = options["ALIAS"]

    def validate_task(self, task: Task) -> None:
        """
        Determine whether the provided task is one which can be executed by the backend.
        """
        if not is_global_function(task.func):
            raise InvalidTaskError(
                "Task function must be a globally importable function"
            )

        if not self.supports_async_task and iscoroutinefunction(task.func):
            raise InvalidTaskError("Backend does not support async tasks")

        if task.priority < 0:
            raise InvalidTaskError("priority must be zero or greater")

        if not self.supports_defer and task.run_after is not None:
            raise InvalidTaskError("Backend does not support run_after")

        if task.run_after is not None and not timezone.is_aware(task.run_after):
            raise InvalidTaskError("run_after must be an aware datetime")

    @abstractmethod
    def enqueue(
        self, task: Task[P, T], args: P.args, kwargs: P.kwargs
    ) -> TaskResult[T]:
        """
        Queue up a task to be executed
        """
        ...

    async def aenqueue(
        self, task: Task[P, T], args: P.args, kwargs: P.kwargs
    ) -> TaskResult[T]:
        """
        Queue up a task function (or coroutine) to be executed
        """
        return await sync_to_async(self.enqueue, thread_sensitive=True)(
            task=task, args=args, kwargs=kwargs
        )

    def get_result(self, result_id: str) -> TaskResult:
        """
        Retrieve a result by its id (if one exists).
        If one doesn't, raises ResultDoesNotExist.
        """
        raise NotImplementedError(
            "This backend does not support retrieving or refreshing results."
        )

    async def aget_result(self, result_id: str) -> TaskResult:
        """
        Queue up a task function (or coroutine) to be executed
        """
        return await sync_to_async(self.get_result, thread_sensitive=True)(
            result_id=result_id
        )

    def close(self) -> None:
        """
        Close any connections opened as part of the constructor
        """
        # HACK: `close` isn't abstract, but should do nothing by default
        return None

    def check(self, **kwargs: Any) -> List[CheckMessage]:
        raise NotImplementedError(
            "subclasses may provide a check() method to verify that task "
            "backend is configured correctly."
        )
