from abc import ABCMeta, abstractmethod
from inspect import iscoroutinefunction
from typing import Any, Iterable, Optional, TypeVar

from asgiref.sync import sync_to_async
from django.core.checks import messages
from django.utils import timezone
from typing_extensions import ParamSpec

from django_tasks.exceptions import InvalidTaskError
from django_tasks.task import MAX_PRIORITY, MIN_PRIORITY, Task, TaskResult
from django_tasks.utils import is_global_function

T = TypeVar("T")
P = ParamSpec("P")


class BaseTaskBackend(metaclass=ABCMeta):
    alias: str
    enqueue_on_commit: Optional[bool]

    task_class = Task

    supports_defer = False
    """Can tasks be enqueued with the run_after attribute"""

    supports_async_task = False
    """Can coroutines be enqueued"""

    supports_get_result = False
    """Can results be retrieved after the fact (from **any** thread / process)"""

    def __init__(self, options: dict) -> None:
        from django_tasks import DEFAULT_QUEUE_NAME

        self.alias = options["ALIAS"]
        self.queues = set(options.get("QUEUES", [DEFAULT_QUEUE_NAME]))
        self.enqueue_on_commit = options.get("ENQUEUE_ON_COMMIT", None)

    def _get_enqueue_on_commit_for_task(self, task: Task) -> Optional[bool]:
        """
        Determine the correct `enqueue_on_commit` setting to use for a given task.

        If the task defines it, use that, otherwise, fall back to the backend.
        """
        if isinstance(task.enqueue_on_commit, bool):
            return task.enqueue_on_commit

        return self.enqueue_on_commit

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

        if (
            task.priority < MIN_PRIORITY
            or task.priority > MAX_PRIORITY
            or int(task.priority) != task.priority
        ):
            raise InvalidTaskError(
                f"priority must be a whole number between {MIN_PRIORITY} and {MAX_PRIORITY}"
            )

        if not self.supports_defer and task.run_after is not None:
            raise InvalidTaskError("Backend does not support run_after")

        if task.run_after is not None and not timezone.is_aware(task.run_after):
            raise InvalidTaskError("run_after must be an aware datetime")

        if self.queues and task.queue_name not in self.queues:
            raise InvalidTaskError(
                f"Queue '{task.queue_name}' is not valid for backend"
            )

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

    def check(self, **kwargs: Any) -> Iterable[messages.CheckMessage]:
        if self.enqueue_on_commit not in {True, False, None}:
            yield messages.CheckMessage(
                messages.ERROR, "`ENQUEUE_ON_COMMIT` must be a bool or None"
            )
