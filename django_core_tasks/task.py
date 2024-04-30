from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Callable, Generic, ParamSpec, Self, TypeVar

from django.db.models.enums import TextChoices
from django.utils import timezone

from .exceptions import ResultDoesNotExist

if TYPE_CHECKING:
    from .backends.base import BaseTaskBackend


class ResultStatus(TextChoices):
    NEW = "NEW"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    COMPLETE = "COMPLETE"


T = TypeVar("T")
P = ParamSpec("P")


@dataclass
class Task(Generic[P, T]):
    priority: int | None
    """The priority of the task"""

    func: Callable[P, T]
    """The task function"""

    queue_name: str | None
    """The name of the queue the task will run on """

    backend: str
    """The name of the backend the task will run on"""

    run_after: datetime | None = None
    """The earliest this task will run"""

    def __post_init__(self) -> None:
        self.get_backend().validate_task(self)

    def using(
        self,
        priority: int | None = None,
        queue_name: str | None = None,
        run_after: datetime | timedelta | None = None,
        backend: str | None = None,
    ) -> Self:
        """
        Create a new task with modified defaults
        """

        task = deepcopy(self)
        if priority is not None:
            task.priority = priority
        if queue_name is not None:
            task.queue_name = queue_name
        if run_after is not None:
            if isinstance(run_after, timedelta):
                task.run_after = timezone.now() + run_after
            else:
                task.run_after = run_after
        if backend is not None:
            task.backend = backend

        task.get_backend().validate_task(self)

        return task

    def enqueue(self, *args: P.args, **kwargs: P.kwargs) -> "TaskResult[T]":
        """
        Queue up the task to be executed
        """
        return self.get_backend().enqueue(self, args, kwargs)

    async def aenqueue(self, *args: P.args, **kwargs: P.kwargs) -> "TaskResult[T]":
        """
        Queue up a task function (or coroutine) to be executed
        """
        return await self.get_backend().aenqueue(self, args, kwargs)

    def get_result(self, result_id: str) -> "TaskResult[T]":
        """
        Retrieve the result for a task of this type by its id (if one exists).
        If one doesn't, or is the wrong type, raises ResultDoesNotExist.
        """
        result = self.get_backend().get_result(result_id)

        if result.task.func != self.func:
            raise ResultDoesNotExist

        return result

    async def aget_result(self, result_id: str) -> "TaskResult[T]":
        """
        Retrieve the result for a task of this type by its id (if one exists).
        If one doesn't, or is the wrong type, raises ResultDoesNotExist.
        """
        result = await self.get_backend().aget_result(result_id)

        if result.task.func != self.func:
            raise ResultDoesNotExist

        return result

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        return self.func(*args, **kwargs)

    def get_backend(self) -> "BaseTaskBackend":
        from . import tasks

        return tasks[self.backend]


def task(
    priority: int | None = None,
    queue_name: str | None = None,
    backend: str = "default",
) -> Callable[[Callable[P, T]], Task[P, T]]:
    """
    A decorator used to create a task.
    """
    from . import tasks

    def wrapper(f: Callable[P, T]) -> Task[P, T]:
        return tasks[backend].task_class(
            priority=priority, func=f, queue_name=queue_name, backend=backend
        )

    return wrapper


@dataclass
class TaskResult(Generic[T]):
    task: Task
    """The task for which this is a result"""

    id: str
    """A unique identifier for the task result"""

    status: ResultStatus
    """The status of the running task"""

    args: list
    """The arguments to pass to the task function"""

    kwargs: dict
    """The keyword arguments to pass to the task function"""

    backend: str
    """The name of the backend the task will run on"""

    _result: T | None = field(init=False, default=None)

    @property
    def result(self) -> T:
        if self.status not in [ResultStatus.COMPLETE, ResultStatus.FAILED]:
            raise ValueError("Task has not finished yet")

        return self._result  # type:ignore

    def get_result(self) -> T | None:
        """
        A convenience method to get the result, or None if it's not ready yet.
        """
        return self._result

    def refresh(self) -> None:
        """
        Reload the cached task data from the task store
        """
        refreshed_task = self.task.get_backend().get_result(self.id)

        # status and result are the only refreshable attributes
        self.status = refreshed_task.status
        self._result = refreshed_task._result

    async def arefresh(self) -> None:
        """
        Reload the cached task data from the task store
        """
        refreshed_task = await self.task.get_backend().aget_result(self.id)

        # status and result are the only refreshable attributes
        self.status = refreshed_task.status
        self._result = refreshed_task._result
