from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from inspect import iscoroutinefunction
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generic,
    Optional,
    TypeVar,
    Union,
    overload,
)

from asgiref.sync import async_to_sync, sync_to_async
from django.db.models.enums import TextChoices
from django.utils import timezone
from typing_extensions import ParamSpec, Self

from .exceptions import ResultDoesNotExist

if TYPE_CHECKING:
    from .backends.base import BaseTaskBackend

DEFAULT_TASK_BACKEND_ALIAS = "default"
DEFAULT_QUEUE_NAME = "default"


class ResultStatus(TextChoices):
    NEW = ("NEW", "New")
    RUNNING = ("RUNNING", "Running")
    FAILED = ("FAILED", "Failed")
    COMPLETE = ("COMPLETE", "Complete")


T = TypeVar("T")
P = ParamSpec("P")


@dataclass
class Task(Generic[P, T]):
    priority: int
    """The priority of the task"""

    func: Callable[P, T]
    """The task function"""

    backend: str
    """The name of the backend the task will run on"""

    queue_name: str = DEFAULT_QUEUE_NAME
    """The name of the queue the task will run on """

    run_after: Optional[datetime] = None
    """The earliest this task will run"""

    def __post_init__(self) -> None:
        self.get_backend().validate_task(self)

    @property
    def name(self) -> str:
        """
        An identifier for the task
        """
        return self.func.__name__

    def using(
        self,
        priority: Optional[int] = None,
        queue_name: Optional[str] = None,
        run_after: Optional[Union[datetime, timedelta]] = None,
        backend: Optional[str] = None,
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

    def call(self, *args: P.args, **kwargs: P.kwargs) -> T:
        if iscoroutinefunction(self.func):
            return async_to_sync(self.func)(*args, **kwargs)  # type:ignore[no-any-return]
        return self.func(*args, **kwargs)

    async def acall(self, *args: P.args, **kwargs: P.kwargs) -> T:
        if iscoroutinefunction(self.func):
            return await self.func(*args, **kwargs)  # type:ignore[no-any-return]
        return await sync_to_async(self.func)(*args, **kwargs)

    def get_backend(self) -> "BaseTaskBackend":
        from . import tasks

        return tasks[self.backend]

    @property
    def module_path(self) -> str:
        return f"{self.func.__module__}.{self.func.__qualname__}"


# Bare decorator usage
# e.g. @task
@overload
def task(function: Callable[P, T], /) -> Task[P, T]: ...


# Decorator with arguments
# e.g. @task() or @task(priority=1, ...)
@overload
def task(
    *,
    priority: int = 0,
    queue_name: str = DEFAULT_QUEUE_NAME,
    backend: str = DEFAULT_TASK_BACKEND_ALIAS,
) -> Callable[[Callable[P, T]], Task[P, T]]: ...


# Implementation
def task(
    function: Optional[Callable[P, T]] = None,
    *,
    priority: int = 0,
    queue_name: str = DEFAULT_QUEUE_NAME,
    backend: str = DEFAULT_TASK_BACKEND_ALIAS,
) -> Union[Task[P, T], Callable[[Callable[P, T]], Task[P, T]]]:
    """
    A decorator used to create a task.
    """
    from . import tasks

    def wrapper(f: Callable[P, T]) -> Task[P, T]:
        return tasks[backend].task_class(
            priority=priority, func=f, queue_name=queue_name, backend=backend
        )

    if function:
        return wrapper(function)

    return wrapper


@dataclass
class TaskResult(Generic[T]):
    task: Task
    """The task for which this is a result"""

    id: str
    """A unique identifier for the task result"""

    status: ResultStatus
    """The status of the running task"""

    enqueued_at: datetime
    """The time this task was enqueued"""

    finished_at: Optional[datetime]
    """The time this task was finished"""

    args: list
    """The arguments to pass to the task function"""

    kwargs: Dict[str, Any]
    """The keyword arguments to pass to the task function"""

    backend: str
    """The name of the backend the task will run on"""

    _result: Optional[T] = field(init=False, default=None)

    @property
    def result(self) -> T:
        if self.status not in [ResultStatus.COMPLETE, ResultStatus.FAILED]:
            raise ValueError("Task has not finished yet")

        return self._result  # type:ignore

    def get_result(self) -> Optional[T]:
        """
        A convenience method to get the result, or None if it's not ready yet.
        """
        return self._result

    def refresh(self) -> None:
        """
        Reload the cached task data from the task store
        """
        refreshed_task = self.task.get_backend().get_result(self.id)

        # status, finished_at and result are the only refreshable attributes
        self.status = refreshed_task.status
        self.finished_at = refreshed_task.finished_at
        self._result = refreshed_task._result

    async def arefresh(self) -> None:
        """
        Reload the cached task data from the task store
        """
        refreshed_task = await self.task.get_backend().aget_result(self.id)

        # status, finished_at and result are the only refreshable attributes
        self.status = refreshed_task.status
        self.finished_at = refreshed_task.finished_at
        self._result = refreshed_task._result
