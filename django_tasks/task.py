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
    cast,
    overload,
)

from asgiref.sync import async_to_sync, sync_to_async
from django.db.models.enums import TextChoices
from django.utils import timezone
from django.utils.translation import gettext_lazy as _
from typing_extensions import ParamSpec, Self

from .exceptions import ResultDoesNotExist
from .utils import SerializedExceptionDict, exception_from_dict, get_module_path

if TYPE_CHECKING:
    from .backends.base import BaseTaskBackend

DEFAULT_TASK_BACKEND_ALIAS = "default"
DEFAULT_QUEUE_NAME = "default"
MIN_PRIORITY = -100
MAX_PRIORITY = 100
DEFAULT_PRIORITY = 0


class ResultStatus(TextChoices):
    NEW = ("NEW", _("New"))
    RUNNING = ("RUNNING", _("Running"))
    FAILED = ("FAILED", _("Failed"))
    COMPLETE = ("COMPLETE", _("Complete"))


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
    """The name of the queue the task will run on"""

    run_after: Optional[datetime] = None
    """The earliest this task will run"""

    enqueue_on_commit: Optional[bool] = None
    """
    Whether the task will be enqueued when the current transaction commits,
    immediately, or whatever the backend decides
    """

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
        *,
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

        task.get_backend().validate_task(task)

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
        return get_module_path(self.func)


# Bare decorator usage
# e.g. @task
@overload
def task(function: Callable[P, T], /) -> Task[P, T]: ...


# Decorator with arguments
# e.g. @task() or @task(priority=1, ...)
@overload
def task(
    *,
    priority: int = DEFAULT_PRIORITY,
    queue_name: str = DEFAULT_QUEUE_NAME,
    backend: str = DEFAULT_TASK_BACKEND_ALIAS,
    enqueue_on_commit: Optional[bool] = None,
) -> Callable[[Callable[P, T]], Task[P, T]]: ...


# Implementation
def task(
    function: Optional[Callable[P, T]] = None,
    *,
    priority: int = DEFAULT_PRIORITY,
    queue_name: str = DEFAULT_QUEUE_NAME,
    backend: str = DEFAULT_TASK_BACKEND_ALIAS,
    enqueue_on_commit: Optional[bool] = None,
) -> Union[Task[P, T], Callable[[Callable[P, T]], Task[P, T]]]:
    """
    A decorator used to create a task.
    """
    from . import tasks

    def wrapper(f: Callable[P, T]) -> Task[P, T]:
        return tasks[backend].task_class(
            priority=priority,
            func=f,
            queue_name=queue_name,
            backend=backend,
            enqueue_on_commit=enqueue_on_commit,
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

    started_at: Optional[datetime]
    """The time this task was started"""

    finished_at: Optional[datetime]
    """The time this task was finished"""

    args: list
    """The arguments to pass to the task function"""

    kwargs: Dict[str, Any]
    """The keyword arguments to pass to the task function"""

    backend: str
    """The name of the backend the task will run on"""

    _result: Optional[Union[T, SerializedExceptionDict]] = field(
        init=False, default=None
    )

    @property
    def result(self) -> Optional[Union[T, BaseException]]:
        if self.status == ResultStatus.COMPLETE:
            return cast(T, self._result)
        elif self.status == ResultStatus.FAILED:
            return (
                exception_from_dict(cast(SerializedExceptionDict, self._result))
                if self._result is not None
                else None
            )

        raise ValueError("Task has not finished yet")

    @property
    def traceback(self) -> Optional[str]:
        """
        Return the string representation of the traceback of the task if it failed
        """
        if self.status == ResultStatus.FAILED and self._result is not None:
            return cast(SerializedExceptionDict, self._result)["exc_traceback"]

        return None

    def get_result(self) -> Optional[T]:
        """
        A convenience method to get the result, or None if it's not ready yet or has failed.
        """
        return cast(T, self.result) if self.status == ResultStatus.COMPLETE else None

    def refresh(self) -> None:
        """
        Reload the cached task data from the task store
        """
        refreshed_task = self.task.get_backend().get_result(self.id)

        # status, started_at, finished_at and result are the only refreshable attributes
        self.status = refreshed_task.status
        self.started_at = refreshed_task.started_at
        self.finished_at = refreshed_task.finished_at
        self._result = refreshed_task._result

    async def arefresh(self) -> None:
        """
        Reload the cached task data from the task store
        """
        refreshed_task = await self.task.get_backend().aget_result(self.id)

        # status, started_at, finished_at and result are the only refreshable attributes
        self.status = refreshed_task.status
        self.started_at = refreshed_task.started_at
        self.finished_at = refreshed_task.finished_at
        self._result = refreshed_task._result
