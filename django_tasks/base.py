from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import datetime
from inspect import isclass, iscoroutinefunction
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    Generic,
    Literal,
    TypeVar,
    cast,
    overload,
)

from asgiref.sync import async_to_sync, sync_to_async
from django.db.models.enums import TextChoices
from django.utils.module_loading import import_string
from django.utils.translation import pgettext_lazy
from django.utils.version import PY311
from typing_extensions import ParamSpec, Self

from .exceptions import TaskIntegrityError
from .utils import (
    get_module_path,
    json_normalize,
)

if TYPE_CHECKING:
    from .backends.base import BaseTaskBackend

DEFAULT_TASK_BACKEND_ALIAS = "default"
DEFAULT_QUEUE_NAME = "default"
MIN_PRIORITY = -100
MAX_PRIORITY = 100
DEFAULT_PRIORITY = 0

TASK_REFRESH_ATTRS = {
    "errors",
    "_return_value",
    "finished_at",
    "started_at",
    "last_attempted_at",
    "status",
    "enqueued_at",
    "worker_ids",
}


class ResultStatus(TextChoices):
    READY = ("READY", pgettext_lazy("Task", "Ready"))
    """The Task has just been enqueued, or is ready to be executed again."""

    RUNNING = ("RUNNING", pgettext_lazy("Task", "Running"))
    """The Task is currently running."""

    FAILED = ("FAILED", pgettext_lazy("Task", "Failed"))
    """The Task raised an exception during execution, or was unable to start."""

    SUCCEEDED = ("SUCCEEDED", pgettext_lazy("Task", "Succeeded"))
    """The Task has finished running successfully."""


T = TypeVar("T")
P = ParamSpec("P")


@dataclass(frozen=True, slots=PY311, kw_only=True)  # type: ignore[literal-required]
class Task(Generic[P, T]):
    priority: int
    """The Task's priority"""

    func: Callable[P, T]
    """The Task function"""

    backend: str
    """The name of the backend the Task will run on"""

    queue_name: str
    """The name of the queue the Task will run on"""

    run_after: datetime | None
    """The earliest this Task will run"""

    enqueue_on_commit: bool | None
    """
    Whether the Task will be enqueued when the current transaction commits,
    immediately, or whatever the backend decides.
    """

    takes_context: bool
    """
    Whether the Task receives the Task context when executed.
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
        priority: int | None = None,
        queue_name: str | None = None,
        run_after: datetime | None = None,
        backend: str | None = None,
    ) -> Self:
        """
        Create a new Task with modified defaults.
        """

        changes: dict[str, Any] = {}

        if priority is not None:
            changes["priority"] = priority
        if queue_name is not None:
            changes["queue_name"] = queue_name
        if run_after is not None:
            changes["run_after"] = run_after
        if backend is not None:
            changes["backend"] = backend

        return replace(self, **changes)

    def enqueue(self, *args: P.args, **kwargs: P.kwargs) -> "TaskResult[T]":
        """
        Queue up the Task to be executed.
        """
        return self.get_backend().enqueue(self, args, kwargs)

    async def aenqueue(self, *args: P.args, **kwargs: P.kwargs) -> "TaskResult[T]":
        """
        Queue up the Task to be executed.
        """
        return await self.get_backend().aenqueue(self, args, kwargs)

    def get_result(self, result_id: str) -> "TaskResult[T]":
        """
        Retrieve the result for a Task of this type by id if it exists,
        otherwise raise ResultDoesNotExist.
        """
        result = self.get_backend().get_result(result_id)

        if result.task.func != self.func:
            raise TaskIntegrityError(
                f"Task does not match (received {result.task.module_path!r})"
            )

        return result

    async def aget_result(self, result_id: str) -> "TaskResult[T]":
        """See get_result()."""
        result = await self.get_backend().aget_result(result_id)

        if result.task.func != self.func:
            raise TaskIntegrityError(
                f"Task does not match (received {result.task.module_path!r})"
            )

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
    enqueue_on_commit: bool | None = None,
    takes_context: Literal[False] = False,
) -> Callable[[Callable[P, T]], Task[P, T]]: ...


# Decorator with context and arguments
# e.g. @task(takes_context=True, ...)
@overload
def task(
    *,
    priority: int = DEFAULT_PRIORITY,
    queue_name: str = DEFAULT_QUEUE_NAME,
    backend: str = DEFAULT_TASK_BACKEND_ALIAS,
    enqueue_on_commit: bool | None = None,
    takes_context: Literal[True],
) -> Callable[[Callable[Concatenate["TaskContext", P], T]], Task[P, T]]: ...


# Implementation
def task(  # type: ignore[misc]
    function: Callable[P, T] | None = None,
    *,
    priority: int = DEFAULT_PRIORITY,
    queue_name: str = DEFAULT_QUEUE_NAME,
    backend: str = DEFAULT_TASK_BACKEND_ALIAS,
    enqueue_on_commit: bool | None = None,
    takes_context: bool = False,
) -> (
    Task[P, T]
    | Callable[[Callable[P, T]], Task[P, T]]
    | Callable[[Callable[Concatenate["TaskContext", P], T]], Task[P, T]]
):
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
            takes_context=takes_context,
            run_after=None,
        )

    if function:
        return wrapper(function)

    return wrapper


@dataclass(frozen=True, slots=PY311, kw_only=True)  # type: ignore[literal-required]
class TaskError:
    exception_class_path: str
    traceback: str

    @property
    def exception_class(self) -> type[BaseException]:
        # Lazy resolve the exception class
        exception_class = import_string(self.exception_class_path)

        if not isclass(exception_class) or not issubclass(
            exception_class, BaseException
        ):
            raise ValueError(
                f"{self.exception_class_path!r} does not reference a valid exception."
            )

        return exception_class


@dataclass(frozen=True, slots=PY311, kw_only=True)  # type: ignore[literal-required]
class TaskResult(Generic[T]):
    task: Task
    """Task for which this is a result"""

    id: str
    """A unique identifier for the task result"""

    status: ResultStatus
    """Status of the running Task"""

    enqueued_at: datetime | None
    """Time this Task was enqueued"""

    started_at: datetime | None
    """Time this Task was started"""

    finished_at: datetime | None
    """Time this Task was finished"""

    last_attempted_at: datetime | None
    """Time this Task was last attempted to be run"""

    args: list
    """The arguments to pass to the task function"""

    kwargs: dict[str, Any]
    """The keyword arguments to pass to the task function"""

    backend: str
    """The name of the backend the task is run with"""

    errors: list[TaskError]
    """Errors raised when running the task"""

    worker_ids: list[str]
    """The workers which have processed the task"""

    _return_value: T | None = field(init=False, default=None)

    def __post_init__(self) -> None:
        object.__setattr__(self, "args", json_normalize(self.args))
        object.__setattr__(self, "kwargs", json_normalize(self.kwargs))

    @property
    def return_value(self) -> T | None:
        """
        The return value of the task.

        If the task didn't succeed, an exception is raised.
        This is to distinguish against the task returning None.
        """
        if self.status == ResultStatus.SUCCEEDED:
            return cast(T, self._return_value)
        elif self.status == ResultStatus.FAILED:
            raise ValueError("Task failed")
        else:
            raise ValueError("Task has not finished yet")

    @property
    def is_finished(self) -> bool:
        """Has the task finished?"""
        return self.status in {ResultStatus.FAILED, ResultStatus.SUCCEEDED}

    @property
    def attempts(self) -> int:
        return len(self.worker_ids)

    def refresh(self) -> None:
        """
        Reload the cached task data from the task store
        """
        refreshed_task = self.task.get_backend().get_result(self.id)

        for attr in TASK_REFRESH_ATTRS:
            object.__setattr__(self, attr, getattr(refreshed_task, attr))

    async def arefresh(self) -> None:
        """
        Reload the cached task data from the task store
        """
        refreshed_task = await self.task.get_backend().aget_result(self.id)

        for attr in TASK_REFRESH_ATTRS:
            object.__setattr__(self, attr, getattr(refreshed_task, attr))


@dataclass(frozen=True, slots=PY311, kw_only=True)  # type: ignore[literal-required]
class TaskContext:
    task_result: TaskResult

    @property
    def attempt(self) -> int:
        return self.task_result.attempts
