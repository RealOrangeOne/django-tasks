from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Self

from asgiref.sync import sync_to_async
from django.db.models.enums import StrEnum
from django.utils import timezone

from .exceptions import ResultDoesNotExist


class TaskStatus(StrEnum):
    NEW = "NEW"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    COMPLETE = "COMPLETE"


@dataclass
class Task:
    priority: int | None
    """The priority of the task"""

    func: Callable
    """The task function"""

    queue_name: str | None
    """The name of the queue the task will run on """

    backend: str
    """The name of the backend the task will run on"""

    run_after: datetime | None = None
    """The earliest this task will run"""

    def __post_init__(self):
        from . import tasks

        tasks[self.backend].validate_task(self)

    def using(
        self,
        priority: int | None = None,
        queue_name: str | None = None,
        run_after: datetime | timedelta | None = None,
    ) -> Self:
        """
        Create a new task with modified defaults
        """
        from . import tasks

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

        tasks[self.backend].validate_task(self)

        return task

    def enqueue(self, *args, **kwargs):
        """
        Queue up the task to be executed
        """
        from . import tasks

        return tasks[self.backend].enqueue(self, args, kwargs)

    async def aenqueue(self, *args, **kwargs):
        """
        Queue up a task function (or coroutine) to be executed
        """
        from . import tasks

        return await tasks[self.backend].aenqueue(self, args, kwargs)

    def get_result(self, result_id: str) -> Self:
        """
        Retrieve the result for a task of this type by its id (if one exists).
        If one doesn't, or is the wrong type, raises ResultDoesNotExist.
        """
        from . import tasks

        result = tasks[self.backend].get_result(result_id)

        if result.task.func != self.func:
            raise ResultDoesNotExist

        return result

    async def aget_result(self, result_id: str) -> Self:
        """
        Retrieve the result for a task of this type by its id (if one exists).
        If one doesn't, or is the wrong type, raises ResultDoesNotExist.
        """
        from . import tasks

        result = await tasks[self.backend].aget_result(result_id)

        if result.task.func != self.func:
            raise ResultDoesNotExist

        return result

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


def task(
    priority: int | None = None,
    queue_name: str | None = None,
    backend: str = "default",
):
    """
    A decorator used to create a task.
    """
    from . import tasks

    def wrapper(f):
        return tasks[backend].task_class(
            priority=priority, func=f, queue_name=queue_name, backend=backend
        )

    return wrapper


@dataclass
class TaskResult:
    task: Task
    """The task for which this is a result"""

    id: str
    """A unique identifier for the task result"""

    status: TaskStatus
    """The status of the running task"""

    args: list
    """The arguments to pass to the task function"""

    kwargs: dict
    """The keyword arguments to pass to the task function"""

    backend: str
    """The name of the backend the task will run on"""

    _result: Any = field(init=False, default=None)

    @property
    def result(self):
        if self.status not in [TaskStatus.COMPLETE, TaskStatus.FAILED]:
            raise ValueError("Task has not finished yet")

        return self._result

    def refresh(self) -> None:
        """
        Reload the cached task data from the task store
        """
        ...

    async def arefresh(self) -> None:
        """
        Reload the cached task data from the task store
        """
        return await sync_to_async(self.refresh, thread_sensitive=True)()
