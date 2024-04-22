from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Self

from django.db.models.enums import StrEnum


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
        return deepcopy(self)

    def enqueue(self, *args, **kwargs):
        """
        Queue up the task to be executed
        """
        from . import tasks

        return tasks[self.backend].enqueue(self, args, kwargs)

    def get(self, result_id: str) -> Self:
        """
        Retrieve the result for a task of this type by its id (if one exists).
        If one doesn't, or is the wrong type, raises ResultDoesNotExist.
        """
        ...

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

    def refresh(self) -> None:
        """
        Reload the cached task data from the task store
        """
        ...
