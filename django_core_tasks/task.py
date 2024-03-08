from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

from asgiref.sync import sync_to_async
from django.db.models.enums import StrEnum


class TaskStatus(StrEnum):
    NEW = "NEW"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    COMPLETE = "COMPLETE"


@dataclass
class BaseTask:
    id: str
    """A unique identifier for the task"""

    status: TaskStatus
    """The status of the task"""

    queued_at: datetime
    """When the task was added to the queue"""

    completed_at: datetime | None
    """When the task was completed"""

    priority: int | None
    """The priority of the task"""

    func: Callable
    """The task function"""

    args: list
    """The arguments to pass to the task function"""

    kwargs: dict
    """The keyword arguments to pass to the task function"""

    when: datetime | None
    """When the task is scheduled to run"""

    _result: Any = field(init=False)

    def refresh(self):
        raise NotImplementedError("This task cannot be refreshed")

    async def arefresh(self):
        return await sync_to_async(self.refresh, thread_sensitive=True)()

    @property
    def result(self):
        """The return value or exception from the task function"""
        if self.status not in [TaskStatus.COMPLETE, TaskStatus.FAILED]:
            raise ValueError("Task has not finished yet")

        return self._result


class Task(BaseTask):
    pass


class ImmutableTask(BaseTask):
    """
    A task which will not update after creation.

    Notably, `refresh` becomes a no-op, rather than explicitly prevented.
    """

    def refresh(self):
        pass
