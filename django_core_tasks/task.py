from django.db.models.enums import StrEnum
from typing import Any, Callable
from datetime import datetime
from asgiref.sync import sync_to_async
from dataclasses import dataclass


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

    result: Any | None
    """The return value from the task function"""

    queued_at: datetime
    """When the task was added to the queue"""

    completed_at: datetime | None
    """When the task was completed"""

    raw: Any | None
    """Return the underlying runner's task handle"""

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

    def refresh(self):
        raise NotImplementedError("This task cannot be refreshed")

    async def arefresh(self):
        return await sync_to_async(self.refresh, thread_sensitive=True)()


class Task(BaseTask):
    pass
