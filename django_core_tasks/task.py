from enum import Enum
from typing import Any
from datetime import datetime
from asgiref.sync import sync_to_async


class TaskStatus(Enum, str):
    NEW = "NEW"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    COMPLETE = "COMPLETE"


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

    def __init__(self, **kwargs):
        pass

    def refresh(self):
        raise NotImplementedError("This task cannot be refreshed")

    async def arefresh(self):
        sync_to_async(self.refresh, thread_sensitive=True)()
