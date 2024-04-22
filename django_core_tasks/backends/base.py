from asgiref.sync import sync_to_async
from django.utils import timezone

from django_core_tasks.exceptions import InvalidTaskError
from django_core_tasks.task import Task, TaskResult


class BaseTaskBackend:
    task_class = Task

    supports_defer = False

    def __init__(self, options):
        self.alias = options["ALIAS"]

    @classmethod
    def validate_task(cls, task: Task) -> None:
        """
        Determine whether the provided task is one which can be executed by the backend.
        """
        if task.priority is not None and task.priority < 1:
            raise InvalidTaskError("priority must be positive")

        if task.run_after is not None and not timezone.is_aware(task.run_after):
            raise InvalidTaskError("run_after must be an aware datetime")

    def enqueue(self, task: Task, args, kwargs) -> TaskResult:
        """
        Queue up a task to be executed
        """
        self.validate_task(task)

    async def aenqueue(self, task: Task, args, kwargs):
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
        ...

    async def aget_result(self, result_id: str):
        """
        Queue up a task function (or coroutine) to be executed
        """
        return await sync_to_async(self.get_result, thread_sensitive=True)(
            result_id=result_id
        )

    def close(self) -> None:
        """
        Close any connections opened as part of the constructor
        """
        ...
