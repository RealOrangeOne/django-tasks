from .base import BaseTaskBackend
from django_core_tasks.exceptions import InvalidTask
from django_core_tasks.task import BaseTask, TaskStatus
from django.utils import timezone
from django.utils.crypto import get_random_string
from inspect import iscoroutinefunction
from asgiref.sync import async_to_sync


class ImmediateTask(BaseTask):
    def __init__(self, id, func, args, kwargs, result, queued_at, completed_at):
        self.func = func
        self.args = args
        self.kwargs = kwargs

        self.status = (
            TaskStatus.FAILED
            if isinstance(result, BaseException)
            else TaskStatus.COMPLETE
        )
        self.result = result

        self.queued_at = queued_at
        self.completed_at = completed_at


class ImmediateBackend(BaseTaskBackend):
    """
    Execute tasks immediately, in the current thread.
    """

    def enqueue(self, func, *, priority=None, args=None, kwargs=None):
        if not self.is_valid_task_function(func):
            raise InvalidTask(func)

        task_id = get_random_string(10)
        queued_at = timezone.now()

        task_func = async_to_sync(func) if iscoroutinefunction(func) else func

        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}

        try:
            result = task_func(*args, **kwargs)
        except Exception as e:
            result = e

        completed_at = timezone.now()

        return ImmediateTask(
            task_id, func, args, kwargs, result, queued_at, completed_at
        )
