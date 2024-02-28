from .base import BaseTaskBackend
from django_core_tasks.exceptions import InvalidTask
from django_core_tasks.task import Task, TaskStatus
from django.utils import timezone
from django.utils.crypto import get_random_string
from inspect import iscoroutinefunction
from asgiref.sync import async_to_sync


class ImmediateBackend(BaseTaskBackend):
    """
    Execute tasks immediately, in the current thread.
    """

    def enqueue(self, func, *, priority=None, args=None, kwargs=None):
        if not self.is_valid_task_function(func):
            raise InvalidTask(func)

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

        return Task(
            id=get_random_string(10),
            status=TaskStatus.FAILED
            if isinstance(result, BaseException)
            else TaskStatus.COMPLETE,
            result=result,
            queued_at=queued_at,
            completed_at=completed_at,
            priority=priority,
            func=func,
            args=args,
            kwargs=kwargs,
            when=None,
            raw=None,
        )
