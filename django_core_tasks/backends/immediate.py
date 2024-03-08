import uuid
from inspect import iscoroutinefunction

from asgiref.sync import async_to_sync
from django.utils import timezone

from django_core_tasks.exceptions import InvalidTaskError
from django_core_tasks.task import ImmutableTask, TaskStatus

from .base import BaseTaskBackend


class ImmediateBackend(BaseTaskBackend):
    """
    Execute tasks immediately, in the current thread.
    """

    def enqueue(self, func, *, priority=None, args=None, kwargs=None):
        if not self.is_valid_task_function(func):
            raise InvalidTaskError(func)

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

        task = ImmutableTask(
            id=str(uuid.uuid4()),
            status=TaskStatus.FAILED
            if isinstance(result, BaseException)
            else TaskStatus.COMPLETE,
            queued_at=queued_at,
            completed_at=completed_at,
            priority=priority,
            func=func,
            args=args,
            kwargs=kwargs,
            when=None,
        )
        task._result = result
        return task
