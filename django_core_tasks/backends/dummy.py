import uuid
from datetime import timedelta

from django.utils import timezone

from django_core_tasks.exceptions import InvalidTaskError, TaskDoesNotExist
from django_core_tasks.task import ImmutableTask, TaskStatus

from .base import BaseTaskBackend


class DummyBackend(BaseTaskBackend):
    """
    Stores tasks for execution later
    """

    def __init__(self, options):
        super().__init__(options)

        self.tasks = []

    def enqueue(self, func, *, priority=None, args=None, kwargs=None):
        if not self.is_valid_task_function(func):
            raise InvalidTaskError(func)

        if priority is not None and priority < 1:
            raise ValueError("priority must be positive")

        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}

        task = ImmutableTask(
            id=str(uuid.uuid4()),
            status=TaskStatus.NEW,
            queued_at=timezone.now(),
            completed_at=None,
            priority=priority,
            func=func,
            args=args,
            kwargs=kwargs,
            when=None,
        )
        task._result = None

        self.tasks.append(task)

        return task

    def defer(self, func, *, when, priority=None, args=None, kwargs=None):
        if not self.is_valid_task_function(func):
            raise InvalidTaskError(func)

        if priority is not None and priority < 1:
            raise ValueError("priority must be positive")

        if when < (timezone.now() - timedelta(seconds=1)):
            raise ValueError("when must be in the future")

        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}

        task = ImmutableTask(
            id=str(uuid.uuid4()),
            status=TaskStatus.NEW,
            queued_at=timezone.now(),
            completed_at=None,
            priority=priority,
            func=func,
            args=args,
            kwargs=kwargs,
            when=when,
        )
        task._result = None

        self.tasks.append(task)

        return task

    def get_task(self, task_id):
        try:
            return next(task for task in self.tasks if task.id == task_id)
        except StopIteration:
            raise TaskDoesNotExist(task_id) from None

    def clear(self):
        self.tasks.clear()
