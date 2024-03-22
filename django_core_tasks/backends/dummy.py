import uuid

from django.utils import timezone

from django_core_tasks.exceptions import TaskDoesNotExist
from django_core_tasks.task import ImmutableTask, TaskCandidate, TaskStatus

from .base import BaseTaskBackend


class DummyBackend(BaseTaskBackend):
    """
    Stores tasks for execution later
    """

    def __init__(self, options):
        super().__init__(options)

        self.tasks = []

    def enqueue(self, func, *, priority=None, args=None, kwargs=None):
        self.validate_candidate(
            TaskCandidate(func=func, priority=priority, args=args, kwargs=kwargs)
        )

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
        self.validate_candidate(
            TaskCandidate(
                func=func, priority=priority, args=args, kwargs=kwargs, when=when
            )
        )

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
