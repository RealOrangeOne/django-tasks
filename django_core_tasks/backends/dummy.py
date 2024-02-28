from .base import BaseTaskBackend
from django_core_tasks.exceptions import InvalidTask, TaskDoesNotExist
from django_core_tasks.task import Task, TaskStatus
from django.utils import timezone
from django.utils.crypto import get_random_string


class DummyBackend(BaseTaskBackend):
    """
    Stores tasks for execution later
    """

    def __init__(self, options):
        super().__init__(options)

        self.tasks = []

    def enqueue(self, func, *, priority=None, args=None, kwargs=None):
        if not self.is_valid_task_function(func):
            raise InvalidTask(func)

        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}

        task = Task(
            id=get_random_string(10),
            status=TaskStatus.NEW,
            result=None,
            queued_at=timezone.now(),
            completed_at=None,
            raw=None,
            priority=priority,
            func=func,
            args=args,
            kwargs=kwargs,
            when=None,
        )

        self.tasks.append(task)

        return task

    def defer(self, func, *, when, priority=None, args=None, kwargs=None):
        if not self.is_valid_task_function(func):
            raise InvalidTask(func)

        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}

        task = Task(
            id=get_random_string(10),
            status=TaskStatus.NEW,
            result=None,
            queued_at=timezone.now(),
            completed_at=None,
            raw=None,
            priority=priority,
            func=func,
            args=args,
            kwargs=kwargs,
            when=when,
        )

        self.tasks.append(task)

        return task

    def get_task(self, task_id):
        try:
            return next(task for task in self.tasks if task.id == task_id)
        except StopIteration:
            raise TaskDoesNotExist()

    def clear(self):
        self.tasks.clear()
