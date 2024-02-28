from .base import BaseTaskBackend
from django_core_tasks.exceptions import InvalidTask, TaskDoesNotExist
from django_core_tasks.task import BaseTask, TaskStatus
from django.utils import timezone
from django.utils.crypto import get_random_string


class DummyTask(BaseTask):
    def __init__(self, func, args, kwargs, priority, when):
        self.func = func
        self.args = args
        self.kwargs = kwargs

        self.task_id = get_random_string(10)

        self.status = TaskStatus.NEW
        self.result = None

        self.queued_at = timezone.now()
        self.completed_at = None
        self.when = when


class DummyBackend(BaseTaskBackend):
    """
    Stores tasks for execution later
    """

    def __init__(self, options):
        super().__init__(options)

        self.tasks = {}

    def enqueue(self, func, *, priority=None, args=None, kwargs=None):
        if not self.is_valid_task_function(func):
            raise InvalidTask(func)

        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}

        task = DummyTask(func, args, kwargs, priority, when=None)

        self.tasks[task.task_id] = task

        return task

    def defer(self, func, *, when, priority=None, args=None, kwargs=None):
        if not self.is_valid_task_function(func):
            raise InvalidTask(func)

        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}

        task = DummyTask(func, args, kwargs, priority, when=when)

        self.tasks[task.task_id] = task

        return task

    def get_task(self, task_id):
        try:
            return self.tasks[task_id]
        except KeyError:
            raise TaskDoesNotExist()

    def clear(self):
        self.tasks.clear()
