import inspect
from datetime import timedelta

from asgiref.sync import sync_to_async
from django.utils import timezone
from django.utils.module_loading import import_string

from django_core_tasks.exceptions import InvalidTaskError
from django_core_tasks.utils import is_marked_task_func


class BaseTaskBackend:
    def __init__(self, options):
        pass

    def supports_defer(self):
        """
        Does this backend support `defer`?
        """
        return (
            getattr(type(self), "defer", None) != BaseTaskBackend.defer
            or getattr(type(self), "bulk_defer", None) != BaseTaskBackend.bulk_defer
        )

    def supports_enqueue(self):
        """
        Does this backend support `enqueue`?
        """
        return (
            getattr(type(self), "enqueue", None) != BaseTaskBackend.enqueue
            or getattr(type(self), "bulk_enqueue", None) != BaseTaskBackend.bulk_enqueue
        )

    def is_valid_task_function(self, func):
        """
        Is the provided callable valid as a task function
        """
        if not inspect.isfunction(func) and not inspect.isbuiltin(func):
            return False

        if not is_marked_task_func(func):
            return False

        try:
            imported_func = import_string(f"{func.__module__}.{func.__qualname__}")
        except (AttributeError, ModuleNotFoundError):
            return False

        if imported_func != func:
            return False

        return True

    def validate_candidate(self, task_candidate):
        """
        Validate a task candidate
        """
        if not self.is_valid_task_function(task_candidate.func):
            raise InvalidTaskError(task_candidate.func)

        if task_candidate.priority is not None and task_candidate.priority < 1:
            raise ValueError("priority must be positive")

        if (
            task_candidate.when is not None
            and task_candidate.when < timezone.now() - timedelta(seconds=1)
        ):
            raise ValueError("when must be in the future")

    def validate_candidates(self, task_candidates):
        """
        Validate a set of task candidates
        """
        for task_candidate in task_candidates:
            self.validate_candidate(task_candidate)

    async def aenqueue(self, func, *, priority=None, args=None, kwargs=None):
        """
        Queue up a task function (or coroutine) to be executed
        """
        return await sync_to_async(self.enqueue, thread_sensitive=True)(
            func, priority=priority, args=args, kwargs=kwargs
        )

    async def adefer(self, func, *, when, priority=None, args=None, kwargs=None):
        """
        Add a task function (or coroutine) to be completed at a specific time
        """
        return await sync_to_async(self.defer, thread_sensitive=True)(
            func, when=when, priority=priority, args=args, kwargs=kwargs
        )

    def enqueue(self, func, *, priority=None, args=None, kwargs=None):
        """
        Queue up a task function to be executed
        """
        raise NotImplementedError("This backend does not support `enqueue`.")

    def defer(self, func, *, when, priority=None, args=None, kwargs=None):
        """
        Add a task to be completed at a specific time
        """
        raise NotImplementedError("This backend does not support `defer`.")

    def bulk_defer(self, task_candidates):
        """
        Add tasks to be completed at a specific time
        """
        # If there's no `defer` implementation, there's no `bulk_defer` implementation
        if getattr(type(self), "defer", None) == BaseTaskBackend.defer:
            raise NotImplementedError("This backend does not support `bulk_defer`.")

        self.validate_candidates(task_candidates)

        if not all(task_candidate.when for task_candidate in task_candidates):
            raise ValueError("Deferred tasks must define when")

        tasks = []
        for task_candidate in task_candidates:
            tasks.append(
                self.defer(
                    func=task_candidate.func,
                    when=task_candidate.when,
                    priority=task_candidate.priority,
                    args=task_candidate.args,
                    kwargs=task_candidate.kwargs,
                )
            )
        return tasks

    def bulk_enqueue(self, task_candidates):
        """
        Queue up task functions to be executed
        """
        # If there's no `enqueue` implementation, there's no `bulk_enqueue` implementation
        if getattr(type(self), "enqueue", None) == BaseTaskBackend.enqueue:
            raise NotImplementedError("This backend does not support `bulk_enqueue`.")

        self.validate_candidates(task_candidates)

        if any(task_candidate.when for task_candidate in task_candidates):
            raise ValueError("Enqueued tasks must not define when")

        tasks = []
        for task_candidate in task_candidates:
            tasks.append(
                self.enqueue(
                    func=task_candidate.func,
                    priority=task_candidate.priority,
                    args=task_candidate.args,
                    kwargs=task_candidate.kwargs,
                )
            )
        return tasks

    async def abulk_enqueue(self, task_candidates):
        """
        Queue up a task function (or coroutine) to be executed
        """
        return await sync_to_async(self.bulk_enqueue, thread_sensitive=True)(
            task_candidates
        )

    async def abulk_defer(self, task_candidates):
        """
        Add a task function (or coroutine) to be completed at a specific time
        """
        return await sync_to_async(self.bulk_defer, thread_sensitive=True)(
            task_candidates
        )

    def get_task(self, task_id):
        """
        Get the handle to a task which has already been scheduled
        """
        raise NotImplementedError(
            "This backend does not support retrieving existing tasks."
        )

    async def aget_task(self, task_id):
        """
        Get the handle to a task which has already been scheduled
        """
        return await sync_to_async(self.get_task, thread_sensitive=True)(task_id)

    def close(self, **kwargs):
        """
        Close any open connections
        """
        pass

    async def aclose(self, **kwargs):
        """
        Close any open connections
        """
        pass
