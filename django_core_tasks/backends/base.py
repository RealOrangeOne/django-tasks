from asgiref.sync import sync_to_async
from django.utils.module_loading import import_string
import inspect


class BaseTaskBackend:
    def __init__(self, options):
        pass

    def supports_defer(self):
        """
        Does this backend support `defer`?
        """
        return getattr(type(self), "defer") != BaseTaskBackend.defer

    def supports_enqueue(self):
        """
        Does this backend support `enqueue`?
        """
        return getattr(type(self), "enqueue") != BaseTaskBackend.enqueue

    def is_valid_task_function(self, func):
        """
        Is the provided callable valid as a task function
        """
        if not inspect.isfunction(func) and not inspect.isbuiltin(func):
            return False

        try:
            imported_func = import_string(f"{func.__module__}.{func.__qualname__}")
        except (AttributeError, ModuleNotFoundError):
            return False

        if imported_func != func:
            return False

        return True

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
