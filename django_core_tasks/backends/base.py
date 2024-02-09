from asgiref.sync import sync_to_async


class BaseTaskBackend:
    def __init__(self, options):
        pass

    def supports_defer(self):
        """
        Does this backend support `defer`?
        """
        return getattr(self, "defer") != BaseTaskBackend.defer

    def supports_enqueue(self):
        """
        Does this backend support `enqueue`?
        """
        return getattr(self, "enqueue") != BaseTaskBackend.enqueue

    async def aenqueue(self, func, *, priority, args=None, kwargs=None):
      """
      Queue up a task function (or coroutine) to be executed
      """
      return await sync_to_async(self.enqueue, thread_sensitive=True)(
        func, priority, args, kwargs
      )

    async def adefer(self, func, *, priority, when, args=None, kwargs=None):
      """
      Add a task function (or coroutine) to be completed at a specific time
      """
      return await sync_to_async(self.defer, thread_sensitive=True)(
        func, priority, when, args, kwargs
      )

    def enqueue(self, func, *, priority, args=None, kwargs=None):
      """
      Queue up a task function to be executed
      """
      raise NotImplementedError("This backend does not support `enqueue`.")

    def defer(self, func, *, priority, when, args=None, kwargs=None):
      """
      Add a task to be completed at a specific time
      """
      raise NotImplementedError("This backend does not support `defer`.")


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
