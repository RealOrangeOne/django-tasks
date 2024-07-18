import logging
from inspect import iscoroutinefunction
from typing import TypeVar
from uuid import uuid4

from asgiref.sync import async_to_sync
from django.utils import timezone
from typing_extensions import ParamSpec

from django_tasks.task import ResultStatus, Task, TaskResult
from django_tasks.utils import exception_to_dict, json_normalize

from .base import BaseTaskBackend

logger = logging.getLogger(__name__)


T = TypeVar("T")
P = ParamSpec("P")


class ImmediateBackend(BaseTaskBackend):
    supports_async_task = True

    def enqueue(
        self, task: Task[P, T], args: P.args, kwargs: P.kwargs
    ) -> TaskResult[T]:
        self.validate_task(task)

        calling_task_func = (
            async_to_sync(task.func) if iscoroutinefunction(task.func) else task.func
        )

        enqueued_at = timezone.now()
        started_at = timezone.now()
        result_id = str(uuid4())
        try:
            result = json_normalize(calling_task_func(*args, **kwargs))
            status = ResultStatus.COMPLETE
        except BaseException as e:
            try:
                result = exception_to_dict(e)
            except Exception:
                logger.exception("Task id=%s unable to save exception", result_id)
                result = None

            # Use `.exception` to integrate with error monitoring tools (eg Sentry)
            logger.exception("Task execution failed: %s", e)
            status = ResultStatus.FAILED

            # If the user tried to terminate, let them
            if isinstance(e, KeyboardInterrupt):
                raise

        task_result = TaskResult[T](
            task=task,
            id=result_id,
            status=status,
            enqueued_at=enqueued_at,
            started_at=started_at,
            finished_at=timezone.now(),
            args=json_normalize(args),
            kwargs=json_normalize(kwargs),
            backend=self.alias,
        )

        task_result._result = result

        return task_result
