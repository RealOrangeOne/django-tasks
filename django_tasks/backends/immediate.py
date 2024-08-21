import logging
from functools import partial
from inspect import iscoroutinefunction
from typing import TypeVar
from uuid import uuid4

from asgiref.sync import async_to_sync
from django.db import transaction
from django.utils import timezone
from typing_extensions import ParamSpec

from django_tasks.task import Task, TaskRun, TaskRunStatus
from django_tasks.utils import exception_to_dict, json_normalize

from .base import BaseTaskBackend

logger = logging.getLogger(__name__)


T = TypeVar("T")
P = ParamSpec("P")


class ImmediateBackend(BaseTaskBackend):
    supports_async_task = True

    def _execute_task(self, task_run: TaskRun) -> None:
        """
        Execute the task for the given `TaskRun`, mutating it with the outcome
        """
        task = task_run.task

        calling_task_func = (
            async_to_sync(task.func) if iscoroutinefunction(task.func) else task.func
        )

        task_run.started_at = timezone.now()
        try:
            task_run._result = json_normalize(
                calling_task_func(*task_run.args, **task_run.kwargs)
            )
        except BaseException as e:
            task_run.finished_at = timezone.now()
            try:
                task_run._result = exception_to_dict(e)
            except Exception:
                logger.exception("Task id=%s unable to save exception", task_run.id)
                task_run._result = None

            # Use `.exception` to integrate with error monitoring tools (eg Sentry)
            logger.exception(
                "Task id=%s path=%s state=%s",
                task_run.id,
                task.module_path,
                TaskRunStatus.FAILED,
            )
            task_run.status = TaskRunStatus.FAILED

            # If the user tried to terminate, let them
            if isinstance(e, KeyboardInterrupt):
                raise
        else:
            task_run.finished_at = timezone.now()
            task_run.status = TaskRunStatus.COMPLETE

    def enqueue(self, task: Task[P, T], args: P.args, kwargs: P.kwargs) -> TaskRun[T]:
        self.validate_task(task)

        task_run = TaskRun[T](
            task=task,
            id=str(uuid4()),
            status=TaskRunStatus.NEW,
            enqueued_at=timezone.now(),
            started_at=None,
            finished_at=None,
            args=json_normalize(args),
            kwargs=json_normalize(kwargs),
            backend=self.alias,
        )

        if self._get_enqueue_on_commit_for_task(task) is not False:
            transaction.on_commit(partial(self._execute_task, task_run))
        else:
            self._execute_task(task_run)

        return task_run
