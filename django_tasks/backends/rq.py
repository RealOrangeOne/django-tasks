from collections.abc import Iterable
from types import TracebackType
from typing import Any, Optional, TypeVar

import django_rq
from django.apps import apps
from django.core.checks import messages
from django.core.exceptions import SuspiciousOperation
from django.db import transaction
from django.utils.functional import cached_property
from redis.client import Redis
from rq.job import Callback, JobStatus
from rq.job import Job as BaseJob
from rq.registry import ScheduledJobRegistry
from rq.results import Result
from typing_extensions import ParamSpec

from django_tasks.backends.base import BaseTaskBackend
from django_tasks.exceptions import ResultDoesNotExist
from django_tasks.signals import task_enqueued, task_finished, task_started
from django_tasks.task import (
    DEFAULT_PRIORITY,
    MAX_PRIORITY,
    ResultStatus,
    Task,
    TaskContext,
    TaskError,
    TaskResult,
)
from django_tasks.utils import get_module_path, get_random_id

T = TypeVar("T")
P = ParamSpec("P")

RQ_STATUS_TO_RESULT_STATUS = {
    JobStatus.QUEUED: ResultStatus.READY,
    JobStatus.FINISHED: ResultStatus.SUCCEEDED,
    JobStatus.FAILED: ResultStatus.FAILED,
    JobStatus.STARTED: ResultStatus.RUNNING,
    JobStatus.DEFERRED: ResultStatus.READY,
    JobStatus.SCHEDULED: ResultStatus.READY,
    JobStatus.STOPPED: ResultStatus.FAILED,
    JobStatus.CANCELED: ResultStatus.FAILED,
    None: ResultStatus.READY,
}


class Job(BaseJob):
    def perform(self) -> Any:
        assert self.worker_name is not None
        self.meta.setdefault("_django_tasks_worker_ids", []).append(self.worker_name)
        self.save_meta()  # type: ignore[no-untyped-call]

        task_started.send(
            type(self.task_result.task.get_backend()), task_result=self.task_result
        )

        return super().perform()

    def _execute(self) -> Any:
        """
        Shim RQ's `Job` to call the underlying `Task` function.
        """
        try:
            if self.func.takes_context:
                return self.func.call(
                    TaskContext(task_result=self.task_result), *self.args, **self.kwargs
                )
            return self.func.call(*self.args, **self.kwargs)
        finally:
            # Clear the task result cache, as it's changed now
            self.__dict__.pop("task_result", None)
            pass

    @property
    def func(self) -> Task:
        func = super().func

        if not isinstance(func, Task):
            raise SuspiciousOperation(
                f"Task {self.id} does not point to a Task ({self.func_name})"
            )

        return func

    @cached_property
    def task_result(self) -> TaskResult:
        task: Task = self.func

        scheduled_job_registry = ScheduledJobRegistry(  # type: ignore[no-untyped-call]
            queue=django_rq.get_queue(self.origin)
        )

        if self.is_scheduled:
            run_after = scheduled_job_registry.get_scheduled_time(self)
        else:
            run_after = None

        task_result: TaskResult = TaskResult(
            task=task.using(
                priority=DEFAULT_PRIORITY,
                queue_name=self.origin,
                run_after=run_after,
                backend=self.meta["backend_name"],
            ),
            id=self.id,
            status=RQ_STATUS_TO_RESULT_STATUS[self.get_status()],
            enqueued_at=self.enqueued_at,
            started_at=self.started_at,
            last_attempted_at=self.started_at,
            finished_at=self.ended_at,
            args=list(self.args),
            kwargs=self.kwargs,
            backend=self.meta["backend_name"],
            errors=[],
            worker_ids=self.meta.get("_django_tasks_worker_ids", []),
        )

        exception_classes = self.meta.get("_django_tasks_exceptions", []).copy()

        rq_results = self.results()

        for rq_result in rq_results:
            if rq_result.type == Result.Type.FAILED:
                task_result.errors.append(
                    TaskError(
                        exception_class_path=exception_classes.pop(),
                        traceback=rq_result.exc_string,  # type: ignore[arg-type]
                    )
                )

        if rq_results:
            object.__setattr__(task_result, "_return_value", rq_results[0].return_value)
            object.__setattr__(
                task_result, "last_attempted_at", rq_results[0].created_at
            )

        return task_result


def failed_callback(
    job: Job,
    connection: Optional[Redis],
    exception_class: type[Exception],
    exception_value: Exception,
    traceback: TracebackType,
) -> None:
    # Smuggle the exception class through meta
    job.meta.setdefault("_django_tasks_exceptions", []).append(
        get_module_path(exception_class)
    )
    job.save_meta()  # type: ignore[no-untyped-call]

    task_result = job.task_result

    object.__setattr__(task_result, "status", ResultStatus.FAILED)

    task_finished.send(type(task_result.task.get_backend()), task_result=task_result)


def success_callback(job: Job, connection: Optional[Redis], result: Any) -> None:
    task_result = job.task_result

    object.__setattr__(task_result, "status", ResultStatus.SUCCEEDED)

    task_finished.send(type(task_result.task.get_backend()), task_result=task_result)


class RQBackend(BaseTaskBackend):
    supports_async_task = True
    supports_get_result = True
    supports_defer = True

    def __init__(self, alias: str, params: dict) -> None:
        super().__init__(alias, params)

        if not self.queues:
            self.queues = set(django_rq.settings.QUEUES.keys())

    def enqueue(
        self,
        task: Task[P, T],
        args: P.args,  # type:ignore[valid-type]
        kwargs: P.kwargs,  # type:ignore[valid-type]
    ) -> TaskResult[T]:
        self.validate_task(task)

        queue = django_rq.get_queue(task.queue_name, job_class=Job)

        task_result = TaskResult[T](
            task=task,
            id=get_random_id(),
            status=ResultStatus.READY,
            enqueued_at=None,
            started_at=None,
            last_attempted_at=None,
            finished_at=None,
            args=args,
            kwargs=kwargs,
            backend=self.alias,
            errors=[],
            worker_ids=[],
        )

        job = queue.create_job(
            task.module_path,
            args=args,
            kwargs=kwargs,
            job_id=task_result.id,
            status=JobStatus.SCHEDULED if task.run_after else JobStatus.QUEUED,
            on_failure=Callback(failed_callback),
            on_success=Callback(success_callback),
            meta={"backend_name": self.alias},
        )

        def save_result() -> None:
            nonlocal job
            if task.run_after is None:
                job = queue.enqueue_job(job, at_front=task.priority == MAX_PRIORITY)
            else:
                job = queue.schedule_job(job, task.run_after)

            object.__setattr__(task_result, "enqueued_at", job.enqueued_at)
            task_enqueued.send(type(self), task_result=task_result)

        if self._get_enqueue_on_commit_for_task(task):
            transaction.on_commit(save_result)
        else:
            save_result()

        return task_result

    def _get_queues(self) -> list[django_rq.queues.DjangoRQ]:
        return django_rq.queues.get_queues(*self.queues, job_class=Job)  # type: ignore[no-any-return,no-untyped-call]

    def _get_job(self, job_id: str) -> Optional[Job]:
        for queue in self._get_queues():
            job = queue.fetch_job(job_id)
            if job is not None:
                return job  # type: ignore[return-value]

        return None

    def get_result(self, result_id: str) -> TaskResult:
        job = self._get_job(result_id)

        if job is None:
            raise ResultDoesNotExist(result_id)

        return job.task_result

    def check(self, **kwargs: Any) -> Iterable[messages.CheckMessage]:
        yield from super().check(**kwargs)

        backend_name = self.__class__.__name__

        if not apps.is_installed("django_rq"):
            yield messages.Error(
                f"{backend_name} configured as django_tasks backend, but django_rq app not installed",
                "Insert 'django_rq' in INSTALLED_APPS",
            )

        for queue_name in self.queues:
            try:
                django_rq.get_queue(queue_name)
            except KeyError:
                yield messages.Error(
                    f"{queue_name!r} is not configured for django-rq",
                    f"Add {queue_name!r} to RQ_QUEUES",
                )
