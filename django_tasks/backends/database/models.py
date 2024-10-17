import logging
import uuid
from typing import TYPE_CHECKING, Any, Generic, Optional, TypeVar

import django
from django.core.exceptions import SuspiciousOperation
from django.db import models
from django.db.models import F, Q
from django.db.models.constraints import CheckConstraint
from django.utils import timezone
from django.utils.module_loading import import_string
from django.utils.translation import gettext_lazy as _
from typing_extensions import ParamSpec

from django_tasks.task import (
    DEFAULT_PRIORITY,
    DEFAULT_QUEUE_NAME,
    MAX_PRIORITY,
    MIN_PRIORITY,
    ResultStatus,
    Task,
)
from django_tasks.utils import exception_to_dict, retry

from .utils import normalize_uuid

logger = logging.getLogger("django_tasks.backends.database")

T = TypeVar("T")
P = ParamSpec("P")

if TYPE_CHECKING:
    from .backend import TaskResult

    class GenericBase(Generic[P, T]):
        pass

else:

    class GenericBase:
        """
        https://code.djangoproject.com/ticket/33174
        """

        def __class_getitem__(cls, _):
            return cls


class DBTaskResultQuerySet(models.QuerySet):
    def ready(self) -> "DBTaskResultQuerySet":
        """
        Return tasks which are ready to be processed.
        """
        return self.filter(
            status=ResultStatus.NEW,
        ).filter(models.Q(run_after=None) | models.Q(run_after__lte=timezone.now()))

    def complete(self) -> "DBTaskResultQuerySet":
        return self.filter(status=ResultStatus.COMPLETE)

    def failed(self) -> "DBTaskResultQuerySet":
        return self.filter(status=ResultStatus.FAILED)

    def running(self) -> "DBTaskResultQuerySet":
        return self.filter(status=ResultStatus.RUNNING)

    def finished(self) -> "DBTaskResultQuerySet":
        return self.failed() | self.complete()

    @retry()
    def get_locked(self) -> Optional["DBTaskResult"]:
        """
        Get a job, locking the row and accounting for deadlocks.
        """
        return self.select_for_update(skip_locked=True).first()


class DBTaskResult(GenericBase[P, T], models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    status = models.CharField(
        _("status"),
        choices=ResultStatus.choices,
        default=ResultStatus.NEW,
        max_length=max(len(value) for value in ResultStatus.values),
    )

    enqueued_at = models.DateTimeField(_("enqueued at"), auto_now_add=True)
    started_at = models.DateTimeField(_("started at"), null=True)
    finished_at = models.DateTimeField(_("finished at"), null=True)

    args_kwargs = models.JSONField(_("args kwargs"))

    priority = models.IntegerField(_("priority"), default=DEFAULT_PRIORITY)

    task_path = models.TextField(_("task path"))

    queue_name = models.TextField(_("queue name"), default=DEFAULT_QUEUE_NAME)
    backend_name = models.TextField(_("backend name"))

    run_after = models.DateTimeField(_("run after"), null=True)

    return_value = models.JSONField(_("return value"), default=None, null=True)
    exception_data = models.JSONField(_("exception data"), default=None, null=True)

    objects = DBTaskResultQuerySet.as_manager()

    class Meta:
        ordering = [F("priority").desc(), F("run_after").desc(nulls_last=True)]
        verbose_name = _("Task Result")
        verbose_name_plural = _("Task Results")

        indexes = [
            models.Index(fields=["status"]),
            models.Index(fields=["queue_name"]),
            models.Index(
                F("priority").desc(),
                F("run_after").desc(nulls_last=True),
                name="idx_task_ordering",
            ),
        ]

        if django.VERSION >= (5, 1):
            constraints = [
                CheckConstraint(
                    condition=Q(priority__range=(MIN_PRIORITY, MAX_PRIORITY)),
                    name="priority_range",
                )
            ]
        else:
            constraints = [
                CheckConstraint(
                    check=Q(priority__range=(MIN_PRIORITY, MAX_PRIORITY)),
                    name="priority_range",
                )
            ]

    @property
    def task(self) -> Task[P, T]:
        task = import_string(self.task_path)

        if not isinstance(task, Task):
            raise SuspiciousOperation(
                f"Task {self.id} does not point to a Task ({self.task_path})"
            )

        return task.using(
            priority=self.priority,
            queue_name=self.queue_name,
            run_after=self.run_after,
            backend=self.backend_name,
        )

    @property
    def task_result(self) -> "TaskResult[T]":
        from .backend import TaskResult

        result = TaskResult[T](
            db_result=self,
            task=self.task,
            id=normalize_uuid(self.id),
            status=ResultStatus[self.status],
            enqueued_at=self.enqueued_at,
            started_at=self.started_at,
            finished_at=self.finished_at,
            args=self.args_kwargs["args"],
            kwargs=self.args_kwargs["kwargs"],
            backend=self.backend_name,
        )

        object.__setattr__(result, "_return_value", self.return_value)
        object.__setattr__(result, "_exception_data", self.exception_data)

        return result

    @retry(backoff_delay=0)
    def claim(self) -> None:
        """
        Mark as job as being run
        """
        self.status = ResultStatus.RUNNING
        self.started_at = timezone.now()
        self.save(update_fields=["status", "started_at"])

    @retry()
    def set_complete(self, return_value: Any) -> None:
        self.status = ResultStatus.COMPLETE
        self.finished_at = timezone.now()
        self.return_value = return_value
        self.exception_data = None
        self.save(
            update_fields=["status", "return_value", "finished_at", "exception_data"]
        )

    @retry()
    def set_failed(self, exc: BaseException) -> None:
        self.status = ResultStatus.FAILED
        self.finished_at = timezone.now()
        try:
            self.exception_data = exception_to_dict(exc)
        except Exception:
            logger.exception("Task id=%s unable to save exception", self.id)
            self.exception_data = None
        self.return_value = None
        self.save(
            update_fields=["status", "finished_at", "exception_data", "return_value"]
        )
