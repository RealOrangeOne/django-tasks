from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar

from django.apps import apps
from django.core import checks
from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils.version import PY311
from typing_extensions import ParamSpec

from django_tasks.backends.base import BaseTaskBackend
from django_tasks.base import Task
from django_tasks.base import TaskResult as BaseTaskResult
from django_tasks.exceptions import TaskResultDoesNotExist
from django_tasks.signals import task_enqueued
from django_tasks.utils import normalize_json

if TYPE_CHECKING:
    from .models import DBTaskResult

T = TypeVar("T")
P = ParamSpec("P")


@dataclass(frozen=True, slots=PY311, kw_only=True)  # type: ignore[literal-required]
class TaskResult(BaseTaskResult[T]):
    db_result: "DBTaskResult"


class DatabaseBackend(BaseTaskBackend):
    supports_async_task = True
    supports_get_result = True
    supports_defer = True
    supports_priority = True

    def _task_to_db_task(
        self,
        task: Task[P, T],
        args: P.args,  # type:ignore[valid-type]
        kwargs: P.kwargs,  # type:ignore[valid-type]
    ) -> "DBTaskResult":
        from .models import DBTaskResult

        return DBTaskResult(
            args_kwargs=normalize_json({"args": args, "kwargs": kwargs}),
            priority=task.priority,
            task_path=task.module_path,
            queue_name=task.queue_name,
            run_after=task.run_after,  # type: ignore[misc]
            backend_name=self.alias,
        )

    def enqueue(
        self,
        task: Task[P, T],
        args: P.args,  # type:ignore[valid-type]
        kwargs: P.kwargs,  # type:ignore[valid-type]
    ) -> TaskResult[T]:
        self.validate_task(task)

        db_result = self._task_to_db_task(task, args, kwargs)

        def save_result() -> None:
            db_result.save()
            task_enqueued.send(type(self), task_result=db_result.task_result)

        if self._get_enqueue_on_commit_for_task(task):
            transaction.on_commit(save_result)
        else:
            save_result()

        return db_result.task_result

    def get_result(self, result_id: str) -> TaskResult:
        from .models import DBTaskResult

        try:
            return DBTaskResult.objects.get(id=result_id).task_result
        except (DBTaskResult.DoesNotExist, ValidationError) as e:
            raise TaskResultDoesNotExist(result_id) from e

    async def aget_result(self, result_id: str) -> TaskResult:
        from .models import DBTaskResult

        try:
            return (await DBTaskResult.objects.aget(id=result_id)).task_result
        except (DBTaskResult.DoesNotExist, ValidationError) as e:
            raise TaskResultDoesNotExist(result_id) from e

    def check(self, **kwargs: Any) -> Iterable[checks.CheckMessage]:
        yield from super().check(**kwargs)

        backend_name = self.__class__.__name__

        if not apps.is_installed("django_tasks.backends.database"):
            yield checks.Error(
                f"{backend_name} configured as django_tasks backend, but database app not installed",
                "Insert 'django_tasks.backends.database' in INSTALLED_APPS",
            )
