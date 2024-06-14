from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, List, TypeVar

from django.apps import apps
from django.core.checks import ERROR, CheckMessage
from django.core.exceptions import ValidationError
from typing_extensions import ParamSpec

from django_tasks.backends.base import BaseTaskBackend
from django_tasks.exceptions import ResultDoesNotExist
from django_tasks.task import Task
from django_tasks.task import TaskResult as BaseTaskResult
from django_tasks.utils import json_normalize

if TYPE_CHECKING:
    from .models import DBTaskResult

T = TypeVar("T")
P = ParamSpec("P")


@dataclass
class TaskResult(BaseTaskResult[T]):
    db_result: "DBTaskResult"

    def refresh(self) -> None:
        self.db_result.refresh_from_db()
        for attr, value in asdict(self.db_result.task_result).items():
            setattr(self, attr, value)

    async def arefresh(self) -> None:
        await self.db_result.arefresh_from_db()
        for attr, value in asdict(self.db_result.task_result).items():
            setattr(self, attr, value)


class DatabaseBackend(BaseTaskBackend):
    supports_async_task = True
    supports_get_result = True
    supports_defer = True

    def _task_to_db_task(
        self, task: Task[P, T], args: P.args, kwargs: P.kwargs
    ) -> "DBTaskResult":
        from .models import DBTaskResult

        return DBTaskResult(
            args_kwargs=json_normalize({"args": args, "kwargs": kwargs}),
            priority=task.priority,
            task_path=task.module_path,
            queue_name=task.queue_name,
            run_after=task.run_after,
            backend_name=self.alias,
        )

    def enqueue(
        self, task: Task[P, T], args: P.args, kwargs: P.kwargs
    ) -> TaskResult[T]:
        self.validate_task(task)

        db_result = self._task_to_db_task(task, args, kwargs)

        db_result.save()

        return db_result.task_result

    async def aenqueue(
        self, task: Task[P, T], args: P.args, kwargs: P.kwargs
    ) -> TaskResult[T]:
        self.validate_task(task)

        db_result = self._task_to_db_task(task, args, kwargs)

        await db_result.asave()

        return db_result.task_result

    def get_result(self, result_id: str) -> TaskResult:
        from .models import DBTaskResult

        try:
            return DBTaskResult.objects.get(id=result_id).task_result
        except (DBTaskResult.DoesNotExist, ValidationError) as e:
            raise ResultDoesNotExist(result_id) from e

    async def aget_result(self, result_id: str) -> TaskResult:
        from .models import DBTaskResult

        try:
            return (await DBTaskResult.objects.aget(id=result_id)).task_result
        except (DBTaskResult.DoesNotExist, ValidationError) as e:
            raise ResultDoesNotExist(result_id) from e

    def check(self, **kwargs: Any) -> List[CheckMessage]:
        if not apps.is_installed("django_tasks.backends.database"):
            backend_name = self.__class__.__name__

            return [
                CheckMessage(
                    ERROR,
                    f"{backend_name} configured as django_tasks backend, but database app not installed",
                    "Insert 'django_tasks.backends.database' in INSTALLED_APPS",
                )
            ]

        return []
