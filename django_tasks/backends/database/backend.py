from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Iterable, TypeVar

import django
from django.apps import apps
from django.core.checks import messages
from django.core.exceptions import ValidationError
from django.db import connections, router, transaction
from typing_extensions import ParamSpec

from django_tasks.backends.base import BaseTaskBackend
from django_tasks.exceptions import TaskRunDoesNotExist
from django_tasks.task import Task
from django_tasks.task import TaskRun as BaseTaskRun
from django_tasks.utils import json_normalize

if TYPE_CHECKING:
    from .models import DBTaskRun

T = TypeVar("T")
P = ParamSpec("P")


@dataclass
class TaskRun(BaseTaskRun[T]):
    db_task_run: "DBTaskRun"


class DatabaseBackend(BaseTaskBackend):
    supports_async_task = True
    supports_get_task_run = True
    supports_defer = True

    def _task_to_db_task(
        self, task: Task[P, T], args: P.args, kwargs: P.kwargs
    ) -> "DBTaskRun":
        from .models import DBTaskRun

        return DBTaskRun(
            args_kwargs=json_normalize({"args": args, "kwargs": kwargs}),
            priority=task.priority,
            task_path=task.module_path,
            queue_name=task.queue_name,
            run_after=task.run_after,
            backend_name=self.alias,
        )

    def enqueue(self, task: Task[P, T], args: P.args, kwargs: P.kwargs) -> TaskRun[T]:
        self.validate_task(task)

        db_result = self._task_to_db_task(task, args, kwargs)

        if self._get_enqueue_on_commit_for_task(task):
            transaction.on_commit(db_result.save)
        else:
            db_result.save()

        return db_result.task_run

    def get_task_run(self, result_id: str) -> TaskRun:
        from .models import DBTaskRun

        try:
            return DBTaskRun.objects.get(id=result_id).task_run
        except (DBTaskRun.DoesNotExist, ValidationError) as e:
            raise TaskRunDoesNotExist(result_id) from e

    async def aget_task_run(self, result_id: str) -> TaskRun:
        from .models import DBTaskRun

        try:
            return (await DBTaskRun.objects.aget(id=result_id)).task_run
        except (DBTaskRun.DoesNotExist, ValidationError) as e:
            raise TaskRunDoesNotExist(result_id) from e

    def check(self, **kwargs: Any) -> Iterable[messages.CheckMessage]:
        from .models import DBTaskRun
        from .utils import connection_requires_manual_exclusive_transaction

        yield from super().check(**kwargs)

        backend_name = self.__class__.__name__

        if not apps.is_installed("django_tasks.backends.database"):
            yield messages.CheckMessage(
                messages.ERROR,
                f"{backend_name} configured as django_tasks backend, but database app not installed",
                "Insert 'django_tasks.backends.database' in INSTALLED_APPS",
            )

        db_connection = connections[router.db_for_read(DBTaskRun)]
        # Manually called to set `transaction_mode`
        db_connection.get_connection_params()
        if (
            # Versions below 5.1 can't be configured, so always assume exclusive transactions
            django.VERSION >= (5, 1)
            and connection_requires_manual_exclusive_transaction(db_connection)
        ):
            yield messages.CheckMessage(
                messages.ERROR,
                f"{backend_name} is using SQLite non-exclusive transactions",
                f"Set settings.DATABASES[{db_connection.alias!r}]['OPTIONS']['transaction_mode'] to 'EXCLUSIVE'",
            )
