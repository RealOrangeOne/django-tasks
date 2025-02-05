import json
import logging
import os
import signal
import subprocess
import sys
import time
import uuid
from contextlib import redirect_stderr
from datetime import timedelta
from functools import partial
from io import StringIO
from typing import Any, List, Optional, Sequence, Union, cast
from unittest import mock, skipIf

import django
from django.core.exceptions import SuspiciousOperation
from django.core.management import call_command, execute_from_command_line
from django.db import connection, connections, transaction
from django.db.models import QuerySet
from django.db.utils import IntegrityError, OperationalError
from django.test import TransactionTestCase, override_settings
from django.test.testcases import _deferredSkip  # type:ignore[attr-defined]
from django.urls import reverse
from django.utils import timezone

from django_tasks import ResultStatus, Task, default_task_backend, tasks
from django_tasks.backends.database import DatabaseBackend
from django_tasks.backends.database.management.commands.prune_db_task_results import (
    logger as prune_db_tasks_logger,
)
from django_tasks.backends.database.models import DBTaskResult, DBWorkerPing
from django_tasks.backends.database.utils import (
    connection_requires_manual_exclusive_transaction,
    exclusive_transaction,
    normalize_uuid,
)
from django_tasks.exceptions import ResultDoesNotExist
from django_tasks.utils import get_random_id
from tests import tasks as test_tasks


def skipIfInMemoryDB() -> Any:  # noqa:N802
    return _deferredSkip(
        lambda: connection.vendor == "sqlite" and connection.is_in_memory_db(),  # type:ignore[attr-defined]
        "Tests cannot run on in-memory DB",
        "skipIfInMemoryDB",
    )


@override_settings(
    TASKS={
        "default": {
            "BACKEND": "django_tasks.backends.database.DatabaseBackend",
            "QUEUES": ["default", "queue-1"],
        }
    }
)
class DatabaseBackendTestCase(TransactionTestCase):
    def get_task_count_in_new_connection(self) -> int:
        """
        See what other connections see
        """
        new_connection = connections.create_connection("default")
        try:
            with new_connection.cursor() as c:
                c.execute(str(DBTaskResult.objects.values("id").query))
                return len(c.fetchall())
        finally:
            new_connection.close()

    def test_using_correct_backend(self) -> None:
        self.assertEqual(default_task_backend, tasks["default"])
        self.assertIsInstance(tasks["default"], DatabaseBackend)

    def test_enqueue_task(self) -> None:
        for task in [test_tasks.noop_task, test_tasks.noop_task_async]:
            with self.subTest(task), self.assertNumQueries(1):
                result = cast(Task, task).enqueue(1, two=3)

                self.assertEqual(result.status, ResultStatus.NEW)
                self.assertFalse(result.is_finished)
                self.assertIsNone(result.started_at)
                self.assertIsNone(result.finished_at)
                with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
                    result.return_value  # noqa:B018
                self.assertEqual(result.task, task)
                self.assertEqual(result.args, [1])
                self.assertEqual(result.kwargs, {"two": 3})

    async def test_enqueue_task_async(self) -> None:
        for task in [test_tasks.noop_task, test_tasks.noop_task_async]:
            with self.subTest(task):
                result = await cast(Task, task).aenqueue()

                self.assertEqual(result.status, ResultStatus.NEW)
                self.assertFalse(result.is_finished)
                self.assertIsNone(result.started_at)
                self.assertIsNone(result.finished_at)
                with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
                    result.return_value  # noqa:B018
                self.assertEqual(result.task, task)
                self.assertEqual(result.args, [])
                self.assertEqual(result.kwargs, {})

    def test_get_result(self) -> None:
        with self.assertNumQueries(1):
            result = default_task_backend.enqueue(test_tasks.noop_task, [], {})

        with self.assertNumQueries(1):
            new_result = default_task_backend.get_result(result.id)

        self.assertEqual(result, new_result)

    async def test_get_result_async(self) -> None:
        result = await default_task_backend.aenqueue(test_tasks.noop_task, [], {})

        new_result = await default_task_backend.aget_result(result.id)

        self.assertEqual(result, new_result)

    def test_refresh_result(self) -> None:
        result = default_task_backend.enqueue(
            test_tasks.calculate_meaning_of_life, (), {}
        )

        DBTaskResult.objects.all().update(
            status=ResultStatus.SUCCEEDED,
            started_at=timezone.now(),
            finished_at=timezone.now(),
            return_value=42,
        )

        self.assertEqual(result.status, ResultStatus.NEW)
        self.assertFalse(result.is_finished)
        self.assertIsNone(result.started_at)
        self.assertIsNone(result.finished_at)
        with self.assertNumQueries(1):
            result.refresh()
        self.assertIsNotNone(result.started_at)
        self.assertIsNotNone(result.finished_at)
        self.assertEqual(result.status, ResultStatus.SUCCEEDED)
        self.assertTrue(result.is_finished)
        self.assertEqual(result.return_value, 42)

    async def test_refresh_result_async(self) -> None:
        result = await default_task_backend.aenqueue(
            test_tasks.calculate_meaning_of_life, (), {}
        )

        await DBTaskResult.objects.all().aupdate(
            status=ResultStatus.SUCCEEDED,
            started_at=timezone.now(),
            finished_at=timezone.now(),
            return_value=42,
        )

        self.assertEqual(result.status, ResultStatus.NEW)
        self.assertFalse(result.is_finished)
        self.assertIsNone(result.started_at)
        self.assertIsNone(result.finished_at)
        await result.arefresh()
        self.assertIsNotNone(result.started_at)
        self.assertIsNotNone(result.finished_at)
        self.assertEqual(result.status, ResultStatus.SUCCEEDED)
        self.assertTrue(result.is_finished)
        self.assertEqual(result.return_value, 42)

    def test_get_missing_result(self) -> None:
        with self.assertRaises(ResultDoesNotExist):
            default_task_backend.get_result(str(uuid.uuid4()))

    async def test_async_get_missing_result(self) -> None:
        with self.assertRaises(ResultDoesNotExist):
            await default_task_backend.aget_result(str(uuid.uuid4()))

    def test_invalid_uuid(self) -> None:
        with self.assertRaises(ResultDoesNotExist):
            default_task_backend.get_result("123")

    async def test_async_invalid_uuid(self) -> None:
        with self.assertRaises(ResultDoesNotExist):
            await default_task_backend.aget_result("123")

    def test_meaning_of_life_view(self) -> None:
        for url in [
            reverse("meaning-of-life"),
            reverse("meaning-of-life-async"),
        ]:
            with self.subTest(url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 200)

                data = json.loads(response.content)

                self.assertEqual(data["result"], None)
                self.assertEqual(data["status"], ResultStatus.NEW)

                result = default_task_backend.get_result(data["result_id"])
                self.assertEqual(result.status, ResultStatus.NEW)

    def test_get_result_from_different_request(self) -> None:
        response = self.client.get(reverse("meaning-of-life"))
        self.assertEqual(response.status_code, 200)

        data = json.loads(response.content)
        result_id = data["result_id"]

        response = self.client.get(reverse("result", args=[result_id]))
        self.assertEqual(response.status_code, 200)

        self.assertEqual(
            json.loads(response.content),
            {"result_id": result_id, "result": None, "status": ResultStatus.NEW},
        )

    def test_invalid_task_path(self) -> None:
        db_task_result = DBTaskResult.objects.create(
            args_kwargs={"args": [["exit", "1"]], "kwargs": {}},
            task_path="subprocess.check_output",
            backend_name="default",
        )

        with self.assertRaisesMessage(
            SuspiciousOperation,
            f"Task {db_task_result.id} does not point to a Task ({db_task_result.task_path})",
        ):
            _ = db_task_result.task

    def test_missing_task_path(self) -> None:
        db_task_result = DBTaskResult.objects.create(
            args_kwargs={"args": [], "kwargs": {}},
            task_path="missing.func",
            backend_name="default",
        )

        with self.assertRaises(ImportError):
            _ = db_task_result.task

    def test_task_name(self) -> None:
        for task_path, expected_task_name in [
            ("tests.tasks.noop_task", "noop_task"),
            ("tests.tasks.task_not_found", "task_not_found"),
            ("tests.tasks.module_not_found.module_not_found", "module_not_found"),
            ("unexpected_function", "unexpected_function"),
        ]:
            with self.subTest(task_path):
                db_task_result = DBTaskResult.objects.create(
                    args_kwargs={"args": [], "kwargs": {}},
                    task_path=task_path,
                    backend_name="default",
                )

                self.assertEqual(db_task_result.task_name, expected_task_name)

    def test_check(self) -> None:
        errors = list(default_task_backend.check())

        self.assertEqual(len(errors), 0, errors)

    @override_settings(INSTALLED_APPS=[])
    def test_database_backend_app_missing(self) -> None:
        errors = list(default_task_backend.check())

        self.assertEqual(len(errors), 1)
        self.assertIn("django_tasks.backends.database", errors[0].hint)  # type:ignore[arg-type]

    def test_priority_range_check(self) -> None:
        with self.assertRaises(IntegrityError):
            DBTaskResult.objects.create(
                task_path="", backend_name="default", priority=-101, args_kwargs={}
            )

        with self.assertRaises(IntegrityError):
            DBTaskResult.objects.create(
                task_path="", backend_name="default", priority=101, args_kwargs={}
            )

        # Django accepts the float, but only stores an int
        result = DBTaskResult.objects.create(
            task_path="", backend_name="default", priority=3.1, args_kwargs={}
        )
        result.refresh_from_db()
        self.assertEqual(result.priority, 3)

        DBTaskResult.objects.create(
            task_path="", backend_name="default", priority=100, args_kwargs={}
        )
        DBTaskResult.objects.create(
            task_path="", backend_name="default", priority=-100, args_kwargs={}
        )
        DBTaskResult.objects.create(
            task_path="", backend_name="default", priority=0, args_kwargs={}
        )

    @override_settings(
        TASKS={
            "default": {
                "BACKEND": "django_tasks.backends.database.DatabaseBackend",
                "ENQUEUE_ON_COMMIT": True,
            }
        }
    )
    def test_wait_until_transaction_commit(self) -> None:
        self.assertTrue(default_task_backend.enqueue_on_commit)
        self.assertTrue(
            default_task_backend._get_enqueue_on_commit_for_task(test_tasks.noop_task)
        )

        with transaction.atomic():
            result = test_tasks.noop_task.enqueue()

            self.assertIsNone(result.enqueued_at)

            self.assertEqual(DBTaskResult.objects.count(), 0)
            # SQLite locks the table during this transaction
            if connection.vendor != "sqlite":
                self.assertEqual(self.get_task_count_in_new_connection(), 0)

        if connection.vendor != "sqlite":
            self.assertEqual(self.get_task_count_in_new_connection(), 1)
        result.refresh()
        self.assertIsNotNone(result.enqueued_at)

    @override_settings(
        TASKS={
            "default": {
                "BACKEND": "django_tasks.backends.database.DatabaseBackend",
                "ENQUEUE_ON_COMMIT": False,
            }
        }
    )
    def test_doesnt_wait_until_transaction_commit(self) -> None:
        self.assertFalse(default_task_backend.enqueue_on_commit)
        self.assertFalse(
            default_task_backend._get_enqueue_on_commit_for_task(test_tasks.noop_task)
        )

        with transaction.atomic():
            result = test_tasks.noop_task.enqueue()

            self.assertIsNotNone(result.enqueued_at)

            self.assertEqual(DBTaskResult.objects.count(), 1)

            # SQLite locks the table during this transaction
            if connection.vendor != "sqlite":
                self.assertEqual(self.get_task_count_in_new_connection(), 0)

        if connection.vendor != "sqlite":
            self.assertEqual(self.get_task_count_in_new_connection(), 1)

    @override_settings(
        TASKS={
            "default": {
                "BACKEND": "django_tasks.backends.database.DatabaseBackend",
            }
        }
    )
    def test_wait_until_transaction_by_default(self) -> None:
        self.assertTrue(default_task_backend.enqueue_on_commit)
        self.assertTrue(
            default_task_backend._get_enqueue_on_commit_for_task(test_tasks.noop_task)
        )

    @override_settings(
        TASKS={
            "default": {
                "BACKEND": "django_tasks.backends.database.DatabaseBackend",
                "ENQUEUE_ON_COMMIT": False,
            }
        }
    )
    def test_task_specific_enqueue_on_commit(self) -> None:
        self.assertFalse(default_task_backend.enqueue_on_commit)
        self.assertTrue(test_tasks.enqueue_on_commit_task.enqueue_on_commit)
        self.assertTrue(
            default_task_backend._get_enqueue_on_commit_for_task(
                test_tasks.enqueue_on_commit_task
            )
        )

    def test_enqueue_logs(self) -> None:
        with self.assertLogs("django_tasks", level="DEBUG") as captured_logs:
            result = test_tasks.noop_task.enqueue()

        self.assertEqual(len(captured_logs.output), 1)
        self.assertIn("enqueued", captured_logs.output[0])
        self.assertIn(result.id, captured_logs.output[0])


@override_settings(
    TASKS={
        "default": {
            "BACKEND": "django_tasks.backends.database.DatabaseBackend",
            "QUEUES": ["default", "queue-1"],
        },
        "dummy": {"BACKEND": "django_tasks.backends.dummy.DummyBackend"},
    }
)
class DatabaseBackendWorkerTestCase(TransactionTestCase):
    worker_id = str(uuid.uuid4())

    run_worker = staticmethod(
        partial(
            call_command,
            "db_worker",
            verbosity=0,
            batch=True,
            interval=0,
            startup_delay=False,
            worker_id=worker_id,
        )
    )

    def tearDown(self) -> None:
        logger = logging.getLogger("django_tasks")

        # Reset the logger after every run, to ensure the correct `stdout` is used
        for handler in logger.handlers:
            logger.removeHandler(handler)

    def test_run_enqueued_task(self) -> None:
        for task in [
            test_tasks.noop_task,
            test_tasks.noop_task_async,
        ]:
            with self.subTest(task):
                result = cast(Task, task).enqueue()
                self.assertEqual(DBTaskResult.objects.ready().count(), 1)

                self.assertEqual(result.status, ResultStatus.NEW)

                with self.assertNumQueries(12 if connection.vendor == "mysql" else 11):
                    self.run_worker()

                self.assertEqual(result.status, ResultStatus.NEW)
                result.refresh()
                self.assertIsNotNone(result.started_at)
                self.assertIsNotNone(result.finished_at)
                self.assertGreaterEqual(result.started_at, result.enqueued_at)  # type:ignore[arg-type,misc]
                self.assertGreaterEqual(result.finished_at, result.started_at)  # type:ignore[arg-type,misc]
                self.assertEqual(result.status, ResultStatus.SUCCEEDED)
                self.assertIsNone(result.worker_id)

                self.assertEqual(DBTaskResult.objects.ready().count(), 0)

    def test_batch_processes_all_tasks(self) -> None:
        for _ in range(3):
            test_tasks.noop_task.enqueue()
        test_tasks.failing_task_value_error.enqueue()

        self.assertEqual(DBTaskResult.objects.ready().count(), 4)

        with self.assertNumQueries(30 if connection.vendor == "mysql" else 26):
            self.run_worker()

        self.assertEqual(DBTaskResult.objects.ready().count(), 0)
        self.assertEqual(DBTaskResult.objects.succeeded().count(), 3)
        self.assertEqual(DBTaskResult.objects.failed().count(), 1)

    def test_no_tasks(self) -> None:
        with self.assertNumQueries(6):
            self.run_worker()

    def test_doesnt_process_different_queue(self) -> None:
        result = test_tasks.noop_task.using(queue_name="queue-1").enqueue()

        self.assertEqual(DBTaskResult.objects.ready().count(), 1)

        with self.assertNumQueries(6):
            self.run_worker()

        self.assertEqual(DBTaskResult.objects.ready().count(), 1)

        with self.assertNumQueries(12 if connection.vendor == "mysql" else 11):
            self.run_worker(queue_name=result.task.queue_name)

        self.assertEqual(DBTaskResult.objects.ready().count(), 0)

    def test_process_all_queues(self) -> None:
        test_tasks.noop_task.using(queue_name="queue-1").enqueue()

        self.assertEqual(DBTaskResult.objects.ready().count(), 1)

        with self.assertNumQueries(6):
            self.run_worker()

        self.assertEqual(DBTaskResult.objects.ready().count(), 1)

        with self.assertNumQueries(12 if connection.vendor == "mysql" else 11):
            self.run_worker(queue_name="*")

        self.assertEqual(DBTaskResult.objects.ready().count(), 0)

    def test_failing_task(self) -> None:
        result = test_tasks.failing_task_value_error.enqueue()
        self.assertEqual(DBTaskResult.objects.ready().count(), 1)

        with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
            result.exception_class  # noqa: B018

        with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
            result.traceback  # noqa: B018

        with self.assertNumQueries(12 if connection.vendor == "mysql" else 11):
            self.run_worker()

        self.assertEqual(result.status, ResultStatus.NEW)
        result.refresh()
        self.assertIsNotNone(result.started_at)
        self.assertIsNotNone(result.finished_at)

        self.assertGreaterEqual(result.started_at, result.enqueued_at)  # type: ignore
        self.assertGreaterEqual(result.finished_at, result.started_at)  # type: ignore
        self.assertEqual(result.status, ResultStatus.FAILED)
        with self.assertRaisesMessage(ValueError, "Task failed"):
            result.return_value  # noqa: B018

        self.assertEqual(result.exception_class, ValueError)
        assert result.traceback  # So that mypy knows the next line is allowed
        self.assertTrue(
            result.traceback.endswith(
                "ValueError: This task failed due to ValueError\n"
            )
        )

        self.assertEqual(DBTaskResult.objects.ready().count(), 0)

    def test_complex_exception(self) -> None:
        result = test_tasks.complex_exception.enqueue()
        self.assertEqual(DBTaskResult.objects.ready().count(), 1)

        with self.assertRaisesMessage(ValueError, "Task has not finished"):
            result.exception_class  # noqa: B018

        with self.assertRaisesMessage(ValueError, "Task has not finished"):
            result.traceback  # noqa: B018

        with self.assertNumQueries(12 if connection.vendor == "mysql" else 11):
            self.run_worker()

        self.assertEqual(result.status, ResultStatus.NEW)
        result.refresh()
        self.assertIsNotNone(result.started_at)
        self.assertIsNotNone(result.finished_at)

        self.assertGreaterEqual(result.started_at, result.enqueued_at)  # type: ignore
        self.assertGreaterEqual(result.finished_at, result.started_at)  # type: ignore
        self.assertEqual(result.status, ResultStatus.FAILED)
        with self.assertRaisesMessage(ValueError, "Task failed"):
            result.return_value  # noqa: B018

        self.assertEqual(result.exception_class, ValueError)
        self.assertIn('ValueError(ValueError("This task failed"))', result.traceback)  # type: ignore[arg-type]

        self.assertEqual(DBTaskResult.objects.ready().count(), 0)

    def test_doesnt_process_different_backend(self) -> None:
        result = test_tasks.failing_task_value_error.enqueue()

        self.assertEqual(DBTaskResult.objects.ready().count(), 1)

        with self.assertNumQueries(6):
            self.run_worker(backend_name="dummy")

        self.assertEqual(DBTaskResult.objects.ready().count(), 1)

        with self.assertNumQueries(12 if connection.vendor == "mysql" else 11):
            self.run_worker(backend_name=result.backend)

        self.assertEqual(DBTaskResult.objects.ready().count(), 0)

    def test_unknown_backend(self) -> None:
        output = StringIO()
        with redirect_stderr(output):
            with self.assertRaises(SystemExit):
                execute_from_command_line(
                    ["django-admin", "db_worker", "--backend", "unknown"]
                )
        self.assertIn("The connection 'unknown' doesn't exist.", output.getvalue())

    def test_incorrect_backend(self) -> None:
        output = StringIO()
        with redirect_stderr(output):
            with self.assertRaises(SystemExit):
                execute_from_command_line(
                    ["django-admin", "db_worker", "--backend", "dummy"]
                )
        self.assertIn("Backend 'dummy' is not a database backend", output.getvalue())

    def test_negative_interval(self) -> None:
        output = StringIO()
        with redirect_stderr(output):
            with self.assertRaises(SystemExit):
                execute_from_command_line(
                    ["django-admin", "db_worker", "--interval", "-1"]
                )
        self.assertIn("Must be greater than zero", output.getvalue())

    def test_infinite_interval(self) -> None:
        output = StringIO()
        with redirect_stderr(output):
            with self.assertRaises(SystemExit):
                execute_from_command_line(
                    ["django-admin", "db_worker", "--interval", "inf"]
                )
        self.assertIn("Must be a finite floating point value", output.getvalue())

    def test_fractional_interval(self) -> None:
        with mock.patch(
            "django_tasks.backends.database.management.commands.db_worker.Worker"
        ) as worker_class:
            execute_from_command_line(
                ["django-admin", "db_worker", "--interval", "0.1"]
            )

        self.assertEqual(worker_class.mock_calls[0].kwargs["interval"], 0.1)

    def test_too_long_worker_id(self) -> None:
        output = StringIO()
        with redirect_stderr(output):
            with self.assertRaises(SystemExit):
                execute_from_command_line(
                    ["django-admin", "db_worker", "--worker-id", "A" * 65]
                )
        self.assertIn(
            "Worker ids must be shorter than 64 characters", output.getvalue()
        )

    def test_empty_worker_id(self) -> None:
        output = StringIO()
        with redirect_stderr(output):
            with self.assertRaises(SystemExit):
                execute_from_command_line(
                    ["django-admin", "db_worker", "--worker-id", ""]
                )
        self.assertIn("Worker id must not be empty", output.getvalue())

    def test_run_after(self) -> None:
        result = test_tasks.noop_task.using(
            run_after=timezone.now() + timedelta(hours=10)
        ).enqueue()

        self.assertEqual(DBTaskResult.objects.count(), 1)
        self.assertEqual(DBTaskResult.objects.ready().count(), 0)

        with self.assertNumQueries(6):
            self.run_worker()

        self.assertEqual(DBTaskResult.objects.count(), 1)
        self.assertEqual(DBTaskResult.objects.ready().count(), 0)
        self.assertEqual(DBTaskResult.objects.succeeded().count(), 0)

        DBTaskResult.objects.filter(id=result.id).update(run_after=timezone.now())

        self.assertEqual(DBTaskResult.objects.ready().count(), 1)

        with self.assertNumQueries(12 if connection.vendor == "mysql" else 11):
            self.run_worker()

        self.assertEqual(DBTaskResult.objects.ready().count(), 0)
        self.assertEqual(DBTaskResult.objects.succeeded().count(), 1)

    def test_run_after_priority(self) -> None:
        far_future_result = test_tasks.noop_task.using(
            run_after=timezone.now() + timedelta(hours=10)
        ).enqueue()

        high_priority_far_future_result = test_tasks.noop_task.using(
            priority=10, run_after=timezone.now() + timedelta(hours=10)
        ).enqueue()

        future_result = test_tasks.noop_task.using(
            run_after=timezone.now() + timedelta(hours=2)
        ).enqueue()

        high_priority_result = test_tasks.noop_task.using(priority=10).enqueue()

        low_priority_result = test_tasks.noop_task.using(priority=2).enqueue()
        lower_priority_result = test_tasks.noop_task.using(priority=-2).enqueue()

        self.assertEqual(
            [dbt.task_result for dbt in DBTaskResult.objects.all()],
            [
                high_priority_far_future_result,
                high_priority_result,
                low_priority_result,
                far_future_result,
                future_result,
                lower_priority_result,
            ],
        )

        self.assertEqual(
            [dbt.task_result for dbt in DBTaskResult.objects.ready()],
            [
                high_priority_result,
                low_priority_result,
                lower_priority_result,
            ],
        )

    def test_verbose_logging(self) -> None:
        result = test_tasks.noop_task.enqueue()

        stdout = StringIO()
        self.run_worker(verbosity=3, stdout=stdout, stderr=stdout)

        lines = stdout.getvalue().splitlines()

        lines.remove(f"Sent ping worker_id={self.worker_id}")

        self.assertEqual(
            lines,
            [
                f"Starting worker worker_id={self.worker_id} for queues=default",
                f"Task id={result.id} path=tests.tasks.noop_task state=RUNNING",
                f"Task id={result.id} path=tests.tasks.noop_task state=SUCCEEDED",
                f"No more tasks to run for worker_id={self.worker_id} - exiting gracefully.",
            ],
        )

    def test_invalid_task_path(self) -> None:
        db_task_result = DBTaskResult.objects.create(
            args_kwargs={"args": [["exit", "1"]], "kwargs": {}},
            task_path="subprocess.check_output",
            backend_name="default",
        )

        self.run_worker()

        db_task_result.refresh_from_db()

        self.assertEqual(db_task_result.status, ResultStatus.FAILED)

    def test_missing_task_path(self) -> None:
        db_task_result = DBTaskResult.objects.create(
            args_kwargs={"args": [], "kwargs": {}},
            task_path="missing.func",
            backend_name="default",
        )

        self.run_worker()

        db_task_result.refresh_from_db()

        self.assertEqual(db_task_result.status, ResultStatus.FAILED)

    def test_worker_doesnt_exit(self) -> None:
        result = test_tasks.exit_task.enqueue()

        self.run_worker()

        result.refresh()
        self.assertEqual(result.status, ResultStatus.FAILED)

    @skipIf(connection.vendor == "sqlite", "SQLite locks the entire database")
    def test_worker_with_locked_rows(self) -> None:
        result_1 = test_tasks.noop_task.enqueue()
        new_connection = connections.create_connection("default")

        with transaction.atomic():
            locked_tasks_query = str(DBTaskResult.objects.select_for_update().query)

        try:
            # Start a transaction in the other connection
            with new_connection.cursor() as c:
                c.execute("BEGIN")

            # Lock the current rows in the table
            with new_connection.cursor() as c:
                c.execute(locked_tasks_query)
                results = list(c.fetchall())
            self.assertEqual(len(results), 1)

            # Add another task which isn't locked
            result_2 = test_tasks.noop_task.enqueue()

            self.run_worker()
        finally:
            new_connection.close()

        result_1.refresh()
        result_2.refresh()

        self.assertEqual(result_1.status, ResultStatus.NEW)
        self.assertEqual(result_2.status, ResultStatus.SUCCEEDED)

    def test_worker_ping(self) -> None:
        result = test_tasks.sleep_for.enqueue(2.5)

        stdout = StringIO()

        with mock.patch(
            "django_tasks.backends.database.management.commands.db_worker.PING_TIMEOUT",
            new=1,
        ):
            self.run_worker(verbosity=3, stdout=stdout, stderr=stdout)

        log_lines = stdout.getvalue().splitlines()

        running_log_line = (
            f"Task id={result.id} path=tests.tasks.sleep_for state=RUNNING"
        )
        succeeded_log_line = (
            f"Task id={result.id} path=tests.tasks.sleep_for state=SUCCEEDED"
        )
        ping_log_line = f"Sent ping worker_id={self.worker_id}"

        self.assertIn(running_log_line, log_lines)
        self.assertIn(succeeded_log_line, log_lines)

        running_log_index = log_lines.index(running_log_line)
        succeeded_log_index = log_lines.index(succeeded_log_line)

        duration_logs = log_lines[running_log_index + 1 : succeeded_log_index]

        # It may be 2 or 3, depending on how the thread is scheduled
        self.assertGreaterEqual(duration_logs.count(ping_log_line), 2)
        self.assertLessEqual(duration_logs.count(ping_log_line), 3)

        self.assertFalse(DBWorkerPing.objects.filter(worker_id=self.worker_id).exists())


@override_settings(
    TASKS={
        "default": {
            "BACKEND": "django_tasks.backends.database.DatabaseBackend",
        },
    }
)
class DatabaseTaskResultTestCase(TransactionTestCase):
    def execute_in_new_connection(self, sql: Union[str, QuerySet]) -> Sequence:
        if isinstance(sql, QuerySet):
            sql = str(sql.query)
        new_connection = connections.create_connection("default")
        try:
            with new_connection.cursor() as c:
                c.execute(sql)
                return cast(list, c.fetchall())
        finally:
            new_connection.close()

    def test_cross_connection(self) -> None:
        test_tasks.noop_task.enqueue()
        test_tasks.noop_task.enqueue()

        self.assertEqual(DBTaskResult.objects.count(), 2)

        self.assertEqual(DBTaskResult.objects.using("default").count(), 2)

        self.assertEqual(
            len(self.execute_in_new_connection(DBTaskResult.objects.all())),
            2,
        )

    @skipIf(connection.vendor == "sqlite", "SQLite handles locks differently")
    def test_locks_tasks(self) -> None:
        test_tasks.noop_task.enqueue()
        test_tasks.noop_task.enqueue()

        with transaction.atomic():
            self.assertEqual(
                len(
                    self.execute_in_new_connection(
                        DBTaskResult.objects.select_for_update(skip_locked=True)
                    )
                ),
                2,
            )

            self.assertIsNotNone(DBTaskResult.objects.get_locked())

            self.assertEqual(
                len(
                    self.execute_in_new_connection(
                        DBTaskResult.objects.select_for_update(skip_locked=True)
                    )
                ),
                # MySQL likes to lock all the rows
                0 if connection.vendor == "mysql" else 1,
            )

            DBTaskResult.objects.get_locked()

        with transaction.atomic():
            # The original transaction has closed, so the result is unlocked
            self.assertEqual(
                len(
                    self.execute_in_new_connection(
                        DBTaskResult.objects.select_for_update(skip_locked=True)
                    )
                ),
                2,
            )

    @skipIf(connection.vendor != "sqlite", "SQLite handles locks differently")
    def test_locks_tasks_sqlite(self) -> None:
        result = test_tasks.noop_task.enqueue()

        with exclusive_transaction():
            locked_result = DBTaskResult.objects.get_locked()

            self.assertEqual(result.id, str(locked_result.id))  # type:ignore[union-attr]

            with self.assertRaisesMessage(OperationalError, "is locked"):
                self.execute_in_new_connection(
                    DBTaskResult.objects.select_for_update(skip_locked=True)
                )

        # The original transaction has closed, so the database is unlocked
        self.execute_in_new_connection(
            DBTaskResult.objects.select_for_update(skip_locked=True)
        )

    @skipIf(connection.vendor == "sqlite", "SQLite handles locks differently")
    def test_locks_tasks_filtered(self) -> None:
        result = test_tasks.noop_task.using(priority=10).enqueue()
        test_tasks.noop_task.enqueue()

        with transaction.atomic():
            self.assertEqual(
                len(
                    self.execute_in_new_connection(
                        DBTaskResult.objects.select_for_update(skip_locked=True)
                    )
                ),
                2,
            )

            locked_result = DBTaskResult.objects.filter(
                priority=result.task.priority
            ).get_locked()
            self.assertEqual(str(locked_result.id), result.id)

            self.assertEqual(
                len(
                    self.execute_in_new_connection(
                        DBTaskResult.objects.select_for_update(skip_locked=True)
                    )
                ),
                1,
            )

        with transaction.atomic():
            # The original transaction has closed, so the result is unlocked
            self.assertEqual(
                len(
                    self.execute_in_new_connection(
                        DBTaskResult.objects.select_for_update(skip_locked=True)
                    )
                ),
                2,
            )

    @skipIf(connection.vendor != "sqlite", "SQLite handles locks differently")
    def test_locks_tasks_filtered_sqlite(self) -> None:
        result = test_tasks.noop_task.using(priority=10).enqueue()
        test_tasks.noop_task.enqueue()

        with exclusive_transaction():
            locked_result = DBTaskResult.objects.filter(
                priority=result.task.priority
            ).get_locked()

            self.assertEqual(result.id, str(locked_result.id))

            with self.assertRaisesMessage(OperationalError, "is locked"):
                self.execute_in_new_connection(
                    DBTaskResult.objects.select_for_update(skip_locked=True)
                )

        # The original transaction has closed, so the database is unlocked
        self.execute_in_new_connection(
            DBTaskResult.objects.select_for_update(skip_locked=True)
        )

    @exclusive_transaction()
    def test_lock_no_rows(self) -> None:
        self.assertEqual(DBTaskResult.objects.count(), 0)
        self.assertIsNone(DBTaskResult.objects.all().get_locked())

    @skipIf(connection.vendor == "sqlite", "SQLite handles locks differently")
    def test_get_locked_with_locked_rows(self) -> None:
        result_1 = test_tasks.noop_task.enqueue()
        new_connection = connections.create_connection("default")

        with transaction.atomic():
            locked_tasks_query = str(DBTaskResult.objects.select_for_update().query)

        try:
            # Start a transaction in the other connection
            with new_connection.cursor() as c:
                c.execute("BEGIN")

            # Lock the current rows in the table from the other connection
            with new_connection.cursor() as c:
                c.execute(locked_tasks_query)
                results = list(c.fetchall())
            self.assertEqual(len(results), 1)
            self.assertEqual(normalize_uuid(results[0][0]), normalize_uuid(result_1.id))

            with transaction.atomic():
                # .count with skip_locked isn't supported
                self.assertEqual(
                    len(DBTaskResult.objects.select_for_update(skip_locked=True)), 0
                )
                self.assertIsNone(DBTaskResult.objects.get_locked())

            # Add another task which isn't locked
            result_2 = test_tasks.noop_task.enqueue()

            with transaction.atomic():
                self.assertEqual(
                    normalize_uuid(
                        DBTaskResult.objects.select_for_update(
                            skip_locked=True
                        ).values_list("id", flat=True)[0]
                    ),
                    normalize_uuid(result_2.id),
                )
                self.assertEqual(
                    normalize_uuid(DBTaskResult.objects.get_locked().id),  # type:ignore
                    normalize_uuid(result_2.id),
                )
        finally:
            new_connection.close()


class ConnectionExclusiveTranscationTestCase(TransactionTestCase):
    def setUp(self) -> None:
        self.connection = connections.create_connection("default")

    def tearDown(self) -> None:
        self.connection.close()
        # connection.close()

    @skipIf(connection.vendor == "sqlite", "SQLite handled separately")
    def test_non_sqlite(self) -> None:
        self.assertFalse(
            connection_requires_manual_exclusive_transaction(self.connection)
        )

    @skipIf(
        django.VERSION >= (5, 1),
        "Newer Django versions support custom transaction modes",
    )
    @skipIf(connection.vendor != "sqlite", "SQLite only")
    def test_old_django_requires_manual_transaction(self) -> None:
        self.assertTrue(
            connection_requires_manual_exclusive_transaction(self.connection)
        )

    @skipIf(django.VERSION < (5, 1), "Old Django versions require manual transactions")
    @skipIf(connection.vendor != "sqlite", "SQLite only")
    def test_explicit_transaction(self) -> None:
        # HACK: Set the attribute manually
        self.connection.transaction_mode = None  # type:ignore[attr-defined]
        self.assertTrue(
            connection_requires_manual_exclusive_transaction(self.connection)
        )

        self.connection.transaction_mode = "EXCLUSIVE"  # type:ignore[attr-defined]
        self.assertFalse(
            connection_requires_manual_exclusive_transaction(self.connection)
        )

    @skipIf(connection.vendor != "sqlite", "SQLite only")
    def test_exclusive_transaction(self) -> None:
        with self.assertNumQueries(2) as c:
            with exclusive_transaction():
                pass

        self.assertEqual(c.captured_queries[0]["sql"], "BEGIN EXCLUSIVE")


@override_settings(
    TASKS={
        "default": {
            "BACKEND": "django_tasks.backends.database.DatabaseBackend",
            "QUEUES": ["default", "queue-1"],
        },
        "dummy": {"BACKEND": "django_tasks.backends.dummy.DummyBackend"},
    }
)
class DatabaseBackendPruneTaskResultsTestCase(TransactionTestCase):
    prune_task_results = staticmethod(
        partial(call_command, "prune_db_task_results", verbosity=0)
    )

    def tearDown(self) -> None:
        # Reset the logger after every run, to ensure the correct `stdout` is used
        for handler in prune_db_tasks_logger.handlers:
            prune_db_tasks_logger.removeHandler(handler)

    def test_prunes_tasks(self) -> None:
        result = test_tasks.noop_task.enqueue()

        DBTaskResult.objects.all().update(
            status=ResultStatus.SUCCEEDED, finished_at=timezone.now()
        )

        self.assertEqual(DBTaskResult.objects.finished().count(), 1)

        stdout = StringIO()

        with self.assertNumQueries(3):
            self.prune_task_results(min_age_days=0, stdout=stdout, verbosity=3)

        self.assertEqual(DBTaskResult.objects.finished().count(), 0)

        with self.assertRaises(ResultDoesNotExist):
            result.refresh()

        self.assertEqual(stdout.getvalue().strip(), "Deleted 1 task result(s)")

    def test_doesnt_prune_new_tasks(self) -> None:
        result = test_tasks.noop_task.enqueue()

        self.assertEqual(DBTaskResult.objects.ready().count(), 1)

        stdout = StringIO()
        with self.assertNumQueries(3):
            self.prune_task_results(min_age_days=0, stdout=stdout, verbosity=3)

        self.assertEqual(DBTaskResult.objects.ready().count(), 1)

        result.refresh()

        self.assertEqual(stdout.getvalue().strip(), "Deleted 0 task result(s)")

    def test_doesnt_prune_running_tasks(self) -> None:
        result = test_tasks.noop_task.enqueue()

        DBTaskResult.objects.all().update(status=ResultStatus.RUNNING)

        self.assertEqual(DBTaskResult.objects.running().count(), 1)

        with self.assertNumQueries(3):
            self.prune_task_results(min_age_days=0)

        self.assertEqual(DBTaskResult.objects.running().count(), 1)

        result.refresh()

    def test_only_prunes_specified_queue(self) -> None:
        result = test_tasks.noop_task.enqueue()
        queue_1_result = test_tasks.noop_task.using(queue_name="queue-1").enqueue()

        DBTaskResult.objects.all().update(
            status=ResultStatus.SUCCEEDED, finished_at=timezone.now()
        )

        self.assertEqual(DBTaskResult.objects.succeeded().count(), 2)

        with self.assertNumQueries(3):
            self.prune_task_results(queue_name="queue-1", min_age_days=0)

        self.assertEqual(DBTaskResult.objects.succeeded().count(), 1)

        result.refresh()

        with self.assertRaises(ResultDoesNotExist):
            queue_1_result.refresh()

    def test_prune_all_queues(self) -> None:
        test_tasks.noop_task.enqueue()
        test_tasks.noop_task.using(queue_name="queue-1").enqueue()

        DBTaskResult.objects.all().update(
            status=ResultStatus.SUCCEEDED, finished_at=timezone.now()
        )

        self.assertEqual(DBTaskResult.objects.succeeded().count(), 2)

        with self.assertNumQueries(3):
            self.prune_task_results(queue_name="*", min_age_days=0)

        self.assertEqual(DBTaskResult.objects.succeeded().count(), 0)

    def test_min_age(self) -> None:
        one_day_result = test_tasks.noop_task.enqueue()

        DBTaskResult.objects.ready().update(
            status=ResultStatus.SUCCEEDED,
            finished_at=timezone.now() - timedelta(days=1),
        )

        three_day_result = test_tasks.noop_task.enqueue()
        DBTaskResult.objects.ready().update(
            status=ResultStatus.SUCCEEDED,
            finished_at=timezone.now() - timedelta(days=3),
        )

        self.assertEqual(DBTaskResult.objects.succeeded().count(), 2)

        with self.assertNumQueries(3):
            self.prune_task_results()

        self.assertEqual(DBTaskResult.objects.succeeded().count(), 2)

        with self.assertNumQueries(3):
            self.prune_task_results(min_age_days=3)

        self.assertEqual(DBTaskResult.objects.succeeded().count(), 1)

        one_day_result.refresh()

        with self.assertRaises(ResultDoesNotExist):
            three_day_result.refresh()

        with self.assertNumQueries(3):
            self.prune_task_results(min_age_days=1)

        self.assertEqual(DBTaskResult.objects.succeeded().count(), 0)

    def test_failed_min_age(self) -> None:
        succeeded_result = test_tasks.noop_task.enqueue()

        DBTaskResult.objects.ready().update(
            status=ResultStatus.SUCCEEDED,
            finished_at=timezone.now() - timedelta(days=3),
        )

        failed_result = test_tasks.noop_task.enqueue()
        DBTaskResult.objects.ready().update(
            status=ResultStatus.FAILED, finished_at=timezone.now() - timedelta(days=3)
        )

        self.assertEqual(DBTaskResult.objects.finished().count(), 2)

        with self.assertNumQueries(3):
            self.prune_task_results()

        self.assertEqual(DBTaskResult.objects.finished().count(), 2)

        with self.assertNumQueries(3):
            self.prune_task_results(min_age_days=3, failed_min_age_days=5)

        self.assertEqual(DBTaskResult.objects.finished().count(), 1)

        failed_result.refresh()

        with self.assertRaises(ResultDoesNotExist):
            succeeded_result.refresh()

        with self.assertNumQueries(3):
            self.prune_task_results(min_age_days=3, failed_min_age_days=1)

        with self.assertRaises(ResultDoesNotExist):
            failed_result.refresh()

    def test_dry_run(self) -> None:
        test_tasks.noop_task.enqueue()

        DBTaskResult.objects.all().update(
            status=ResultStatus.SUCCEEDED, finished_at=timezone.now()
        )

        self.assertEqual(DBTaskResult.objects.count(), 1)

        stdout = StringIO()
        with self.assertNumQueries(1):
            self.prune_task_results(
                min_age_days=0, dry_run=True, stdout=stdout, verbosity=3
            )

        self.assertEqual(DBTaskResult.objects.count(), 1)

        self.assertEqual(stdout.getvalue().strip(), "Would delete 1 task result(s)")

    def test_unknown_backend(self) -> None:
        output = StringIO()
        with redirect_stderr(output):
            with self.assertRaises(SystemExit):
                execute_from_command_line(
                    ["django-admin", "prune_db_task_results", "--backend", "unknown"]
                )
        self.assertIn("The connection 'unknown' doesn't exist.", output.getvalue())

    def test_incorrect_backend(self) -> None:
        output = StringIO()
        with redirect_stderr(output):
            with self.assertRaises(SystemExit):
                execute_from_command_line(
                    ["django-admin", "prune_db_task_results", "--backend", "dummy"]
                )
        self.assertIn("Backend 'dummy' is not a database backend", output.getvalue())

    def test_negative_age(self) -> None:
        output = StringIO()
        with redirect_stderr(output):
            with self.assertRaises(SystemExit):
                execute_from_command_line(
                    ["django-admin", "prune_db_task_results", "--min-age-days", "-1"]
                )
        self.assertIn("Must be greater than zero", output.getvalue())


@override_settings(
    TASKS={
        "default": {
            "BACKEND": "django_tasks.backends.database.DatabaseBackend",
        },
    }
)
@skipIfInMemoryDB()
class DatabaseWorkerProcessTestCase(TransactionTestCase):
    def setUp(self) -> None:
        self.processes: List[subprocess.Popen] = []

    def tearDown(self) -> None:
        # Try n times to kill any remaining child processes
        for _ in range(3):
            for process in self.processes:
                if process.poll() is None:
                    process.kill()
                    time.sleep(0.1)

    def start_worker(
        self,
        args: Optional[List[str]] = None,
        *,
        debug: bool = False,
        worker_id: Optional[str] = None,
    ) -> subprocess.Popen:
        if args is None:
            args = []

        if worker_id is None:
            worker_id = get_random_id()

        p = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "manage",
                "db_worker",
                "--verbosity",
                "3",
                "--no-startup-delay",
                "--worker-id",
                worker_id,
                *args,
            ],
            stdout=None if debug else subprocess.PIPE,
            stderr=None if debug else subprocess.STDOUT,
            env={
                **os.environ,
                "DJANGO_SETTINGS_MODULE": "tests.db_worker_test_settings",
            },
            text=True,
        )
        self.processes.append(p)
        return p

    def wait_for_worker_ping(self, worker_id: Optional[str] = None) -> None:
        qs = DBWorkerPing.objects.all()

        if worker_id is not None:
            qs = qs.filter(worker_id=worker_id)

        for _ in range(15, 0, -1):
            if qs.exists():
                return

            time.sleep(0.1)

    def test_run_subprocess(self) -> None:
        result = test_tasks.noop_task.enqueue()
        process = self.start_worker(["--batch"])
        process.wait()
        self.assertEqual(process.returncode, 0)

        self.assertEqual(result.status, ResultStatus.NEW)

        result.refresh()

        self.assertEqual(result.status, ResultStatus.SUCCEEDED)

    @skipIf(sys.platform == "win32", "Terminate is always forceful on Windows")
    def test_interrupt_no_tasks(self) -> None:
        process = self.start_worker()

        self.wait_for_worker_ping()

        process.terminate()

        process.wait(timeout=0.5)
        self.assertEqual(process.returncode, 0)

    @skipIf(sys.platform == "win32", "Cannot emulate CTRL-C on Windows")
    def test_interrupt_signals(self) -> None:
        for sig in [
            signal.SIGINT,  # ctrl-c
            signal.SIGTERM,
        ]:
            with self.subTest(sig):
                result = test_tasks.sleep_for.enqueue(2)

                worker_id = get_random_id()

                process = self.start_worker(worker_id=worker_id)

                # Make sure the task is running by now
                self.wait_for_worker_ping(worker_id)

                result.refresh()
                self.assertEqual(result.status, ResultStatus.RUNNING)
                self.assertIsNotNone(result.worker_id)

                process.send_signal(sig)

                process.wait(timeout=3)

                self.assertEqual(process.returncode, 0)

                result.refresh()

                self.assertEqual(result.status, ResultStatus.SUCCEEDED)
                self.assertIsNone(result.worker_id)

    @skipIf(sys.platform == "win32", "Cannot emulate CTRL-C on Windows")
    def test_repeat_ctrl_c(self) -> None:
        result = test_tasks.hang.enqueue()

        process = self.start_worker()

        self.wait_for_worker_ping()

        result.refresh()
        self.assertEqual(result.status, ResultStatus.RUNNING)
        self.assertIsNotNone(result.worker_id)

        process.send_signal(signal.SIGINT)

        time.sleep(0.5)

        self.assertIsNone(process.poll())
        result.refresh()
        self.assertEqual(result.status, ResultStatus.RUNNING)

        process.send_signal(signal.SIGINT)

        process.wait(timeout=2)

        self.assertEqual(process.returncode, 0)

        result.refresh()
        self.assertEqual(result.status, ResultStatus.FAILED)
        self.assertEqual(result.exception_class, SystemExit)
        self.assertIsNone(result.worker_id)

    @skipIf(sys.platform == "win32", "Windows doesn't support SIGKILL")
    def test_kill(self) -> None:
        # Required to keep mypy happy
        assert hasattr(signal, "SIGKILL")

        result = test_tasks.hang.enqueue()

        process = self.start_worker()

        # Make sure the task is running by now
        self.wait_for_worker_ping()

        result.refresh()
        self.assertEqual(result.status, ResultStatus.RUNNING)

        process.kill()

        process.wait(timeout=2)

        self.assertEqual(process.returncode, -signal.SIGKILL)

        result.refresh()

        # TODO: https://github.com/RealOrangeOne/django-tasks/issues/46
        self.assertEqual(result.status, ResultStatus.RUNNING)

    def test_system_exit_task(self) -> None:
        result = test_tasks.failing_task_system_exit.enqueue()

        process = self.start_worker(["--batch"])
        process.wait(timeout=2)

        self.assertEqual(process.returncode, 0)

        result.refresh()
        self.assertEqual(result.status, ResultStatus.FAILED)
        self.assertEqual(result.exception_class, SystemExit)
        self.assertIsNone(result.worker_id)

    def test_keyboard_interrupt_task(self) -> None:
        result = test_tasks.failing_task_keyboard_interrupt.enqueue()

        process = self.start_worker(["--batch"])
        process.wait(timeout=2)

        self.assertEqual(process.returncode, 0)

        result.refresh()
        self.assertEqual(result.status, ResultStatus.FAILED)
        self.assertEqual(result.exception_class, KeyboardInterrupt)
        self.assertIsNone(result.worker_id)

    def test_multiple_workers(self) -> None:
        results = [test_tasks.sleep_for.enqueue(0.1) for _ in range(10)]

        for _ in range(3):
            self.start_worker(["--batch"])

        self.wait_for_worker_ping()

        for process in self.processes:
            process.wait(timeout=5)
            self.assertIsNotNone(process.returncode)

        for result in results:
            result.refresh()
            self.assertEqual(result.status, ResultStatus.SUCCEEDED)
            self.assertIsNone(result.worker_id)

        all_output = ""

        for process in self.processes:
            stdout_text = process.stdout.read()  # type:ignore[union-attr]
            all_output += stdout_text
            self.assertIn("gracefully", stdout_text)

        for result in results:
            # Running and succeeded
            self.assertEqual(all_output.count(result.id), 2)

    def test_worker_ping(self) -> None:
        test_tasks.sleep_for.enqueue(3)

        worker_id = get_random_id()

        process = self.start_worker(["--batch"], worker_id=worker_id)

        self.wait_for_worker_ping(worker_id)

        self.assertTrue(DBWorkerPing.objects.filter(worker_id=worker_id).exists())

        process.wait(timeout=4)
        self.assertEqual(process.returncode, 0)

        self.assertFalse(DBWorkerPing.objects.filter(worker_id=worker_id).exists())
