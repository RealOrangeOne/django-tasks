import json
import os
import uuid
from typing import Union, cast
from unittest.mock import patch

import django_rq
from asgiref.sync import async_to_sync
from django.core.exceptions import SuspiciousOperation
from django.db import transaction
from django.test import TransactionTestCase, modify_settings, override_settings
from django.urls import reverse
from fakeredis import FakeRedis, FakeStrictRedis
from rq.timeouts import TimerDeathPenalty

from django_tasks import ResultStatus, Task, default_task_backend, tasks
from django_tasks.backends.rq import Job, RQBackend
from django_tasks.exceptions import ResultDoesNotExist
from tests import tasks as test_tasks


# RQ
# Configuration to pretend there is a Redis service available.
# Set up the connection before RQ Django reads the settings.
# The connection must be the same because in fakeredis connections
# do not share the state. Therefore, we define a singleton object to reuse it.
def get_fake_connection(
    config: dict, strict: bool
) -> Union[FakeRedis, FakeStrictRedis]:
    redis_cls = FakeStrictRedis if strict else FakeRedis
    if "URL" in config:
        return redis_cls.from_url(
            config["URL"],
            db=config.get("DB"),
        )
    return redis_cls(
        host=config["HOST"],
        port=config["PORT"],
        db=config.get("DB", 0),
        username=config.get("USERNAME", None),
        password=config.get("PASSWORD"),
    )


@override_settings(
    TASKS={
        "default": {
            "BACKEND": "django_tasks.backends.rq.RQBackend",
            "QUEUES": ["default", "queue-1"],
        }
    },
    RQ_QUEUES={
        "default": {
            "HOST": "localhost",
            "PORT": 6379,
        },
        "queue-1": {
            "HOST": "localhost",
            "PORT": 6379,
        },
    },
)
@modify_settings(INSTALLED_APPS={"append": ["django_rq"]})
class DatabaseBackendTestCase(TransactionTestCase):
    def setUp(self) -> None:
        super().setUp()

        fake_connection_patcher = patch(
            "django_rq.queues.get_redis_connection", get_fake_connection
        )
        fake_connection_patcher.start()
        self.addCleanup(fake_connection_patcher.stop)

        django_rq.get_connection().flushall()

    def run_worker(self) -> None:
        from rq import SimpleWorker

        for queue in default_task_backend._get_queues():  # type: ignore[attr-defined]
            worker = SimpleWorker([queue], prepare_for_work=False, job_class=Job)

            # Use timer death penalty to support Windows
            worker.death_penalty_class = TimerDeathPenalty  # type: ignore[assignment]

            # HACK: Work around fakeredis not supporting `CLIENT LIST`
            worker.hostname = "example-hostname"
            worker.pid = os.getpid()

            with self.assertLogs("rq.worker"):
                worker.work(burst=True)

    def test_using_correct_backend(self) -> None:
        self.assertEqual(default_task_backend, tasks["default"])
        self.assertIsInstance(tasks["default"], RQBackend)

    def test_enqueue_task(self) -> None:
        for task in [test_tasks.noop_task, test_tasks.noop_task_async]:
            with self.subTest(task):
                result = cast(Task, task).enqueue(1, two=3)

                self.assertEqual(result.status, ResultStatus.READY)
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

                self.assertEqual(result.status, ResultStatus.READY)
                self.assertFalse(result.is_finished)
                self.assertIsNone(result.started_at)
                self.assertIsNone(result.finished_at)
                with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
                    result.return_value  # noqa:B018
                self.assertEqual(result.task, task)
                self.assertEqual(result.args, [])
                self.assertEqual(result.kwargs, {})

    def test_catches_exception(self) -> None:
        test_data = [
            (
                test_tasks.failing_task_value_error,  # task function
                ValueError,  # expected exception
                "This task failed due to ValueError",  # expected message
            ),
            (
                test_tasks.failing_task_system_exit,
                SystemExit,
                "This task failed due to SystemExit",
            ),
        ]
        for task, exception, message in test_data:
            with (
                self.subTest(task),
            ):
                result = task.enqueue()

                with self.assertLogs("django_tasks", "DEBUG"):
                    self.run_worker()

                result.refresh()

                # assert result
                self.assertEqual(result.status, ResultStatus.FAILED)
                with self.assertRaisesMessage(ValueError, "Task failed"):
                    result.return_value  # noqa: B018
                self.assertTrue(result.is_finished)
                self.assertIsNotNone(result.started_at)
                self.assertIsNotNone(result.finished_at)
                self.assertGreaterEqual(result.started_at, result.enqueued_at)  # type:ignore[arg-type, misc]
                self.assertGreaterEqual(result.finished_at, result.started_at)  # type:ignore[arg-type, misc]
                self.assertEqual(result.errors[0].exception_class, exception)
                traceback = result.errors[0].traceback
                self.assertTrue(
                    traceback
                    and traceback.endswith(f"{exception.__name__}: {message}\n"),
                    traceback,
                )
                self.assertEqual(result.task, task)
                self.assertEqual(result.args, [])
                self.assertEqual(result.kwargs, {})

    def test_complex_exception(self) -> None:
        result = test_tasks.complex_exception.enqueue()

        with self.assertLogs("django_tasks", "DEBUG"):
            self.run_worker()

        result.refresh()

        self.assertEqual(result.status, ResultStatus.FAILED)
        self.assertIsNotNone(result.started_at)
        self.assertIsNotNone(result.finished_at)
        self.assertGreaterEqual(result.started_at, result.enqueued_at)  # type:ignore[arg-type,misc]
        self.assertGreaterEqual(result.finished_at, result.started_at)  # type:ignore[arg-type,misc]

        self.assertIsNone(result._return_value)
        self.assertEqual(result.errors[0].exception_class, ValueError)
        self.assertIn(
            'ValueError(ValueError("This task failed"))', result.errors[0].traceback
        )

        self.assertEqual(result.task, test_tasks.complex_exception)
        self.assertEqual(result.args, [])
        self.assertEqual(result.kwargs, {})

    def test_get_result(self) -> None:
        result = default_task_backend.enqueue(test_tasks.noop_task, [], {})

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

        self.run_worker()

        self.assertEqual(result.status, ResultStatus.READY)
        self.assertFalse(result.is_finished)
        self.assertIsNone(result.started_at)
        self.assertIsNone(result.finished_at)

        result.refresh()

        self.assertIsNotNone(result.started_at)
        self.assertIsNotNone(result.finished_at)
        self.assertEqual(result.status, ResultStatus.SUCCEEDED)
        self.assertTrue(result.is_finished)
        self.assertEqual(result.return_value, 42)

    def test_refresh_result_async(self) -> None:
        result = async_to_sync(default_task_backend.aenqueue)(
            test_tasks.calculate_meaning_of_life, (), {}
        )

        self.run_worker()

        self.assertEqual(result.status, ResultStatus.READY)
        self.assertFalse(result.is_finished)
        self.assertIsNone(result.started_at)
        self.assertIsNone(result.finished_at)

        async_to_sync(result.arefresh)()

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
                self.assertEqual(data["status"], ResultStatus.READY)

                result = default_task_backend.get_result(data["result_id"])
                self.assertEqual(result.status, ResultStatus.READY)

    def test_get_result_from_different_request(self) -> None:
        response = self.client.get(reverse("meaning-of-life"))
        self.assertEqual(response.status_code, 200)

        data = json.loads(response.content)
        result_id = data["result_id"]

        response = self.client.get(reverse("result", args=[result_id]))
        self.assertEqual(response.status_code, 200)

        self.assertEqual(
            json.loads(response.content),
            {"result_id": result_id, "result": None, "status": ResultStatus.READY},
        )

    def test_invalid_task_path(self) -> None:
        job = django_rq.get_queue("default", job_class=Job).enqueue_call(  # type: ignore[no-untyped-call]
            "subprocess.check_output", args=["exit", "1"]
        )

        with self.assertRaisesMessage(
            SuspiciousOperation,
            f"Task {job.id} does not point to a Task (subprocess.check_output)",
        ):
            default_task_backend.get_result(job.id)

    def test_check(self) -> None:
        errors = list(default_task_backend.check())

        self.assertEqual(len(errors), 0, errors)

    @override_settings(INSTALLED_APPS=[])
    def test_rq_app_missing(self) -> None:
        errors = list(default_task_backend.check())

        self.assertEqual(len(errors), 1)
        self.assertIn("django_rq", errors[0].hint)  # type:ignore[arg-type]

    @override_settings(
        TASKS={
            "default": {
                "BACKEND": "django_tasks.backends.rq.RQBackend",
                "ENQUEUE_ON_COMMIT": True,
            }
        }
    )
    def test_wait_until_transaction_commit(self) -> None:
        self.assertTrue(default_task_backend.enqueue_on_commit)
        self.assertTrue(
            default_task_backend._get_enqueue_on_commit_for_task(test_tasks.noop_task)
        )

        queue = django_rq.get_queue("default", job_class=Job)

        with transaction.atomic():
            result = test_tasks.noop_task.enqueue()

            self.assertIsNone(result.enqueued_at)

            self.assertEqual(queue.count, 0)
        self.assertEqual(queue.count, 1)

        result.refresh()
        self.assertIsNotNone(result.enqueued_at)

    @override_settings(
        TASKS={
            "default": {
                "BACKEND": "django_tasks.backends.rq.RQBackend",
                "ENQUEUE_ON_COMMIT": False,
            }
        }
    )
    def test_doesnt_wait_until_transaction_commit(self) -> None:
        self.assertFalse(default_task_backend.enqueue_on_commit)
        self.assertFalse(
            default_task_backend._get_enqueue_on_commit_for_task(test_tasks.noop_task)
        )

        queue = django_rq.get_queue("default", job_class=Job)

        with transaction.atomic():
            result = test_tasks.noop_task.enqueue()

            self.assertIsNotNone(result.enqueued_at)

            self.assertEqual(queue.count, 1)

        self.assertEqual(queue.count, 1)

    @override_settings(
        TASKS={
            "default": {
                "BACKEND": "django_tasks.backends.rq.RQBackend",
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
                "BACKEND": "django_tasks.backends.rq.RQBackend",
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

    def test_started_finished_logs(self) -> None:
        result = test_tasks.noop_task.enqueue()

        with self.assertLogs("django_tasks", level="DEBUG") as captured_logs:
            self.run_worker()

        self.assertEqual(len(captured_logs.output), 2)
        self.assertIn("state=RUNNING", captured_logs.output[0])
        self.assertIn(result.id, captured_logs.output[0])

        self.assertIn("state=SUCCEEDED", captured_logs.output[1])
        self.assertIn(result.id, captured_logs.output[1])

    def test_failed_logs(self) -> None:
        result = test_tasks.failing_task_value_error.enqueue()

        with self.assertLogs("django_tasks", level="DEBUG") as captured_logs:
            self.run_worker()

        self.assertEqual(len(captured_logs.output), 2)
        self.assertIn("state=RUNNING", captured_logs.output[0])
        self.assertIn(result.id, captured_logs.output[0])

        self.assertIn("state=FAILED", captured_logs.output[1])
        self.assertIn(result.id, captured_logs.output[1])

    def test_enqueue_priority(self) -> None:
        task_1 = test_tasks.noop_task.enqueue()
        task_2 = test_tasks.noop_task.using(priority=100).enqueue()

        queue = django_rq.get_queue("default")

        self.assertEqual(queue.job_ids, [task_2.id, task_1.id])

        self.assertEqual(task_2.task.priority, 100)

        self.assertEqual(default_task_backend.get_result(task_2.id).task.priority, 0)

    def test_queue_isolation(self) -> None:
        default_task = test_tasks.noop_task.enqueue()
        other_task = test_tasks.noop_task.using(queue_name="queue-1").enqueue()

        default_task_backend.get_result(default_task.id)
        default_task_backend.get_result(other_task.id)

        self.assertEqual(django_rq.get_queue("default").job_ids, [default_task.id])
        self.assertEqual(django_rq.get_queue("queue-1").job_ids, [other_task.id])

    @override_settings(
        TASKS={
            "default": {"BACKEND": "django_tasks.backends.rq.RQBackend", "QUEUES": []}
        }
    )
    def test_uses_rq_queues_for_queue_names(self) -> None:
        self.assertEqual(default_task_backend.queues, {"default", "queue-1"})

    @override_settings(
        TASKS={
            "default": {
                "BACKEND": "django_tasks.backends.rq.RQBackend",
                "QUEUES": ["queue-2"],
            }
        }
    )
    def test_unknown_queue_name(self) -> None:
        errors = list(default_task_backend.check())

        self.assertEqual(len(errors), 1)
        self.assertIn("Add 'queue-2' to RQ_QUEUES", errors[0].hint)  # type:ignore[arg-type]
