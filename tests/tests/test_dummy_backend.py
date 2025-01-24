import json
from typing import cast
from unittest import mock

from django.db import transaction
from django.db.utils import ConnectionHandler
from django.test import (
    SimpleTestCase,
    TransactionTestCase,
    override_settings,
)
from django.urls import reverse

from django_tasks import ResultStatus, Task, default_task_backend, tasks
from django_tasks.backends.dummy import DummyBackend
from django_tasks.exceptions import InvalidTaskError, ResultDoesNotExist
from tests import tasks as test_tasks


@override_settings(
    TASKS={
        "default": {
            "BACKEND": "django_tasks.backends.dummy.DummyBackend",
            "ENQUEUE_ON_COMMIT": False,
        }
    }
)
class DummyBackendTestCase(SimpleTestCase):
    def setUp(self) -> None:
        default_task_backend.clear()  # type:ignore[attr-defined]

    def test_using_correct_backend(self) -> None:
        self.assertEqual(default_task_backend, tasks["default"])
        self.assertIsInstance(tasks["default"], DummyBackend)

    def test_enqueue_task(self) -> None:
        for task in [test_tasks.noop_task, test_tasks.noop_task_async]:
            with self.subTest(task):
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

                self.assertIn(result, default_task_backend.results)  # type:ignore[attr-defined]

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

                self.assertIn(result, default_task_backend.results)  # type:ignore[attr-defined]

    def test_get_result(self) -> None:
        result = default_task_backend.enqueue(test_tasks.noop_task, (), {})

        new_result = default_task_backend.get_result(result.id)

        self.assertEqual(result, new_result)

    async def test_get_result_async(self) -> None:
        result = await default_task_backend.aenqueue(test_tasks.noop_task, (), {})

        new_result = await default_task_backend.aget_result(result.id)

        self.assertEqual(result, new_result)

    def test_refresh_result(self) -> None:
        result = default_task_backend.enqueue(
            test_tasks.calculate_meaning_of_life, (), {}
        )

        enqueued_result = default_task_backend.results[0]  # type:ignore[attr-defined]
        object.__setattr__(enqueued_result, "status", ResultStatus.SUCCEEDED)

        self.assertEqual(result.status, ResultStatus.NEW)
        result.refresh()
        self.assertEqual(result.status, ResultStatus.SUCCEEDED)

    async def test_refresh_result_async(self) -> None:
        result = await default_task_backend.aenqueue(
            test_tasks.calculate_meaning_of_life, (), {}
        )

        enqueued_result = default_task_backend.results[0]  # type:ignore[attr-defined]
        object.__setattr__(enqueued_result, "status", ResultStatus.SUCCEEDED)

        self.assertEqual(result.status, ResultStatus.NEW)
        await result.arefresh()
        self.assertEqual(result.status, ResultStatus.SUCCEEDED)

    async def test_get_missing_result(self) -> None:
        with self.assertRaises(ResultDoesNotExist):
            default_task_backend.get_result("123")

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

    def test_enqueue_on_commit(self) -> None:
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

    def test_exceptions(self) -> None:
        result = test_tasks.noop_task.enqueue()

        with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
            result.exception_class  # noqa: B018

        with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
            result.traceback  # noqa: B018

    def test_validate_disallowed_async_task(self) -> None:
        with mock.patch.multiple(default_task_backend, supports_async_task=False):
            with self.assertRaisesMessage(
                InvalidTaskError, "Backend does not support async tasks"
            ):
                default_task_backend.validate_task(test_tasks.noop_task_async)

    def test_check(self) -> None:
        errors = list(default_task_backend.check())

        self.assertEqual(len(errors), 0, errors)

    @override_settings(
        TASKS={
            "default": {
                "BACKEND": "django_tasks.backends.dummy.DummyBackend",
                "ENQUEUE_ON_COMMIT": True,
            }
        }
    )
    @mock.patch("django_tasks.backends.base.connections", ConnectionHandler({}))
    def test_enqueue_on_commit_with_no_databases(self) -> None:
        errors = list(default_task_backend.check())

        self.assertEqual(len(errors), 1)
        self.assertIn("Set `ENQUEUE_ON_COMMIT` to False", errors[0].hint)  # type:ignore[arg-type]


class DummyBackendTransactionTestCase(TransactionTestCase):
    @override_settings(
        TASKS={
            "default": {
                "BACKEND": "django_tasks.backends.dummy.DummyBackend",
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
            test_tasks.noop_task.enqueue()

            self.assertEqual(len(default_task_backend.results), 0)  # type:ignore[attr-defined]

        self.assertEqual(len(default_task_backend.results), 1)  # type:ignore[attr-defined]

    @override_settings(
        TASKS={
            "default": {
                "BACKEND": "django_tasks.backends.dummy.DummyBackend",
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

            self.assertEqual(len(default_task_backend.results), 1)  # type:ignore[attr-defined]

        self.assertEqual(len(default_task_backend.results), 1)  # type:ignore[attr-defined]

    @override_settings(
        TASKS={
            "default": {
                "BACKEND": "django_tasks.backends.dummy.DummyBackend",
            }
        }
    )
    def test_wait_until_transaction_by_default(self) -> None:
        self.assertTrue(default_task_backend.enqueue_on_commit)
        self.assertTrue(
            default_task_backend._get_enqueue_on_commit_for_task(test_tasks.noop_task)
        )

        with transaction.atomic():
            result = test_tasks.noop_task.enqueue()

            self.assertIsNone(result.enqueued_at)

            self.assertEqual(len(default_task_backend.results), 0)  # type:ignore[attr-defined]

        self.assertEqual(len(default_task_backend.results), 1)  # type:ignore[attr-defined]
        self.assertIsNone(result.enqueued_at)
        result.refresh()
        self.assertIsNotNone(result.enqueued_at)

    @override_settings(
        TASKS={
            "default": {
                "BACKEND": "django_tasks.backends.dummy.DummyBackend",
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

        with transaction.atomic():
            result = test_tasks.enqueue_on_commit_task.enqueue()

            self.assertIsNone(result.enqueued_at)

            self.assertEqual(len(default_task_backend.results), 0)  # type:ignore[attr-defined]

        self.assertEqual(len(default_task_backend.results), 1)  # type:ignore[attr-defined]
        self.assertIsNone(result.enqueued_at)
        result.refresh()
        self.assertIsNotNone(result.enqueued_at)
