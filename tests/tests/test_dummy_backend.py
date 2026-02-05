import json
from typing import cast
from unittest import mock

from django.test import (
    SimpleTestCase,
    override_settings,
)
from django.urls import reverse

from django_tasks import TaskResultStatus, default_task_backend, task_backends
from django_tasks.backends.dummy import DummyBackend
from django_tasks.base import Task
from django_tasks.exceptions import InvalidTaskError, TaskResultDoesNotExist
from tests import tasks as test_tasks


@override_settings(
    TASKS={
        "default": {
            "BACKEND": "django_tasks.backends.dummy.DummyBackend",
            "QUEUES": [],
        }
    }
)
class DummyBackendTestCase(SimpleTestCase):
    def setUp(self) -> None:
        default_task_backend.clear()  # type:ignore[attr-defined]

    def test_using_correct_backend(self) -> None:
        self.assertEqual(default_task_backend, task_backends["default"])
        self.assertIsInstance(task_backends["default"], DummyBackend)
        self.assertEqual(default_task_backend.alias, "default")
        self.assertEqual(default_task_backend.options, {})

    def test_enqueue_task(self) -> None:
        for task in [test_tasks.noop_task, test_tasks.noop_task_async]:
            with self.subTest(task):
                result = cast(Task, task).enqueue(1, two=3)

                self.assertEqual(result.status, TaskResultStatus.READY)
                self.assertFalse(result.is_finished)
                self.assertIsNone(result.started_at)
                self.assertIsNone(result.last_attempted_at)
                self.assertIsNone(result.finished_at)
                with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
                    result.return_value  # noqa:B018
                self.assertEqual(result.task, task)
                self.assertEqual(result.args, [1])
                self.assertEqual(result.kwargs, {"two": 3})
                self.assertEqual(result.attempts, 0)

                self.assertIn(result, default_task_backend.results)  # type:ignore[attr-defined]

    async def test_enqueue_task_async(self) -> None:
        for task in [test_tasks.noop_task, test_tasks.noop_task_async]:
            with self.subTest(task):
                result = await cast(Task, task).aenqueue()

                self.assertEqual(result.status, TaskResultStatus.READY)
                self.assertFalse(result.is_finished)
                self.assertIsNone(result.started_at)
                self.assertIsNone(result.last_attempted_at)
                self.assertIsNone(result.finished_at)
                with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
                    result.return_value  # noqa:B018
                self.assertEqual(result.task, task)
                self.assertEqual(result.args, [])
                self.assertEqual(result.kwargs, {})
                self.assertEqual(result.attempts, 0)

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
        object.__setattr__(enqueued_result, "status", TaskResultStatus.SUCCESSFUL)

        self.assertEqual(result.status, TaskResultStatus.READY)
        result.refresh()
        self.assertEqual(result.status, TaskResultStatus.SUCCESSFUL)

    async def test_refresh_result_async(self) -> None:
        result = await default_task_backend.aenqueue(
            test_tasks.calculate_meaning_of_life, (), {}
        )

        enqueued_result = default_task_backend.results[0]  # type:ignore[attr-defined]
        object.__setattr__(enqueued_result, "status", TaskResultStatus.SUCCESSFUL)

        self.assertEqual(result.status, TaskResultStatus.READY)
        await result.arefresh()
        self.assertEqual(result.status, TaskResultStatus.SUCCESSFUL)

    async def test_get_missing_result(self) -> None:
        with self.assertRaises(TaskResultDoesNotExist):
            default_task_backend.get_result("123")

        with self.assertRaises(TaskResultDoesNotExist):
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
                self.assertEqual(data["status"], TaskResultStatus.READY)

                result = default_task_backend.get_result(data["result_id"])
                self.assertEqual(result.status, TaskResultStatus.READY)

    def test_get_result_from_different_request(self) -> None:
        response = self.client.get(reverse("meaning-of-life"))
        self.assertEqual(response.status_code, 200)

        data = json.loads(response.content)
        result_id = data["result_id"]

        response = self.client.get(reverse("result", args=[result_id]))
        self.assertEqual(response.status_code, 200)

        self.assertEqual(
            json.loads(response.content),
            {"result_id": result_id, "result": None, "status": TaskResultStatus.READY},
        )

    def test_enqueue_logs(self) -> None:
        with self.assertLogs("django_tasks", level="DEBUG") as captured_logs:
            result = test_tasks.noop_task.enqueue()

        self.assertEqual(len(captured_logs.output), 1)
        self.assertIn("enqueued", captured_logs.output[0])
        self.assertIn(result.id, captured_logs.output[0])

    def test_errors(self) -> None:
        result = test_tasks.noop_task.enqueue()

        self.assertEqual(result.errors, [])

    def test_validate_disallowed_async_task(self) -> None:
        with mock.patch.multiple(default_task_backend, supports_async_task=False):
            with self.assertRaisesMessage(
                InvalidTaskError, "Backend does not support async tasks"
            ):
                default_task_backend.validate_task(test_tasks.noop_task_async)

    def test_check(self) -> None:
        errors = list(default_task_backend.check())

        self.assertEqual(len(errors), 0, errors)

    def test_takes_context(self) -> None:
        result = test_tasks.get_task_id.enqueue()
        self.assertEqual(result.status, TaskResultStatus.READY)

    def test_clear(self) -> None:
        result = test_tasks.noop_task.enqueue()

        default_task_backend.get_result(result.id)

        default_task_backend.clear()  # type:ignore[attr-defined]

        with self.assertRaisesMessage(TaskResultDoesNotExist, result.id):
            default_task_backend.get_result(result.id)

    def test_validate_on_enqueue(self) -> None:
        task_with_custom_queue_name = test_tasks.noop_task.using(
            queue_name="unknown_queue"
        )

        with override_settings(
            TASKS={
                "default": {
                    "BACKEND": "django_tasks.backends.dummy.DummyBackend",
                    "QUEUES": ["queue-1"],
                }
            }
        ):
            with self.assertRaisesMessage(
                InvalidTaskError, "Queue 'unknown_queue' is not valid for backend"
            ):
                task_with_custom_queue_name.enqueue()

    async def test_validate_on_aenqueue(self) -> None:
        task_with_custom_queue_name = test_tasks.noop_task.using(
            queue_name="unknown_queue"
        )

        with override_settings(
            TASKS={
                "default": {
                    "BACKEND": "django_tasks.backends.dummy.DummyBackend",
                    "QUEUES": ["queue-1"],
                }
            }
        ):
            with self.assertRaisesMessage(
                InvalidTaskError, "Queue 'unknown_queue' is not valid for backend"
            ):
                await task_with_custom_queue_name.aenqueue()
