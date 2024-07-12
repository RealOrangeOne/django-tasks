import json

from django.test import SimpleTestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from django_tasks import ResultStatus, default_task_backend, tasks
from django_tasks.backends.immediate import ImmediateBackend
from django_tasks.exceptions import InvalidTaskError
from tests import tasks as test_tasks


@override_settings(
    TASKS={"default": {"BACKEND": "django_tasks.backends.immediate.ImmediateBackend"}}
)
class ImmediateBackendTestCase(SimpleTestCase):
    def test_using_correct_backend(self) -> None:
        self.assertEqual(default_task_backend, tasks["default"])
        self.assertIsInstance(tasks["default"], ImmediateBackend)

    def test_enqueue_task(self) -> None:
        for task in [test_tasks.noop_task, test_tasks.noop_task_async]:
            with self.subTest(task):
                result = default_task_backend.enqueue(task, (1,), {"two": 3})

                self.assertEqual(result.status, ResultStatus.COMPLETE)
                self.assertIsNotNone(result.started_at)
                self.assertIsNotNone(result.finished_at)
                self.assertGreaterEqual(result.started_at, result.enqueued_at)
                self.assertGreaterEqual(result.finished_at, result.started_at)
                self.assertIsNone(result.result)
                self.assertEqual(result.task, task)
                self.assertEqual(result.args, [1])
                self.assertEqual(result.kwargs, {"two": 3})

    async def test_enqueue_task_async(self) -> None:
        for task in [test_tasks.noop_task, test_tasks.noop_task_async]:
            with self.subTest(task):
                result = await default_task_backend.aenqueue(task, (), {})

                self.assertEqual(result.status, ResultStatus.COMPLETE)
                self.assertIsNotNone(result.started_at)
                self.assertIsNotNone(result.finished_at)
                self.assertGreaterEqual(result.started_at, result.enqueued_at)
                self.assertGreaterEqual(result.finished_at, result.started_at)
                self.assertIsNone(result.result)
                self.assertIsNone(result.get_result())
                self.assertEqual(result.task, task)
                self.assertEqual(result.args, [])
                self.assertEqual(result.kwargs, {})

    def test_catches_exception(self) -> None:
        result = default_task_backend.enqueue(test_tasks.failing_task, [], {})

        self.assertEqual(result.status, ResultStatus.FAILED)
        self.assertIsNotNone(result.started_at)
        self.assertIsNotNone(result.finished_at)
        self.assertGreaterEqual(result.started_at, result.enqueued_at)
        self.assertGreaterEqual(result.finished_at, result.started_at)
        self.assertIsInstance(result.result, ValueError)
        self.assertIsNone(result.get_result())
        self.assertEqual(result.task, test_tasks.failing_task)
        self.assertEqual(result.args, [])
        self.assertEqual(result.kwargs, {})

    def test_complex_exception(self) -> None:
        with self.assertLogs("django_tasks.backends.immediate", level="ERROR"):
            result = default_task_backend.enqueue(test_tasks.complex_exception, [], {})

        self.assertEqual(result.status, ResultStatus.FAILED)
        self.assertIsNotNone(result.started_at)
        self.assertIsNotNone(result.finished_at)
        self.assertGreaterEqual(result.started_at, result.enqueued_at)
        self.assertGreaterEqual(result.finished_at, result.started_at)
        self.assertIsNone(result.result, TypeError)
        self.assertIsNone(result.get_result())
        self.assertEqual(result.task, test_tasks.complex_exception)
        self.assertEqual(result.args, [])
        self.assertEqual(result.kwargs, {})

    def test_result(self) -> None:
        result = default_task_backend.enqueue(
            test_tasks.calculate_meaning_of_life, [], {}
        )

        self.assertEqual(result.status, ResultStatus.COMPLETE)
        self.assertEqual(result.result, 42)

    async def test_result_async(self) -> None:
        result = await default_task_backend.aenqueue(
            test_tasks.calculate_meaning_of_life, [], {}
        )

        self.assertEqual(result.status, ResultStatus.COMPLETE)
        self.assertEqual(result.result, 42)

    async def test_cannot_get_result(self) -> None:
        with self.assertRaisesMessage(
            NotImplementedError,
            "This backend does not support retrieving or refreshing results.",
        ):
            default_task_backend.get_result("123")

        with self.assertRaisesMessage(
            NotImplementedError,
            "This backend does not support retrieving or refreshing results.",
        ):
            await default_task_backend.get_result(123)

    async def test_cannot_refresh_result(self) -> None:
        result = default_task_backend.enqueue(
            test_tasks.calculate_meaning_of_life, (), {}
        )

        with self.assertRaisesMessage(
            NotImplementedError,
            "This backend does not support retrieving or refreshing results.",
        ):
            await result.arefresh()

        with self.assertRaisesMessage(
            NotImplementedError,
            "This backend does not support retrieving or refreshing results.",
        ):
            result.refresh()

    def test_cannot_pass_run_after(self) -> None:
        with self.assertRaisesMessage(
            InvalidTaskError,
            "Backend does not support run_after",
        ):
            default_task_backend.validate_task(
                test_tasks.failing_task.using(run_after=timezone.now())
            )

    def test_meaning_of_life_view(self) -> None:
        for url in [
            reverse("meaning-of-life"),
            reverse("meaning-of-life-async"),
        ]:
            with self.subTest(url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 200)

                data = json.loads(response.content)

                self.assertEqual(data["result"], 42)
                self.assertEqual(data["status"], ResultStatus.COMPLETE)

    def test_get_result_from_different_request(self) -> None:
        response = self.client.get(reverse("meaning-of-life"))
        self.assertEqual(response.status_code, 200)

        data = json.loads(response.content)
        result_id = data["result_id"]

        with self.assertRaisesMessage(
            NotImplementedError,
            "This backend does not support retrieving or refreshing results.",
        ):
            response = self.client.get(reverse("result", args=[result_id]))
