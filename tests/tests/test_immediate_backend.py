import json

from django.test import SimpleTestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from django_core_tasks import TaskStatus, default_task_backend, tasks
from django_core_tasks.backends.immediate import ImmediateBackend
from django_core_tasks.exceptions import InvalidTaskError
from tests import tasks as test_tasks


@override_settings(
    TASKS={
        "default": {"BACKEND": "django_core_tasks.backends.immediate.ImmediateBackend"}
    }
)
class ImmediateBackendTestCase(SimpleTestCase):
    def test_using_correct_backend(self) -> None:
        self.assertEqual(default_task_backend, tasks["default"])
        self.assertIsInstance(tasks["default"], ImmediateBackend)

    def test_enqueue_task(self) -> None:
        for task in [test_tasks.noop_task, test_tasks.noop_task_async]:
            with self.subTest(task):
                result = default_task_backend.enqueue(task, (1,), {"two": 3})

                self.assertEqual(result.status, TaskStatus.COMPLETE)
                self.assertIsNone(result.result)
                self.assertEqual(result.task, task)
                self.assertEqual(result.args, (1,))
                self.assertEqual(result.kwargs, {"two": 3})

    async def test_enqueue_task_async(self) -> None:
        for task in [test_tasks.noop_task, test_tasks.noop_task_async]:
            with self.subTest(task):
                result = await default_task_backend.aenqueue(task, (), {})

                self.assertEqual(result.status, TaskStatus.COMPLETE)
                self.assertIsNone(result.result)
                self.assertEqual(result.task, task)
                self.assertEqual(result.args, ())
                self.assertEqual(result.kwargs, {})

    def test_catches_exception(self) -> None:
        result = default_task_backend.enqueue(test_tasks.failing_task, (), {})

        self.assertEqual(result.status, TaskStatus.FAILED)
        self.assertIsInstance(result.result, ValueError)
        self.assertEqual(result.task, test_tasks.failing_task)
        self.assertEqual(result.args, ())
        self.assertEqual(result.kwargs, {})

    def test_result(self) -> None:
        result = default_task_backend.enqueue(
            test_tasks.calculate_meaning_of_life, (), {}
        )

        self.assertEqual(result.status, TaskStatus.COMPLETE)
        self.assertEqual(result.result, 42)

    async def test_result_async(self) -> None:
        result = await default_task_backend.aenqueue(
            test_tasks.calculate_meaning_of_life, (), {}
        )

        self.assertEqual(result.status, TaskStatus.COMPLETE)
        self.assertEqual(result.result, 42)

    async def test_cannot_get_result(self) -> None:
        with self.assertRaisesMessage(
            NotImplementedError,
            "This backend does not support retrieving results.",
        ):
            default_task_backend.get_result("123")

        with self.assertRaisesMessage(
            NotImplementedError,
            "This backend does not support retrieving results.",
        ):
            await default_task_backend.get_result(123)

    def test_cannot_pass_run_after(self) -> None:
        with self.assertRaisesMessage(
            InvalidTaskError,
            "Backend does not support run_after",
        ):
            default_task_backend.validate_task(
                test_tasks.failing_task.using(run_after=timezone.now())
            )

    def test_meaning_of_life_view(self) -> None:
        response = self.client.get(reverse("meaning-of-life"))
        self.assertEqual(response.status_code, 200)

        data = json.loads(response.content)

        self.assertEqual(data["result"], 42)
        self.assertEqual(data["status"], TaskStatus.COMPLETE)
