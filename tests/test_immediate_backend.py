from django.test import SimpleTestCase, override_settings
from django.utils import timezone

from django_core_tasks import TaskStatus, default_task_backend, tasks
from django_core_tasks.backends.immediate import ImmediateBackend
from django_core_tasks.exceptions import InvalidTaskError

from . import tasks as test_tasks


@override_settings(
    TASKS={
        "default": {"BACKEND": "django_core_tasks.backends.immediate.ImmediateBackend"}
    }
)
class ImmediateBackendTestCase(SimpleTestCase):
    def test_using_correct_backend(self):
        self.assertEqual(default_task_backend, tasks["default"])
        self.assertIsInstance(tasks["default"], ImmediateBackend)

    def test_enqueue_task(self):
        for task in [test_tasks.noop_task, test_tasks.noop_task_async]:
            with self.subTest(task):
                result = default_task_backend.enqueue(task, (1,), {"two": 3})

                self.assertEqual(result.status, TaskStatus.COMPLETE)
                self.assertIsNone(result.result)
                self.assertEqual(result.task, task)
                self.assertEqual(result.args, (1,))
                self.assertEqual(result.kwargs, {"two": 3})

    async def test_enqueue_task_async(self):
        for task in [test_tasks.noop_task, test_tasks.noop_task_async]:
            with self.subTest(task):
                result = await default_task_backend.aenqueue(task, (), {})

                self.assertEqual(result.status, TaskStatus.COMPLETE)
                self.assertIsNone(result.result)
                self.assertEqual(result.task, task)
                self.assertEqual(result.args, ())
                self.assertEqual(result.kwargs, {})

    def test_catches_exception(self):
        result = default_task_backend.enqueue(test_tasks.failing_task, (), {})

        self.assertEqual(result.status, TaskStatus.FAILED)
        self.assertIsInstance(result.result, ValueError)
        self.assertEqual(result.task, test_tasks.failing_task)
        self.assertEqual(result.args, ())
        self.assertEqual(result.kwargs, {})

    async def test_cannot_get_result(self):
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

    def test_cannot_pass_run_after(self):
        with self.assertRaisesMessage(
            InvalidTaskError,
            "Immediate backend does not support run_after",
        ):
            default_task_backend.validate_task(
                test_tasks.failing_task.using(run_after=timezone.now())
            )
