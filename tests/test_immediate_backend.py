import dataclasses
import inspect

from django.test import SimpleTestCase, override_settings
from django.utils import timezone

from django_core_tasks import TaskStatus, default_task_backend, tasks
from django_core_tasks.backends.base import BaseTaskBackend
from django_core_tasks.backends.immediate import ImmediateBackend

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

    def test_executes_task(self):
        self.assertTrue(default_task_backend.supports_enqueue())

        task = default_task_backend.enqueue(test_tasks.noop_task)

        self.assertEqual(task.status, TaskStatus.COMPLETE, task.result)
        self.assertIsNone(task.result)
        self.assertEqual(task.func, test_tasks.noop_task)
        self.assertEqual(task.args, [])
        self.assertEqual(task.kwargs, {})

    async def test_execute_async(self):
        task = await default_task_backend.aenqueue(test_tasks.noop_task)

        self.assertEqual(task.status, TaskStatus.COMPLETE, task.result)
        self.assertIsNone(task.result)
        self.assertEqual(task.func, test_tasks.noop_task)
        self.assertEqual(task.args, [])
        self.assertEqual(task.kwargs, {})

    def test_executes_async_task(self):
        task = default_task_backend.enqueue(test_tasks.noop_task_async)

        self.assertEqual(task.status, TaskStatus.COMPLETE, task.result)
        self.assertIsNone(task.result)
        self.assertEqual(task.func, test_tasks.noop_task_async)
        self.assertEqual(task.args, [])
        self.assertEqual(task.kwargs, {})

    async def test_executes_async_task_async(self):
        task = await default_task_backend.aenqueue(test_tasks.noop_task_async)

        self.assertEqual(task.status, TaskStatus.COMPLETE, task.result)
        self.assertIsNone(task.result)
        self.assertEqual(task.func, test_tasks.noop_task_async)
        self.assertEqual(task.args, [])
        self.assertEqual(task.kwargs, {})

    def test_catches_exception(self):
        task = default_task_backend.enqueue(test_tasks.erroring_task)

        self.assertEqual(task.status, TaskStatus.FAILED, task.result)
        self.assertIsInstance(task.result, ValueError)
        self.assertEqual(task.func, test_tasks.erroring_task)
        self.assertEqual(task.args, [])
        self.assertEqual(task.kwargs, {})

    async def test_cannot_defer(self):
        self.assertFalse(default_task_backend.supports_defer())

        with self.assertRaisesMessage(
            NotImplementedError, "This backend does not support `defer`."
        ):
            default_task_backend.defer(test_tasks.noop_task, when=timezone.now())

        with self.assertRaisesMessage(
            NotImplementedError, "This backend does not support `defer`."
        ):
            await default_task_backend.adefer(test_tasks.noop_task, when=timezone.now())

    async def test_cannot_get_task(self):
        with self.assertRaisesMessage(
            NotImplementedError,
            "This backend does not support retrieving existing tasks.",
        ):
            default_task_backend.get_task("123")

        with self.assertRaisesMessage(
            NotImplementedError,
            "This backend does not support retrieving existing tasks.",
        ):
            await default_task_backend.aget_task(123)

    async def test_refresh_task(self):
        task = default_task_backend.enqueue(test_tasks.noop_task)

        original_task = dataclasses.asdict(task)

        task.refresh()

        self.assertEqual(dataclasses.asdict(task), original_task)

        await task.arefresh()

        self.assertEqual(dataclasses.asdict(task), original_task)

    def test_enqueue_signature(self):
        self.assertEqual(
            inspect.signature(ImmediateBackend.enqueue),
            inspect.signature(BaseTaskBackend.enqueue),
        )