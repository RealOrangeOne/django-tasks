import dataclasses
import inspect

from django.test import SimpleTestCase, override_settings
from django.utils import timezone

from django_core_tasks import TaskCandidate, TaskStatus, default_task_backend, tasks
from django_core_tasks.backends.base import BaseTaskBackend
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

    def test_executes_task(self):
        self.assertTrue(default_task_backend.supports_enqueue())

        task = default_task_backend.enqueue(test_tasks.calculate_meaning_of_life)

        self.assertEqual(task.status, TaskStatus.COMPLETE, task.result)
        self.assertEqual(task.result, 42)
        self.assertEqual(task.func, test_tasks.calculate_meaning_of_life)
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

        with self.assertRaisesMessage(
            NotImplementedError, "This backend does not support `bulk_defer`."
        ):
            default_task_backend.bulk_defer(
                TaskCandidate(test_tasks.noop_task, when=timezone.now())
            )

        with self.assertRaisesMessage(
            NotImplementedError, "This backend does not support `bulk_defer`."
        ):
            await default_task_backend.abulk_defer(
                TaskCandidate(test_tasks.noop_task, when=timezone.now())
            )

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

    async def test_enqueue_invalid_task(self):
        with self.assertRaises(InvalidTaskError):
            default_task_backend.enqueue(lambda: True)

        with self.assertRaises(InvalidTaskError):
            await default_task_backend.aenqueue(lambda: True)

    async def test_invalid_priority(self):
        with self.assertRaisesMessage(ValueError, "priority must be positive"):
            default_task_backend.enqueue(test_tasks.noop_task, priority=0)

        with self.assertRaisesMessage(ValueError, "priority must be positive"):
            await default_task_backend.aenqueue(test_tasks.noop_task, priority=0)

    def test_bulk_enqueue_tasks(self):
        created_tasks = default_task_backend.bulk_enqueue(
            [
                TaskCandidate(test_tasks.calculate_meaning_of_life),
                TaskCandidate(test_tasks.noop_task),
            ]
        )

        self.assertEqual(len(created_tasks), 2)

        self.assertEqual(created_tasks[0].func, test_tasks.calculate_meaning_of_life)
        self.assertEqual(created_tasks[1].func, test_tasks.noop_task)

        self.assertEqual(
            created_tasks[0].status, TaskStatus.COMPLETE, created_tasks[0].result
        )
        self.assertEqual(
            created_tasks[1].status, TaskStatus.COMPLETE, created_tasks[1].result
        )

    async def test_bulk_enqueue_tasks_async(self):
        created_tasks = await default_task_backend.abulk_enqueue(
            [
                TaskCandidate(test_tasks.calculate_meaning_of_life),
                TaskCandidate(test_tasks.noop_task),
            ]
        )

        self.assertEqual(len(created_tasks), 2)

        self.assertEqual(created_tasks[0].func, test_tasks.calculate_meaning_of_life)
        self.assertEqual(created_tasks[1].func, test_tasks.noop_task)

        self.assertEqual(
            created_tasks[0].status, TaskStatus.COMPLETE, created_tasks[0].result
        )
        self.assertEqual(
            created_tasks[1].status, TaskStatus.COMPLETE, created_tasks[1].result
        )

    async def test_cannot_bulk_defer(self):
        with self.assertRaisesMessage(
            NotImplementedError, "This backend does not support `bulk_defer`."
        ):
            default_task_backend.bulk_defer(TaskCandidate(test_tasks.noop_task))

        with self.assertRaisesMessage(
            NotImplementedError, "This backend does not support `bulk_defer`."
        ):
            await default_task_backend.abulk_defer(TaskCandidate(test_tasks.noop_task))
