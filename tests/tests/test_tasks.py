import dataclasses
from datetime import datetime, timedelta

from django.test import SimpleTestCase, override_settings
from django.utils import timezone

from django_core_tasks import TaskStatus, default_task_backend, task, tasks
from django_core_tasks.backends.dummy import DummyBackend
from django_core_tasks.backends.immediate import ImmediateBackend
from django_core_tasks.exceptions import InvalidTaskError, ResultDoesNotExist
from tests import tasks as test_tasks


@override_settings(
    TASKS={
        "default": {"BACKEND": "django_core_tasks.backends.dummy.DummyBackend"},
        "immediate": {
            "BACKEND": "django_core_tasks.backends.immediate.ImmediateBackend"
        },
    }
)
class TaskTestCase(SimpleTestCase):
    def setUp(self) -> None:
        default_task_backend.clear()

    def test_using_correct_backend(self) -> None:
        self.assertEqual(default_task_backend, tasks["default"])
        self.assertIsInstance(tasks["default"], DummyBackend)

    def test_enqueue_task(self) -> None:
        result = test_tasks.noop_task.enqueue()

        self.assertEqual(result.status, TaskStatus.NEW)
        self.assertIs(result.task, test_tasks.noop_task)
        self.assertEqual(result.args, ())
        self.assertEqual(result.kwargs, {})

        self.assertEqual(default_task_backend.results, [result])

    async def test_enqueue_task_async(self) -> None:
        result = await test_tasks.noop_task.aenqueue()

        self.assertEqual(result.status, TaskStatus.NEW)
        self.assertIs(result.task, test_tasks.noop_task)
        self.assertEqual(result.args, ())
        self.assertEqual(result.kwargs, {})

        self.assertEqual(default_task_backend.results, [result])

    def test_using_priority(self) -> None:
        self.assertIsNone(test_tasks.noop_task.priority)
        self.assertEqual(test_tasks.noop_task.using(priority=1).priority, 1)
        self.assertIsNone(test_tasks.noop_task.priority)

    def test_using_queue_name(self) -> None:
        self.assertIsNone(test_tasks.noop_task.queue_name)
        self.assertEqual(
            test_tasks.noop_task.using(queue_name="queue_1").queue_name, "queue_1"
        )
        self.assertIsNone(test_tasks.noop_task.queue_name)

    def test_using_run_after(self) -> None:
        now = timezone.now()

        self.assertIsNone(test_tasks.noop_task.run_after)
        self.assertEqual(test_tasks.noop_task.using(run_after=now).run_after, now)
        self.assertIsInstance(
            test_tasks.noop_task.using(run_after=timedelta(hours=1)).run_after,
            datetime,
        )
        self.assertIsNone(test_tasks.noop_task.run_after)

    async def test_refresh_result(self) -> None:
        result = test_tasks.noop_task.enqueue()

        original_result = dataclasses.asdict(result)

        result.refresh()

        self.assertEqual(dataclasses.asdict(result), original_result)

        await result.arefresh()

        self.assertEqual(dataclasses.asdict(result), original_result)

    async def test_naive_datetime(self) -> None:
        with self.assertRaisesMessage(
            InvalidTaskError, "run_after must be an aware datetime"
        ):
            test_tasks.noop_task.using(run_after=datetime.now()).enqueue()

        with self.assertRaisesMessage(
            InvalidTaskError, "run_after must be an aware datetime"
        ):
            await test_tasks.noop_task.using(run_after=datetime.now()).aenqueue()

    async def test_invalid_priority(self) -> None:
        with self.assertRaisesMessage(InvalidTaskError, "priority must be positive"):
            test_tasks.noop_task.using(priority=0).enqueue()

        with self.assertRaisesMessage(InvalidTaskError, "priority must be positive"):
            await test_tasks.noop_task.using(priority=0).aenqueue()

    def test_call_task(self) -> None:
        self.assertEqual(test_tasks.calculate_meaning_of_life(), 42)

    def test_get_result(self) -> None:
        result = default_task_backend.enqueue(test_tasks.noop_task, (), {})

        new_result = test_tasks.noop_task.get_result(result.id)

        self.assertIs(result, new_result)

    async def test_get_result_async(self) -> None:
        result = await default_task_backend.aenqueue(test_tasks.noop_task, (), {})

        new_result = await test_tasks.noop_task.aget_result(result.id)

        self.assertIs(result, new_result)

    async def test_get_missing_result(self) -> None:
        with self.assertRaises(ResultDoesNotExist):
            test_tasks.noop_task.get_result("123")

        with self.assertRaises(ResultDoesNotExist):
            await test_tasks.noop_task.aget_result("123")

    async def test_get_incorrect_result(self) -> None:
        result = default_task_backend.enqueue(test_tasks.noop_task_async, (), {})

        with self.assertRaises(ResultDoesNotExist):
            test_tasks.noop_task.get_result(result.id)

        with self.assertRaises(ResultDoesNotExist):
            await test_tasks.noop_task.aget_result(result.id)

    def test_invalid_function(self) -> None:
        for invalid_function in [any, self.test_invalid_function]:
            with self.subTest(invalid_function):
                with self.assertRaisesMessage(
                    InvalidTaskError,
                    "Task function must be a globally importable function",
                ):
                    task()(invalid_function)  # type:ignore[arg-type]

    def test_get_backend(self) -> None:
        self.assertEqual(test_tasks.noop_task.backend, "default")
        self.assertIsInstance(test_tasks.noop_task.get_backend(), DummyBackend)

        immediate_task = test_tasks.noop_task.using(backend="immediate")
        self.assertEqual(immediate_task.backend, "immediate")
        self.assertIsInstance(immediate_task.get_backend(), ImmediateBackend)
