import dataclasses
from datetime import datetime, timedelta

from django.test import SimpleTestCase, override_settings
from django.utils import timezone
from django.utils.module_loading import import_string

from django_tasks import (
    DEFAULT_QUEUE_NAME,
    ResultStatus,
    Task,
    default_task_backend,
    task,
    tasks,
)
from django_tasks.backends.dummy import DummyBackend
from django_tasks.backends.immediate import ImmediateBackend
from django_tasks.exceptions import (
    InvalidTaskBackendError,
    InvalidTaskError,
    ResultDoesNotExist,
)
from django_tasks.task import MAX_PRIORITY, MIN_PRIORITY
from tests import tasks as test_tasks


@override_settings(
    TASKS={
        "default": {
            "BACKEND": "django_tasks.backends.dummy.DummyBackend",
            "QUEUES": ["default", "queue_1"],
        },
        "immediate": {"BACKEND": "django_tasks.backends.immediate.ImmediateBackend"},
        "missing": {"BACKEND": "does.not.exist"},
    }
)
class TaskTestCase(SimpleTestCase):
    def setUp(self) -> None:
        default_task_backend.clear()

    def test_using_correct_backend(self) -> None:
        self.assertEqual(default_task_backend, tasks["default"])
        self.assertIsInstance(tasks["default"], DummyBackend)

    def test_task_decorator(self) -> None:
        self.assertIsInstance(test_tasks.noop_task, Task)
        self.assertIsInstance(test_tasks.noop_task_async, Task)
        self.assertIsInstance(test_tasks.noop_task_from_bare_decorator, Task)

    def test_enqueue_task(self) -> None:
        result = test_tasks.noop_task.enqueue()

        self.assertEqual(result.status, ResultStatus.NEW)
        self.assertIs(result.task, test_tasks.noop_task)
        self.assertEqual(result.args, [])
        self.assertEqual(result.kwargs, {})

        self.assertEqual(default_task_backend.results, [result])

    async def test_enqueue_task_async(self) -> None:
        result = await test_tasks.noop_task.aenqueue()

        self.assertEqual(result.status, ResultStatus.NEW)
        self.assertIs(result.task, test_tasks.noop_task)
        self.assertEqual(result.args, [])
        self.assertEqual(result.kwargs, {})

        self.assertEqual(default_task_backend.results, [result])

    def test_using_priority(self) -> None:
        self.assertEqual(test_tasks.noop_task.priority, 0)
        self.assertEqual(test_tasks.noop_task.using(priority=1).priority, 1)
        self.assertEqual(test_tasks.noop_task.priority, 0)

    def test_using_queue_name(self) -> None:
        self.assertEqual(test_tasks.noop_task.queue_name, DEFAULT_QUEUE_NAME)
        self.assertEqual(
            test_tasks.noop_task.using(queue_name="queue_1").queue_name, "queue_1"
        )
        self.assertEqual(test_tasks.noop_task.queue_name, DEFAULT_QUEUE_NAME)

    def test_using_run_after(self) -> None:
        now = timezone.now()

        self.assertIsNone(test_tasks.noop_task.run_after)
        self.assertEqual(test_tasks.noop_task.using(run_after=now).run_after, now)
        self.assertIsInstance(
            test_tasks.noop_task.using(run_after=timedelta(hours=1)).run_after,
            datetime,
        )
        self.assertIsNone(test_tasks.noop_task.run_after)

    def test_using_unknown_backend(self) -> None:
        self.assertEqual(test_tasks.noop_task.backend, "default")

        with self.assertRaisesMessage(
            InvalidTaskBackendError, "The connection 'unknown' doesn't exist."
        ):
            test_tasks.noop_task.using(backend="unknown")

    def test_using_missing_backend(self) -> None:
        self.assertEqual(test_tasks.noop_task.backend, "default")

        with self.assertRaisesMessage(
            InvalidTaskBackendError,
            "Could not find backend 'does.not.exist': No module named 'does'",
        ):
            test_tasks.noop_task.using(backend="missing")

    def test_using_creates_new_instance(self) -> None:
        new_task = test_tasks.noop_task.using()

        self.assertEqual(new_task, test_tasks.noop_task)
        self.assertIsNot(new_task, test_tasks.noop_task)

    async def test_refresh_result(self) -> None:
        result = test_tasks.noop_task.enqueue()

        original_result = dataclasses.asdict(result)

        result.refresh()

        self.assertEqual(dataclasses.asdict(result), original_result)

        await result.arefresh()

        self.assertEqual(dataclasses.asdict(result), original_result)

    def test_naive_datetime(self) -> None:
        with self.assertRaisesMessage(
            InvalidTaskError, "run_after must be an aware datetime"
        ):
            test_tasks.noop_task.using(run_after=datetime.now())

    def test_invalid_priority(self) -> None:
        with self.assertRaisesMessage(
            InvalidTaskError,
            f"priority must be a whole number between {MIN_PRIORITY} and {MAX_PRIORITY}",
        ):
            test_tasks.noop_task.using(priority=-101)

        with self.assertRaisesMessage(
            InvalidTaskError,
            f"priority must be a whole number between {MIN_PRIORITY} and {MAX_PRIORITY}",
        ):
            test_tasks.noop_task.using(priority=101)

        with self.assertRaisesMessage(
            InvalidTaskError,
            f"priority must be a whole number between {MIN_PRIORITY} and {MAX_PRIORITY}",
        ):
            test_tasks.noop_task.using(priority=3.1)  # type:ignore[arg-type]

        test_tasks.noop_task.using(priority=100)
        test_tasks.noop_task.using(priority=-100)
        test_tasks.noop_task.using(priority=0)

    def test_call_task(self) -> None:
        self.assertEqual(test_tasks.calculate_meaning_of_life.call(), 42)

    async def test_call_task_async(self) -> None:
        self.assertEqual(await test_tasks.calculate_meaning_of_life.acall(), 42)

    async def test_call_async_task(self) -> None:
        self.assertIsNone(await test_tasks.noop_task_async.acall())

    def test_call_async_task_sync(self) -> None:
        self.assertIsNone(test_tasks.noop_task_async.call())

    def test_get_result(self) -> None:
        result = default_task_backend.enqueue(test_tasks.noop_task, (), {})

        new_result = test_tasks.noop_task.get_result(result.id)

        self.assertEqual(result, new_result)

    async def test_get_result_async(self) -> None:
        result = await default_task_backend.aenqueue(test_tasks.noop_task, (), {})

        new_result = await test_tasks.noop_task.aget_result(result.id)

        self.assertEqual(result, new_result)

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

    def test_name(self) -> None:
        self.assertEqual(test_tasks.noop_task.name, "noop_task")
        self.assertEqual(test_tasks.noop_task_async.name, "noop_task_async")

    def test_module_path(self) -> None:
        self.assertEqual(test_tasks.noop_task.module_path, "tests.tasks.noop_task")
        self.assertEqual(
            test_tasks.noop_task_async.module_path, "tests.tasks.noop_task_async"
        )

        self.assertIs(
            import_string(test_tasks.noop_task.module_path), test_tasks.noop_task
        )
        self.assertIs(
            import_string(test_tasks.noop_task_async.module_path),
            test_tasks.noop_task_async,
        )
