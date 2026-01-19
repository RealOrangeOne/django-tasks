import time
from typing import cast

from django.test import SimpleTestCase, override_settings
from django.utils import timezone

from django_tasks import TaskResultStatus, default_task_backend, task_backends
from django_tasks.backends.threadpool import ThreadPoolBackend
from django_tasks.base import Task, TaskResult
from django_tasks.exceptions import InvalidTaskError
from tests import tasks as test_tasks


@override_settings(
    TASKS={
        "default": {
            "BACKEND": "django_tasks.backends.threadpool.ThreadPoolBackend",
            "QUEUES": [],
        }
    }
)
class ThreadPoolBackendTestCase(SimpleTestCase):
    MAX_WAIT_SECONDS = 5

    def _wait_for_task(self, result: TaskResult) -> None:
        start = time.perf_counter()
        while not result.is_finished:
            # Explicitly yield to the other thread
            time.sleep(0)

            if time.perf_counter() - start >= self.MAX_WAIT_SECONDS:
                raise RuntimeError("Task took too long to finish")

        # Add a small sleep afterwards to ensure the thread has fully finished
        time.sleep(0.01)

    def test_using_correct_backend(self) -> None:
        self.assertEqual(default_task_backend, task_backends["default"])
        self.assertIsInstance(task_backends["default"], ThreadPoolBackend)
        self.assertEqual(default_task_backend.alias, "default")
        self.assertEqual(default_task_backend.options, {})

    def test_enqueue_task(self) -> None:
        for task in [test_tasks.noop_task, test_tasks.noop_task_async]:
            with self.subTest(task):
                result = cast(Task, task).enqueue(1, two=3)

                self._wait_for_task(result)

                self.assertEqual(result.status, TaskResultStatus.SUCCEEDED)
                self.assertTrue(result.is_finished)
                self.assertIsNotNone(result.started_at)
                self.assertIsNotNone(result.last_attempted_at)
                self.assertIsNotNone(result.finished_at)
                self.assertGreaterEqual(result.started_at, result.enqueued_at)  # type:ignore[arg-type, misc]
                self.assertGreaterEqual(result.finished_at, result.started_at)  # type:ignore[arg-type, misc]
                self.assertIsNone(result.return_value)
                self.assertEqual(result.task, task)
                self.assertEqual(result.args, [1])
                self.assertEqual(result.kwargs, {"two": 3})
                self.assertEqual(result.attempts, 1)
                self.assertEqual(result.metadata, {})

    async def test_enqueue_task_async(self) -> None:
        for task in [test_tasks.noop_task, test_tasks.noop_task_async]:
            with self.subTest(task):
                result = await cast(Task, task).aenqueue()

                self._wait_for_task(result)

                self.assertEqual(result.status, TaskResultStatus.SUCCEEDED)
                self.assertTrue(result.is_finished)
                self.assertIsNotNone(result.started_at)
                self.assertIsNotNone(result.last_attempted_at)
                self.assertIsNotNone(result.finished_at)
                self.assertGreaterEqual(result.started_at, result.enqueued_at)  # type:ignore[arg-type, misc]
                self.assertGreaterEqual(result.finished_at, result.started_at)  # type:ignore[arg-type, misc]
                self.assertIsNone(result.return_value)
                self.assertEqual(result.task, task)
                self.assertEqual(result.args, [])
                self.assertEqual(result.kwargs, {})
                self.assertEqual(result.attempts, 1)
                self.assertEqual(result.metadata, {})

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
                with self.assertLogs("django_tasks", level="ERROR") as captured_logs:
                    result = task.enqueue()
                    self._wait_for_task(result)

                # assert logging
                self.assertEqual(len(captured_logs.output), 1)
                self.assertIn(message, captured_logs.output[0])

                # assert result
                self.assertEqual(result.status, TaskResultStatus.FAILED)
                with self.assertRaisesMessage(ValueError, "Task failed"):
                    result.return_value  # noqa: B018
                self.assertTrue(result.is_finished)
                self.assertIsNotNone(result.started_at)
                self.assertIsNotNone(result.last_attempted_at)
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
        with self.assertLogs("django_tasks", level="ERROR"):
            result = test_tasks.complex_exception.enqueue()
            self._wait_for_task(result)

        self.assertEqual(result.status, TaskResultStatus.FAILED)
        self.assertIsNotNone(result.started_at)
        self.assertIsNotNone(result.last_attempted_at)
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

    def test_complex_return_value(self) -> None:
        with self.assertLogs("django_tasks", level="ERROR"):
            result = test_tasks.complex_return_value.enqueue()
            self._wait_for_task(result)

        self.assertEqual(result.status, TaskResultStatus.FAILED)
        self.assertIsNotNone(result.started_at)
        self.assertIsNotNone(result.last_attempted_at)
        self.assertIsNotNone(result.finished_at)
        self.assertGreaterEqual(result.started_at, result.enqueued_at)  # type:ignore[arg-type,misc]
        self.assertGreaterEqual(result.finished_at, result.started_at)  # type:ignore[arg-type,misc]

        self.assertIsNone(result._return_value)
        self.assertEqual(result.errors[0].exception_class, TypeError)
        self.assertIn("Unsupported type", result.errors[0].traceback)

    def test_result(self) -> None:
        result = default_task_backend.enqueue(
            test_tasks.calculate_meaning_of_life, [], {}
        )
        self._wait_for_task(result)

        self.assertEqual(result.status, TaskResultStatus.SUCCEEDED)
        self.assertEqual(result.return_value, 42)

    async def test_result_async(self) -> None:
        result = await default_task_backend.aenqueue(
            test_tasks.calculate_meaning_of_life, [], {}
        )
        self._wait_for_task(result)

        self.assertEqual(result.status, TaskResultStatus.SUCCEEDED)
        self.assertEqual(result.return_value, 42)

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
            await default_task_backend.aget_result(123)  # type:ignore[arg-type]

    async def test_cannot_refresh_result(self) -> None:
        result = await default_task_backend.aenqueue(
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
                test_tasks.failing_task_value_error.using(run_after=timezone.now())
            )

    def test_enqueue_logs(self) -> None:
        with self.assertLogs("django_tasks", level="DEBUG") as captured_logs:
            result = test_tasks.noop_task.enqueue()
            self._wait_for_task(result)

        self.assertEqual(len(captured_logs.output), 3)

        self.assertIn("enqueued", captured_logs.output[0])
        self.assertIn(result.id, captured_logs.output[0])

        self.assertIn("state=RUNNING", captured_logs.output[1])
        self.assertIn(result.id, captured_logs.output[1])

        self.assertIn("state=SUCCEEDED", captured_logs.output[2])
        self.assertIn(result.id, captured_logs.output[2])

    def test_failed_logs(self) -> None:
        with self.assertLogs("django_tasks", level="DEBUG") as captured_logs:
            result = test_tasks.failing_task_value_error.enqueue()
            self._wait_for_task(result)

        self.assertEqual(len(captured_logs.output), 3, captured_logs.output)
        self.assertIn("state=RUNNING", captured_logs.output[1])
        self.assertIn(result.id, captured_logs.output[1])

        self.assertIn("state=FAILED", captured_logs.output[2])
        self.assertIn(result.id, captured_logs.output[2])

    def test_check(self) -> None:
        errors = list(default_task_backend.check())

        self.assertEqual(len(errors), 0, errors)

    def test_takes_context(self) -> None:
        result = test_tasks.get_task_id.enqueue()

        self._wait_for_task(result)

        self.assertEqual(result.return_value, result.id)

    def test_context(self) -> None:
        result = test_tasks.test_context.enqueue(1)
        self._wait_for_task(result)
        self.assertEqual(result.status, TaskResultStatus.SUCCEEDED)

    def test_metadata(self) -> None:
        result = test_tasks.add_to_metadata.enqueue({"foo": "bar"})
        self._wait_for_task(result)
        self.assertEqual(result.status, TaskResultStatus.SUCCEEDED)
        self.assertEqual(result.metadata["foo"], "bar")

    def test_save_metadata(self) -> None:
        for task in [test_tasks.save_metadata, test_tasks.asave_metadata]:
            with self.subTest(task):
                result = task.enqueue()  # type: ignore[attr-defined]
                self._wait_for_task(result)
                self.assertEqual(result.status, TaskResultStatus.SUCCEEDED)
                self.assertEqual(result.metadata["flushes"], "flush 2")

    def test_validate_on_enqueue(self) -> None:
        task_with_custom_queue_name = test_tasks.noop_task.using(
            queue_name="unknown_queue"
        )

        with override_settings(
            TASKS={
                "default": {
                    "BACKEND": "django_tasks.backends.immediate.ImmediateBackend",
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
                    "BACKEND": "django_tasks.backends.immediate.ImmediateBackend",
                    "QUEUES": ["queue-1"],
                }
            }
        ):
            with self.assertRaisesMessage(
                InvalidTaskError, "Queue 'unknown_queue' is not valid for backend"
            ):
                await task_with_custom_queue_name.aenqueue()
