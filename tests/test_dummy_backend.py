from django.test import SimpleTestCase, override_settings

from django_core_tasks import TaskStatus, default_task_backend, tasks
from django_core_tasks.backends.dummy import DummyBackend
from django_core_tasks.exceptions import ResultDoesNotExist

from . import tasks as test_tasks


@override_settings(
    TASKS={"default": {"BACKEND": "django_core_tasks.backends.dummy.DummyBackend"}}
)
class DummyBackendTestCase(SimpleTestCase):
    def setUp(self) -> None:
        default_task_backend.clear()

    def test_using_correct_backend(self) -> None:
        self.assertEqual(default_task_backend, tasks["default"])
        self.assertIsInstance(tasks["default"], DummyBackend)

    def test_enqueue_task(self) -> None:
        for task in [test_tasks.noop_task, test_tasks.noop_task_async]:
            with self.subTest(task):
                result = default_task_backend.enqueue(task, (1,), {"two": 3})

                self.assertEqual(result.status, TaskStatus.NEW)
                with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
                    result.result  # noqa:B018
                self.assertEqual(result.task, task)
                self.assertEqual(result.args, (1,))
                self.assertEqual(result.kwargs, {"two": 3})

                self.assertIn(result, default_task_backend.results)

    async def test_enqueue_task_async(self) -> None:
        for task in [test_tasks.noop_task, test_tasks.noop_task_async]:
            with self.subTest(task):
                result = await default_task_backend.aenqueue(task, (), {})

                self.assertEqual(result.status, TaskStatus.NEW)
                with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
                    result.result  # noqa:B018
                self.assertEqual(result.task, task)
                self.assertEqual(result.args, ())
                self.assertEqual(result.kwargs, {})

                self.assertIn(result, default_task_backend.results)

    def test_get_result(self) -> None:
        result = default_task_backend.enqueue(test_tasks.noop_task, (), {})

        new_result = default_task_backend.get_result(result.id)

        self.assertIs(result, new_result)

    async def test_get_result_async(self) -> None:
        result = await default_task_backend.aenqueue(test_tasks.noop_task, (), {})

        new_result = await default_task_backend.aget_result(result.id)

        self.assertIs(result, new_result)

    async def test_get_missing_result(self) -> None:
        with self.assertRaises(ResultDoesNotExist):
            default_task_backend.get_result("123")

        with self.assertRaises(ResultDoesNotExist):
            await default_task_backend.aget_result("123")
