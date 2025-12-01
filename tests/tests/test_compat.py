from unittest import skipUnless

from django import VERSION
from django.test import SimpleTestCase, override_settings

from django_tasks import compat, task, task_backends
from django_tasks.backends.immediate import ImmediateBackend
from django_tasks.base import Task, TaskResult

HAS_DJANGO_TASKS = VERSION >= (6, 0)


def _test_task_func() -> None:
    pass


class DjangoCompatTestCase(SimpleTestCase):
    def test_uses_lib_tasks_by_default(self) -> None:
        self.assertIsInstance(task_backends["default"], ImmediateBackend)
        self.assertEqual(task_backends["default"].task_class, Task)

    @skipUnless(HAS_DJANGO_TASKS, "Requires django.tasks")
    def test_django_using_lib_backend(self) -> None:
        from django.tasks import task, task_backends

        with override_settings(
            TASKS={
                "default": {
                    "BACKEND": "django_tasks.backends.immediate.ImmediateBackend"
                }
            }
        ):
            self.assertIsInstance(task_backends["default"], ImmediateBackend)

            test_task_func_task = task(_test_task_func)
            self.assertIsInstance(test_task_func_task, Task)

            result = test_task_func_task.enqueue()
            self.assertIsInstance(result, TaskResult)
            self.assertIsNone(result.return_value)

    @skipUnless(HAS_DJANGO_TASKS, "Requires django.tasks")
    def test_lib_using_django_backend(self) -> None:
        from django.tasks.backends.immediate import ImmediateBackend
        from django.tasks.base import Task, TaskResult

        with override_settings(
            TASKS={
                "default": {
                    "BACKEND": "django.tasks.backends.immediate.ImmediateBackend"
                }
            }
        ):
            self.assertIsInstance(task_backends["default"], ImmediateBackend)

            test_task_func_task = task(_test_task_func)
            self.assertIsInstance(test_task_func_task, Task)

            result = test_task_func_task.enqueue()
            self.assertIsInstance(result, TaskResult)
            self.assertIsNone(result.return_value)

    def test_compat_has_django_task(self) -> None:
        self.assertIn(Task, compat.TASK_CLASSES)

        if HAS_DJANGO_TASKS:
            from django.tasks.base import Task as DjangoTask

            self.assertIn(DjangoTask, compat.TASK_CLASSES)
