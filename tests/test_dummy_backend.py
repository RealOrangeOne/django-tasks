from django.test import SimpleTestCase, override_settings

from django_core_tasks import TaskStatus, default_task_backend, tasks
from django_core_tasks.backends.dummy import DummyBackend

from . import tasks as test_tasks


@override_settings(
    TASKS={"default": {"BACKEND": "django_core_tasks.backends.dummy.DummyBackend"}}
)
class DummyBackendTestCase(SimpleTestCase):
    def setUp(self):
        default_task_backend.clear()

    def test_using_correct_backend(self):
        self.assertEqual(default_task_backend, tasks["default"])
        self.assertIsInstance(tasks["default"], DummyBackend)

    def test_enqueue_task(self):
        result = default_task_backend.enqueue(test_tasks.noop_task, (), {})

        self.assertEqual(result.status, TaskStatus.NEW)
        self.assertEqual(result.task, test_tasks.noop_task)
        self.assertEqual(result.args, ())
        self.assertEqual(result.kwargs, {})

        self.assertEqual(default_task_backend.results, [result])
