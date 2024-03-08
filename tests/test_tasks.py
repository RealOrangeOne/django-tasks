import inspect

from django.test import SimpleTestCase, override_settings
from django.utils import timezone

from django_core_tasks import (
    adefer,
    aenqueue,
    default_task_backend,
    defer,
    enqueue,
    tasks,
)
from django_core_tasks.backends.base import BaseTaskBackend
from django_core_tasks.backends.dummy import DummyBackend
from django_core_tasks.backends.immediate import ImmediateBackend
from django_core_tasks.exceptions import InvalidTaskBackendError

from . import tasks as test_tasks


class TasksTestCase(SimpleTestCase):
    @override_settings(TASKS={"default": {"BACKEND": "invalid.module"}})
    def test_unknown_module(self):
        with self.assertRaisesMessage(
            InvalidTaskBackendError,
            "Could not find backend 'invalid.module': No module named 'invalid'",
        ):
            tasks["default"].supports_defer()

    @override_settings(TASKS={"default": {}})
    def test_missing_backend(self):
        with self.assertRaisesMessage(
            InvalidTaskBackendError, "No backend specified for default"
        ):
            tasks["default"].supports_defer()

    def test_override_during_tests(self):
        with override_settings(
            TASKS={
                "default": {
                    "BACKEND": "django_core_tasks.backends.immediate.ImmediateBackend"
                }
            }
        ):
            self.assertIsInstance(tasks["default"], BaseTaskBackend)
            self.assertIsInstance(tasks["default"], ImmediateBackend)

        with override_settings(
            TASKS={
                "default": {
                    "BACKEND": "django_core_tasks.backends.base.BaseTaskBackend"
                }
            }
        ):
            self.assertIsInstance(tasks["default"], BaseTaskBackend)
            self.assertNotIsInstance(tasks["default"], ImmediateBackend)


@override_settings(
    TASKS={"default": {"BACKEND": "django_core_tasks.backends.dummy.DummyBackend"}}
)
class ShortcutTestCase(SimpleTestCase):
    def setUp(self):
        default_task_backend.clear()

    def test_uses_correct_backend(self):
        with override_settings(
            TASKS={
                "default": {
                    "BACKEND": "django_core_tasks.backends.immediate.ImmediateBackend"
                },
                "other": {"BACKEND": "django_core_tasks.backends.dummy.DummyBackend"},
            }
        ):
            self.assertEqual(default_task_backend, tasks["default"])
            self.assertIsInstance(tasks["default"], ImmediateBackend)
            self.assertIsInstance(tasks["other"], DummyBackend)

    def test_shortcut_signatures(self):
        base_backend = BaseTaskBackend(options={})

        self.assertEqual(
            inspect.signature(enqueue), inspect.signature(base_backend.enqueue)
        )
        self.assertEqual(
            inspect.signature(defer), inspect.signature(base_backend.defer)
        )
        self.assertEqual(
            inspect.signature(aenqueue), inspect.signature(base_backend.aenqueue)
        )
        self.assertEqual(
            inspect.signature(adefer), inspect.signature(base_backend.adefer)
        )

    def test_enqueue(self):
        task = enqueue(test_tasks.noop_task)
        self.assertEqual(default_task_backend.tasks, [task])

    async def test_enqueue_async(self):
        task = await aenqueue(test_tasks.noop_task)
        self.assertEqual(default_task_backend.tasks, [task])

    def test_defer(self):
        task = defer(test_tasks.noop_task, when=timezone.now())
        self.assertEqual(default_task_backend.tasks, [task])

    async def test_defer_async(self):
        task = await adefer(test_tasks.noop_task, when=timezone.now())
        self.assertEqual(default_task_backend.tasks, [task])
