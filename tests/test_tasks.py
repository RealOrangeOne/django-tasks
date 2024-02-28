from django_core_tasks import (
    enqueue,
    defer,
    aenqueue,
    adefer,
    default_task_backend,
    tasks,
)
from django.test import SimpleTestCase, override_settings
from django_core_tasks.backends.base import BaseTaskBackend
import inspect
from django_core_tasks.backends.immediate import ImmediateBackend
from django_core_tasks.backends.dummy import DummyBackend
from django_core_tasks.exceptions import InvalidTaskBackendError


class TasksTestCase(SimpleTestCase):
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
