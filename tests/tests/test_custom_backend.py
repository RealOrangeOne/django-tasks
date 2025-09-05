import logging
from typing import Any
from unittest import mock

from django.test import SimpleTestCase, override_settings

from django_tasks import default_task_backend, tasks
from django_tasks.backends.base import BaseTaskBackend
from django_tasks.exceptions import InvalidTaskError
from django_tasks.utils import get_module_path
from tests import tasks as test_tasks


class CustomBackend(BaseTaskBackend):
    def __init__(self, alias: str, params: dict) -> None:
        super().__init__(alias, params)
        self.prefix = self.options.get("prefix", "")

    def enqueue(self, *args: Any, **kwargs: Any) -> Any:
        logger = logging.getLogger(__name__)
        logger.info(f"{self.prefix}Task enqueued.")


class CustomBackendNoEnqueue(BaseTaskBackend):
    pass


@override_settings(
    TASKS={
        "default": {
            "BACKEND": get_module_path(CustomBackend),
            "ENQUEUE_ON_COMMIT": False,
            "OPTIONS": {"prefix": "PREFIX: "},
        },
        "no_enqueue": {
            "BACKEND": get_module_path(CustomBackendNoEnqueue),
        },
    }
)
class CustomBackendTestCase(SimpleTestCase):
    def test_using_correct_backend(self) -> None:
        self.assertEqual(default_task_backend, tasks["default"])
        self.assertIsInstance(tasks["default"], CustomBackend)

    @mock.patch.multiple(CustomBackend, supports_async_task=False)
    def test_enqueue_async_task_on_non_async_backend(self) -> None:
        with self.assertRaisesMessage(
            InvalidTaskError, "Backend does not support async tasks"
        ):
            default_task_backend.validate_task(test_tasks.noop_task_async)

    def test_backend_does_not_support_priority(self) -> None:
        with self.assertRaisesMessage(
            InvalidTaskError, "Backend does not support setting priority of tasks."
        ):
            test_tasks.noop_task.using(priority=10)

    def test_options(self) -> None:
        with self.assertLogs(__name__, level="INFO") as captured_logs:
            test_tasks.noop_task.enqueue()
        self.assertEqual(len(captured_logs.output), 1)
        self.assertIn("PREFIX: Task enqueued", captured_logs.output[0])

    def test_no_enqueue(self) -> None:
        with self.assertRaisesMessage(
            TypeError,
            "Can't instantiate abstract class CustomBackendNoEnqueue "
            "without an implementation for abstract method 'enqueue'",
        ):
            test_tasks.noop_task.using(backend="no_enqueue")
