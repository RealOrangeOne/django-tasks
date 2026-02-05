import logging
from dataclasses import dataclass
from typing import Any
from unittest import mock

from django.test import SimpleTestCase, override_settings
from django.utils.version import PY311, PY312

from django_tasks import default_task_backend, task, task_backends
from django_tasks.backends.base import BaseTaskBackend
from django_tasks.base import Task
from django_tasks.exceptions import InvalidTaskError
from django_tasks.utils import get_module_path
from tests import tasks as test_tasks


@dataclass(frozen=True, slots=PY311, kw_only=True)  # type: ignore[literal-required]
class CustomTask(Task):
    other_data: str = ""


class CustomBackend(BaseTaskBackend):
    def __init__(self, alias: str, params: dict) -> None:
        super().__init__(alias, params)
        self.prefix = self.options.get("prefix", "")

    def enqueue(self, *args: Any, **kwargs: Any) -> Any:
        logger = logging.getLogger(__name__)
        logger.info(f"{self.prefix}Task enqueued.")


class CustomBackendNoEnqueue(BaseTaskBackend):
    pass


class CustomTaskBackend(BaseTaskBackend):
    task_class = CustomTask
    supports_priority = True

    def enqueue(self, *args: Any, **kwargs: Any) -> Any:
        pass


@override_settings(
    TASKS={
        "default": {
            "BACKEND": get_module_path(CustomBackend),
            "OPTIONS": {"prefix": "PREFIX: "},
        },
        "no_enqueue": {
            "BACKEND": get_module_path(CustomBackendNoEnqueue),
        },
    }
)
class CustomBackendTestCase(SimpleTestCase):
    def test_using_correct_backend(self) -> None:
        self.assertEqual(default_task_backend, task_backends["default"])
        self.assertIsInstance(task_backends["default"], CustomBackend)

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
        if PY312:
            error_message = "Can't instantiate abstract class CustomBackendNoEnqueue without an implementation for abstract method 'enqueue'"
        else:
            error_message = "Can't instantiate abstract class CustomBackendNoEnqueue with abstract method enqueue"

        with self.assertRaisesMessage(
            TypeError,
            error_message,
        ):
            test_tasks.noop_task.using(backend="no_enqueue")


@override_settings(
    TASKS={
        "default": {
            "BACKEND": f"{CustomTaskBackend.__module__}."
            f"{CustomTaskBackend.__qualname__}",
            "QUEUES": ["default", "high"],
        },
    }
)
class CustomTaskTestCase(SimpleTestCase):
    def test_custom_task_default_values(self) -> None:
        my_task = task()(test_tasks.noop_task.func)

        self.assertIsInstance(my_task, CustomTask)
        self.assertEqual(my_task.other_data, "")  # type: ignore[attr-defined]

    def test_custom_task_with_custom_values(self) -> None:
        my_task = task(other_data="other")(test_tasks.noop_task.func)

        self.assertIsInstance(my_task, CustomTask)
        self.assertEqual(my_task.other_data, "other")  # type: ignore[attr-defined]

    def test_custom_task_with_standard_and_custom_values(self) -> None:
        my_task = task(priority=10, queue_name="high", other_data="other")(
            test_tasks.noop_task.func
        )

        self.assertIsInstance(my_task, CustomTask)
        self.assertEqual(my_task.priority, 10)
        self.assertEqual(my_task.queue_name, "high")
        self.assertEqual(my_task.other_data, "other")  # type: ignore[attr-defined]
        self.assertFalse(my_task.takes_context)
        self.assertIsNone(my_task.run_after)

    def test_custom_task_invalid_argument(self) -> None:
        with self.assertRaises(TypeError):
            task(unknown_param=123)(test_tasks.noop_task.func)
