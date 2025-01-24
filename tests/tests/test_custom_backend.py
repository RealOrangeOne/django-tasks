from typing import Any
from unittest import mock

from django.test import SimpleTestCase, override_settings

from django_tasks import default_task_backend, tasks
from django_tasks.backends.base import BaseTaskBackend
from django_tasks.exceptions import InvalidTaskError
from django_tasks.utils import get_module_path
from tests import tasks as test_tasks


class CustomBackend(BaseTaskBackend):
    def enqueue(self, *args: Any, **kwargs: Any) -> Any:
        pass


@override_settings(
    TASKS={
        "default": {
            "BACKEND": get_module_path(CustomBackend),
            "ENQUEUE_ON_COMMIT": False,
        }
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
