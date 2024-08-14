import secrets

from django.test import SimpleTestCase, override_settings

from django_tasks import (
    ResultStatus,
    default_task_backend,
)
from tests import tasks as test_tasks

# test ser/de, sign, and validate


@override_settings(
    TASKS={
        "default": {
            "BACKEND": "django_tasks.backends.dummy.DummyBackend",
            "QUEUES": ["default", "queue_1"],
            "SIGN_TASKS": True,
        },
        "immediate": {"BACKEND": "django_tasks.backends.immediate.ImmediateBackend"},
        "missing": {"BACKEND": "does.not.exist"},
    }
)
class TaskTestCase(SimpleTestCase):
    def setUp(self) -> None:
        default_task_backend.clear()  # type:ignore[attr-defined]

    def test_sign_and_update_task(self) -> None:
        result = test_tasks.noop_task.enqueue()
        result.status = ResultStatus.NEW
        signature = result.sign()
        result.status = ResultStatus.COMPLETE
        self.assertEqual(result.validate(signature), True)

    def test_sign_and_validate_task(self) -> None:
        result = test_tasks.noop_task.enqueue()
        signature = result.sign()
        self.assertEqual(result.validate(signature), True)

    def test_sign_and_validate_task_modified(self) -> None:
        result = test_tasks.noop_task.enqueue()
        signature = result.sign()
        result.args = ["modified"]
        self.assertNotEqual(result.validate(signature), True)

    def test_sign_and_validate_task_with_salt(self) -> None:
        result = test_tasks.noop_task.enqueue()
        salt = secrets.token_hex(8)
        signature = result.sign(salt=salt)
        self.assertEqual(result.validate(signature, salt=salt), True)

    def test_sign_and_validate_task_with_salt_sha256(self) -> None:
        result = test_tasks.noop_task.enqueue()
        salt = secrets.token_hex(8)
        signature = result.sign(salt=salt, algorithm="sha256")
        self.assertEqual(
            result.validate(signature, salt=salt, algorithm="sha256"), True
        )
