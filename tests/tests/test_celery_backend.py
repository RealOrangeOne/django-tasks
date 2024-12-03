from datetime import timedelta
from unittest.mock import patch

from celery import Celery
from celery.result import AsyncResult
from django.db import transaction
from django.test import TestCase, TransactionTestCase, override_settings
from django.utils import timezone

from django_tasks import ResultStatus, default_task_backend, task, tasks
from django_tasks.backends.celery import CeleryBackend
from django_tasks.backends.celery.backend import _map_priority
from django_tasks.task import DEFAULT_PRIORITY, DEFAULT_QUEUE_NAME


def noop_task(*args: tuple, **kwargs: dict) -> None:
    return None


def enqueue_on_commit_task(*args: tuple, **kwargs: dict) -> None:
    pass


@override_settings(
    TASKS={
        "default": {
            "BACKEND": "django_tasks.backends.celery.CeleryBackend",
            "QUEUES": [DEFAULT_QUEUE_NAME, "queue-1"],
        }
    }
)
class CeleryBackendTestCase(TransactionTestCase):
    def setUp(self) -> None:
        # register task during setup so it is registered as a Celery task
        self.task = task()(noop_task)
        self.enqueue_on_commit_task = task(enqueue_on_commit=True)(
            enqueue_on_commit_task
        )

    def test_using_correct_backend(self) -> None:
        self.assertEqual(default_task_backend, tasks["default"])
        self.assertIsInstance(tasks["default"], CeleryBackend)

    def test_check(self) -> None:
        errors = list(default_task_backend.check())

        self.assertEqual(len(errors), 0)

    @override_settings(INSTALLED_APPS=[])
    def test_celery_backend_app_missing(self) -> None:
        errors = list(default_task_backend.check())

        self.assertEqual(len(errors), 1)
        self.assertIn("django_tasks.backends.celery", errors[0].hint)  # type:ignore[arg-type]

    def test_enqueue_task(self) -> None:
        task = self.task
        assert task.celery_task  # type: ignore[attr-defined]

        # import here so that it is not set as default before registering the task
        from django_tasks.backends.celery.app import app as celery_app

        self.assertEqual(task.celery_task.app, celery_app)  # type: ignore[attr-defined]
        task_id = "123"
        with patch("django_tasks.backends.celery.backend.uuid", return_value=task_id):
            with patch("celery.app.task.Task.apply_async") as mock_apply_async:
                mock_apply_async.return_value = AsyncResult(id=task_id)
                result = default_task_backend.enqueue(task, (1,), {"two": 3})

        self.assertEqual(result.id, task_id)
        self.assertEqual(result.status, ResultStatus.NEW)
        self.assertIsNone(result.started_at)
        self.assertIsNone(result.finished_at)
        with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
            result.return_value  # noqa:B018
        self.assertEqual(result.task, task)
        self.assertEqual(result.args, (1,))
        self.assertEqual(result.kwargs, {"two": 3})
        expected_priority = _map_priority(DEFAULT_PRIORITY)
        mock_apply_async.assert_called_once_with(
            (1,),
            kwargs={"two": 3},
            task_id=task_id,
            eta=None,
            priority=expected_priority,
            queue=DEFAULT_QUEUE_NAME,
        )

    def test_using_additional_params(self) -> None:
        task_id = "123"
        with patch("django_tasks.backends.celery.backend.uuid", return_value=task_id):
            with patch("celery.app.task.Task.apply_async") as mock_apply_async:
                mock_apply_async.return_value = AsyncResult(id=task_id)
                run_after = timezone.now() + timedelta(hours=10)
                result = self.task.using(
                    run_after=run_after, priority=75, queue_name="queue-1"
                ).enqueue()

        self.assertEqual(result.id, task_id)
        self.assertEqual(result.status, ResultStatus.NEW)
        mock_apply_async.assert_called_once_with(
            [], kwargs={}, task_id=task_id, eta=run_after, priority=7, queue="queue-1"
        )

    def test_priority_mapping(self) -> None:
        for priority, expected in [(-100, 0), (-50, 2), (0, 4), (75, 7), (100, 9)]:
            task_id = "123"
            with patch(
                "django_tasks.backends.celery.backend.uuid", return_value=task_id
            ):
                with patch("celery.app.task.Task.apply_async") as mock_apply_async:
                    mock_apply_async.return_value = AsyncResult(id=task_id)
                    self.task.using(priority=priority).enqueue()

            mock_apply_async.assert_called_with(
                [],
                kwargs={},
                task_id=task_id,
                eta=None,
                priority=expected,
                queue=DEFAULT_QUEUE_NAME,
            )

    @override_settings(
        TASKS={
            "default": {
                "BACKEND": "django_tasks.backends.celery.CeleryBackend",
                "ENQUEUE_ON_COMMIT": True,
            }
        }
    )
    def test_wait_until_transaction_commit(self) -> None:
        self.assertTrue(default_task_backend.enqueue_on_commit)
        self.assertTrue(default_task_backend._get_enqueue_on_commit_for_task(self.task))

        with patch("celery.app.task.Task.apply_async") as mock_apply_async:
            mock_apply_async.return_value = AsyncResult(id="task_id")
            with transaction.atomic():
                self.task.enqueue()
                assert not mock_apply_async.called

        mock_apply_async.assert_called_once()

    @override_settings(
        TASKS={
            "default": {
                "BACKEND": "django_tasks.backends.celery.CeleryBackend",
            }
        }
    )
    def test_wait_until_transaction_by_default(self) -> None:
        self.assertTrue(default_task_backend.enqueue_on_commit)
        self.assertTrue(default_task_backend._get_enqueue_on_commit_for_task(self.task))

    @override_settings(
        TASKS={
            "default": {
                "BACKEND": "django_tasks.backends.celery.CeleryBackend",
                "ENQUEUE_ON_COMMIT": False,
            }
        }
    )
    def test_task_specific_enqueue_on_commit(self) -> None:
        self.assertFalse(default_task_backend.enqueue_on_commit)
        self.assertTrue(self.enqueue_on_commit_task.enqueue_on_commit)
        self.assertTrue(
            default_task_backend._get_enqueue_on_commit_for_task(
                self.enqueue_on_commit_task
            )
        )


@override_settings(
    TASKS={
        "default": {
            "BACKEND": "django_tasks.backends.celery.CeleryBackend",
            "QUEUES": [DEFAULT_QUEUE_NAME, "queue-1"],
        }
    }
)
class CeleryBackendCustomAppTestCase(TestCase):
    def setUp(self) -> None:
        self.celery_app = Celery("test_app")
        self.task = task()(noop_task)

    def tearDown(self) -> None:
        # restore the default Celery app
        from django_tasks.backends.celery.app import app as celery_app

        celery_app.set_current()
        return super().tearDown()

    def test_enqueue_task(self) -> None:
        task = self.task
        assert task.celery_task  # type: ignore[attr-defined]

        from django_tasks.backends.celery.app import app as celery_app

        self.assertNotEqual(celery_app, self.celery_app)
        # it should use the custom Celery app
        self.assertEqual(task.celery_task.app, self.celery_app)  # type: ignore[attr-defined]
