from datetime import timedelta
from unittest.mock import patch

from celery import Celery
from celery.result import AsyncResult
from django.test import TestCase, override_settings
from django.utils import timezone

from django_tasks import ResultStatus, default_task_backend, task, tasks
from django_tasks.backends.celery import CeleryBackend
from django_tasks.backends.celery.backend import _map_priority
from django_tasks.task import DEFAULT_PRIORITY, DEFAULT_QUEUE_NAME


def noop_task(*args: tuple, **kwargs: dict) -> None:
    return None


@override_settings(
    TASKS={
        "default": {
            "BACKEND": "django_tasks.backends.celery.CeleryBackend",
            "QUEUES": [DEFAULT_QUEUE_NAME, "queue-1"],
        }
    }
)
class CeleryBackendTestCase(TestCase):
    def setUp(self) -> None:
        # register task during setup so it is registered as a Celery task
        self.task = task()(noop_task)

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
        self.assertIn("django_tasks.backends.celery", errors[0].hint)

    def test_enqueue_task(self) -> None:
        task = self.task
        assert task.celery_task  # type: ignore[attr-defined]

        # import here so that it is not set as default before registering the task
        from django_tasks.backends.celery.app import app as celery_app

        self.assertEqual(task.celery_task.app, celery_app)  # type: ignore[attr-defined]
        with patch("celery.app.task.Task.apply_async") as mock_apply_async:
            mock_apply_async.return_value = AsyncResult(id="123")
            result = default_task_backend.enqueue(task, (1,), {"two": 3})

        self.assertEqual(result.id, "123")
        self.assertEqual(result.status, ResultStatus.NEW)
        self.assertIsNone(result.started_at)
        self.assertIsNone(result.finished_at)
        with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
            result.result  # noqa:B018
        self.assertEqual(result.task, task)
        self.assertEqual(result.args, [1])
        self.assertEqual(result.kwargs, {"two": 3})
        expected_priority = _map_priority(DEFAULT_PRIORITY)
        mock_apply_async.assert_called_once_with(
            (1,),
            kwargs={"two": 3},
            eta=None,
            priority=expected_priority,
            queue=DEFAULT_QUEUE_NAME,
        )

    def test_using_additional_params(self) -> None:
        with patch("celery.app.task.Task.apply_async") as mock_apply_async:
            mock_apply_async.return_value = AsyncResult(id="123")
            run_after = timezone.now() + timedelta(hours=10)
            result = self.task.using(
                run_after=run_after, priority=75, queue_name="queue-1"
            ).enqueue()

        self.assertEqual(result.id, "123")
        self.assertEqual(result.status, ResultStatus.NEW)
        mock_apply_async.assert_called_once_with(
            (), kwargs={}, eta=run_after, priority=7, queue="queue-1"
        )

    def test_priority_mapping(self) -> None:
        for priority, expected in [(-100, 0), (-50, 2), (0, 4), (75, 7), (100, 9)]:
            with patch("celery.app.task.Task.apply_async") as mock_apply_async:
                mock_apply_async.return_value = AsyncResult(id="123")
                self.task.using(priority=priority).enqueue()

            mock_apply_async.assert_called_with(
                (), kwargs={}, eta=None, priority=expected, queue=DEFAULT_QUEUE_NAME
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
