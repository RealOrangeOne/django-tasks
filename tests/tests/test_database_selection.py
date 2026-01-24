from django.test import TestCase, override_settings

from django_tasks.backends.database.models import DBTaskResult
from tests import tasks as test_tasks


class DatabaseSelectionTestCase(TestCase):
    databases = {"default", "secondary"}

    @override_settings(
        TASKS={
            "default": {
                "BACKEND": "django_tasks.backends.database.DatabaseBackend",
                "OPTIONS": {"database": "default"},
            },
            "secondary": {
                "BACKEND": "django_tasks.backends.database.DatabaseBackend",
                "OPTIONS": {"database": "secondary"},
            },
        }
    )
    def test_enqueue_to_different_databases(self):
        result_default = test_tasks.calculate_meaning_of_life.enqueue()

        result_secondary = test_tasks.calculate_meaning_of_life.using(
            backend="secondary"
        ).enqueue()

        self.assertTrue(
            DBTaskResult.objects.using("default").filter(id=result_default.id).exists()
        )
        self.assertFalse(
            DBTaskResult.objects.using("secondary")
            .filter(id=result_default.id)
            .exists()
        )

        self.assertTrue(
            DBTaskResult.objects.using("secondary")
            .filter(id=result_secondary.id)
            .exists()
        )
        self.assertFalse(
            DBTaskResult.objects.using("default")
            .filter(id=result_secondary.id)
            .exists()
        )

    @override_settings(
        TASKS={
            "secondary": {
                "BACKEND": "django_tasks.backends.database.DatabaseBackend",
                "OPTIONS": {"database": "secondary"},
            },
        }
    )
    def test_get_result_from_correct_database(self):
        from django_tasks import task_backends

        backend = task_backends["secondary"]

        db_result = DBTaskResult.objects.using("secondary").create(
            task_path="tests.tasks.calculate_meaning_of_life",
            args_kwargs={"args": [], "kwargs": {}},
            run_after=test_tasks.calculate_meaning_of_life.run_after,
            backend_name="secondary",
        )

        retrieved_result = backend.get_result(str(db_result.id))
        self.assertEqual(retrieved_result.id, str(db_result.id))

        with self.assertRaises(DBTaskResult.DoesNotExist):
            DBTaskResult.objects.using("default").get(id=db_result.id)
