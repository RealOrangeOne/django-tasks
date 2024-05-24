import json
import uuid

from django.test import TestCase, override_settings
from django.urls import reverse

from django_tasks import ResultStatus, default_task_backend, tasks
from django_tasks.backends.database import DatabaseBackend
from django_tasks.backends.database.models import DBTaskResult
from django_tasks.exceptions import ResultDoesNotExist
from tests import tasks as test_tasks


@override_settings(
    TASKS={"default": {"BACKEND": "django_tasks.backends.database.DatabaseBackend"}}
)
class DatabaseBackendTestCase(TestCase):
    def test_using_correct_backend(self) -> None:
        self.assertEqual(default_task_backend, tasks["default"])
        self.assertIsInstance(tasks["default"], DatabaseBackend)

    def test_enqueue_task(self) -> None:
        for task in [test_tasks.noop_task, test_tasks.noop_task_async]:
            with self.subTest(task), self.assertNumQueries(1):
                result = default_task_backend.enqueue(task, (1,), {"two": 3})

                self.assertEqual(result.status, ResultStatus.NEW)
                with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
                    result.result  # noqa:B018
                self.assertEqual(result.task, task)
                self.assertEqual(result.args, [1])
                self.assertEqual(result.kwargs, {"two": 3})

    async def test_enqueue_task_async(self) -> None:
        for task in [test_tasks.noop_task, test_tasks.noop_task_async]:
            with self.subTest(task):
                result = await default_task_backend.aenqueue(task, [], {})

                self.assertEqual(result.status, ResultStatus.NEW)
                with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
                    result.result  # noqa:B018
                self.assertEqual(result.task, task)
                self.assertEqual(result.args, [])
                self.assertEqual(result.kwargs, {})

    def test_get_result(self) -> None:
        with self.assertNumQueries(1):
            result = default_task_backend.enqueue(test_tasks.noop_task, [], {})

        with self.assertNumQueries(1):
            new_result = default_task_backend.get_result(result.id)

        self.assertEqual(result, new_result)

    async def test_get_result_async(self) -> None:
        result = await default_task_backend.aenqueue(test_tasks.noop_task, [], {})

        new_result = await default_task_backend.aget_result(result.id)

        self.assertEqual(result, new_result)

    def test_refresh_result(self) -> None:
        result = default_task_backend.enqueue(
            test_tasks.calculate_meaning_of_life, (), {}
        )

        DBTaskResult.objects.all().update(status=ResultStatus.COMPLETE)

        self.assertEqual(result.status, ResultStatus.NEW)
        with self.assertNumQueries(1):
            result.refresh()
        self.assertEqual(result.status, ResultStatus.COMPLETE)

    async def test_refresh_result_async(self) -> None:
        result = await default_task_backend.aenqueue(
            test_tasks.calculate_meaning_of_life, (), {}
        )

        await DBTaskResult.objects.all().aupdate(status=ResultStatus.COMPLETE)

        self.assertEqual(result.status, ResultStatus.NEW)
        await result.arefresh()
        self.assertEqual(result.status, ResultStatus.COMPLETE)

    def test_get_missing_result(self) -> None:
        with self.assertRaises(ResultDoesNotExist):
            default_task_backend.get_result(uuid.uuid4())

    async def test_async_get_missing_result(self) -> None:
        with self.assertRaises(ResultDoesNotExist):
            await default_task_backend.aget_result(uuid.uuid4())

    def test_invalid_uuid(self) -> None:
        with self.assertRaises(ResultDoesNotExist):
            default_task_backend.get_result("123")

    async def test_async_invalid_uuid(self) -> None:
        with self.assertRaises(ResultDoesNotExist):
            await default_task_backend.aget_result("123")

    def test_meaning_of_life_view(self) -> None:
        for url in [
            reverse("meaning-of-life"),
            reverse("meaning-of-life-async"),
        ]:
            with self.subTest(url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 200)

                data = json.loads(response.content)

                self.assertEqual(data["result"], None)
                self.assertEqual(data["status"], ResultStatus.NEW)

                result = default_task_backend.get_result(data["result_id"])
                self.assertEqual(result.status, ResultStatus.NEW)

    def test_get_result_from_different_request(self) -> None:
        response = self.client.get(reverse("meaning-of-life"))
        self.assertEqual(response.status_code, 200)

        data = json.loads(response.content)
        result_id = data["result_id"]

        response = self.client.get(reverse("result", args=[result_id]))
        self.assertEqual(response.status_code, 200)

        self.assertEqual(
            json.loads(response.content),
            {"result_id": result_id, "result": None, "status": ResultStatus.NEW},
        )
