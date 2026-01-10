from typing import Any

from django.http import Http404, HttpRequest, HttpResponse, JsonResponse

from django_tasks import TaskResultStatus, default_task_backend
from django_tasks.base import TaskResult
from django_tasks.exceptions import TaskResultDoesNotExist

from . import tasks


def get_result_value(result: TaskResult) -> Any:
    if result.status == TaskResultStatus.SUCCESSFUL:
        return result.return_value
    elif result.status == TaskResultStatus.FAILED:
        return result.errors[0].traceback

    return None


def calculate_meaning_of_life(request: HttpRequest) -> HttpResponse:
    result = tasks.calculate_meaning_of_life.enqueue()

    return JsonResponse(
        {
            "result_id": result.id,
            "result": get_result_value(result),
            "status": result.status,
        }
    )


async def calculate_meaning_of_life_async(request: HttpRequest) -> HttpResponse:
    result = await tasks.calculate_meaning_of_life.aenqueue()

    return JsonResponse(
        {
            "result_id": result.id,
            "result": get_result_value(result),
            "status": result.status,
        }
    )


async def get_task_result(request: HttpRequest, result_id: str) -> HttpResponse:
    try:
        result = await default_task_backend.aget_result(result_id)
    except TaskResultDoesNotExist:
        raise Http404 from None

    return JsonResponse(
        {
            "result_id": result.id,
            "result": get_result_value(result),
            "status": result.status,
        }
    )
