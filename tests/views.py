from typing import Any

from django.http import Http404, HttpRequest, HttpResponse, JsonResponse

from django_tasks import ResultStatus, default_task_backend
from django_tasks.exceptions import ResultDoesNotExist
from django_tasks.task import TaskResult

from . import tasks


def get_result_value(result: TaskResult) -> Any:
    if result.status == ResultStatus.SUCCEEDED:
        return result.return_value
    elif result.status == ResultStatus.FAILED:
        return result._exception_data

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
    except ResultDoesNotExist:
        raise Http404 from None

    return JsonResponse(
        {
            "result_id": result.id,
            "result": get_result_value(result),
            "status": result.status,
        }
    )
