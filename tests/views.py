from django.http import Http404, HttpRequest, HttpResponse, JsonResponse

from django_tasks import default_task_backend
from django_tasks.exceptions import TaskRunDoesNotExist

from . import tasks


def calculate_meaning_of_life(request: HttpRequest) -> HttpResponse:
    result = tasks.calculate_meaning_of_life.enqueue()

    return JsonResponse(
        {
            "result_id": result.id,
            "result": result.get_task_run(),
            "status": result.status,
        }
    )


async def calculate_meaning_of_life_async(request: HttpRequest) -> HttpResponse:
    result = await tasks.calculate_meaning_of_life.aenqueue()

    return JsonResponse(
        {
            "result_id": result.id,
            "result": result.get_task_run(),
            "status": result.status,
        }
    )


async def get_task_run(request: HttpRequest, result_id: str) -> HttpResponse:
    try:
        result = await default_task_backend.aget_task_run(result_id)
    except TaskRunDoesNotExist:
        raise Http404 from None

    return JsonResponse(
        {
            "result_id": result.id,
            "result": result.get_task_run(),
            "status": result.status,
        }
    )
