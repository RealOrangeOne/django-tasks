from django.http import HttpRequest, HttpResponse, JsonResponse

from . import tasks


def calculate_meaning_of_life(request: HttpRequest) -> HttpResponse:
    result = tasks.calculate_meaning_of_life.enqueue()

    try:
        task_result = result.result
    except ValueError:
        task_result = None

    return JsonResponse(
        {"result_id": result.id, "result": task_result, "status": result.status}
    )
