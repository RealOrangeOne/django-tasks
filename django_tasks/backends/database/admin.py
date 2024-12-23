from typing import List, Optional

from django.contrib import admin
from django.db.models import QuerySet
from django.http import HttpRequest

from django_tasks.task import ResultStatus

from .models import DBTaskResult


def reenqueue(
    modeladmin: admin.ModelAdmin,
    request: HttpRequest,
    queryset: QuerySet[DBTaskResult],
) -> None:
    tasks = queryset.update(status=ResultStatus.NEW)
    modeladmin.message_user(request, f"Rescheduled {tasks} tasks.", "SUCCESS")


def duplicate(
    modeladmin: admin.ModelAdmin,
    request: HttpRequest,
    queryset: QuerySet[DBTaskResult],
) -> None:
    tasks = DBTaskResult.objects.bulk_create(
        old_task.duplicate() for old_task in queryset
    )
    modeladmin.message_user(request, f"Rescheduled {tasks} tasks.", "SUCCESS")


@admin.register(DBTaskResult)
class DBTaskResultAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "task_name",
        "status",
        "enqueued_at",
        "started_at",
        "finished_at",
        "priority",
        "queue_name",
    )
    list_filter = ("status", "priority", "queue_name")
    ordering = ["-enqueued_at"]
    actions = [reenqueue, duplicate]

    def has_add_permission(
        self, request: HttpRequest, obj: Optional[DBTaskResult] = None
    ) -> bool:
        return False

    def has_change_permission(
        self, request: HttpRequest, obj: Optional[DBTaskResult] = None
    ) -> bool:
        return False

    def get_readonly_fields(
        self, request: HttpRequest, obj: Optional[DBTaskResult] = None
    ) -> List[str]:
        return [f.name for f in self.model._meta.fields]
