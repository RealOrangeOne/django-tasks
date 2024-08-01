from typing import List, Optional

from django.contrib import admin
from django.http import HttpRequest

from django_tasks.task import ResultStatus

from .models import DBTaskResult


def reenqueue(modeladmin: admin.ModelAdmin, request, queryset):
    tasks = queryset.update(status=ResultStatus.NEW)
    modeladmin.message_user(request, f"Rescheduled {tasks} tasks.", "SUCCESS")


def duplicate(modeladmin: admin.ModelAdmin, request, queryset):
    tasks = DBTaskResult.objects.bulk_create(
        old_task.duplicate() for old_task in queryset
    )
    modeladmin.message_user(request, f"Rescheduled {tasks} tasks.", "SUCCESS")


@admin.register(DBTaskResult)
class DBTaskResultAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "get_task_name",
        "status",
        "enqueued_at",
        "started_at",
        "finished_at",
        "priority",
        "queue_name",
    )
    list_filter = ("status", "priority", "queue_name")
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

    @admin.display(description="Task")
    def get_task_name(self, obj: DBTaskResult) -> str:
        return obj.task.name
