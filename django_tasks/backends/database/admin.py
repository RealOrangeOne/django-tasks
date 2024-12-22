from typing import List, Optional

from django.contrib import admin
from django.http import HttpRequest

from .models import DBTaskResult


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

    def has_add_permission(
        self, request: HttpRequest, obj: Optional[DBTaskResult] = None
    ) -> bool:
        return False

    def has_delete_permission(
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
