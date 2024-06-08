from typing import List, Optional

from django.contrib import admin
from django.http import HttpRequest

from .models import DBTaskResult


@admin.register(DBTaskResult)
class DBTaskResultAdmin(admin.ModelAdmin):
    list_display = ("id", "task_path", "status", "priority", "queue_name")
    list_filter = ("status", "priority", "queue_name")

    def has_add_permission(
        self, request: HttpRequest, obj: Optional[DBTaskResult] = None
    ) -> bool:
        return False

    def get_readonly_fields(
        self, request: HttpRequest, obj: Optional[DBTaskResult] = None
    ) -> List[str]:
        return [f.name for f in self.model._meta.fields]
