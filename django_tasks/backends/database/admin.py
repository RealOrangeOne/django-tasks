from django.contrib import admin

from .models import DBTaskResult


@admin.register(DBTaskResult)
class DBTaskResultAdmin(admin.ModelAdmin):
    list_display = ("id", "task_path", "status", "priority", "queue_name")
    list_filter = ("status", "priority", "queue_name")

    def has_add_permission(self, request, obj=None):
        return False

    def get_readonly_fields(self, request, obj=None):
        return [f.name for f in self.model._meta.fields]
