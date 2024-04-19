from django.db.models.enums import StrEnum


class TaskStatus(StrEnum):
    NEW = "NEW"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    COMPLETE = "COMPLETE"
