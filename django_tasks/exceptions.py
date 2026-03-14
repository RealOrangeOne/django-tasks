# ruff: noqa: E402
from typing import TYPE_CHECKING

from django.core.exceptions import ImproperlyConfigured

_USE_DJANGO_TASKS = False
if not TYPE_CHECKING:
    try:
        from django.tasks.exceptions import (  # noqa: F401
            TaskException,
            TaskResultDoesNotExist,
            TaskResultMismatch,
        )

        _USE_DJANGO_TASKS = True
    except ImportError:
        pass

if not _USE_DJANGO_TASKS:

    class TaskException(Exception):  # noqa: N818
        """Base class for task-related exceptions. Do not raise directly."""

    class TaskResultDoesNotExist(TaskException):
        pass

    class TaskResultMismatch(TaskException):
        pass


class InvalidTaskError(TaskException):
    """
    The provided Task is invalid.
    """


class InvalidTaskBackendError(ImproperlyConfigured):
    pass
