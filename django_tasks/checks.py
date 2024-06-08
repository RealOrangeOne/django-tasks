from typing import Any, List, Sequence

from django.apps.config import AppConfig
from django.core.checks.messages import CheckMessage

from django_tasks import tasks


def check_tasks(
    app_configs: Sequence[AppConfig] = None, **kwargs: Any
) -> List[CheckMessage]:
    """Checks all registered task backends."""

    errors = []
    for backend in tasks.all():
        try:
            backend_errors = backend.check()
        except NotImplementedError:
            pass
        else:
            errors.extend(backend_errors)

    return errors
