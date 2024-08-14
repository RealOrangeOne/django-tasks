from typing import Any, Iterable, Sequence

from django.apps.config import AppConfig
from django.core.checks.messages import CheckMessage

from django_tasks import tasks


def check_tasks(
    app_configs: Sequence[AppConfig] = None, **kwargs: Any
) -> Iterable[CheckMessage]:
    """Checks all registered task backends."""

    for backend in tasks.all():
        try:
            yield from backend.check()
        except NotImplementedError:
            pass
