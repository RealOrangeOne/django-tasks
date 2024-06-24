import logging
from argparse import ArgumentParser, ArgumentTypeError
from datetime import timedelta
from typing import Optional

from django.core.management.base import BaseCommand
from django.db.models import Q
from django.utils import timezone

from django_tasks import DEFAULT_QUEUE_NAME, DEFAULT_TASK_BACKEND_ALIAS, tasks
from django_tasks.backends.database.backend import DatabaseBackend
from django_tasks.backends.database.models import DBTaskResult
from django_tasks.exceptions import InvalidTaskBackendError
from django_tasks.task import ResultStatus

logger = logging.getLogger("django_tasks.backends.database.db_worker")


def valid_backend_name(val: str) -> DatabaseBackend:
    try:
        backend = tasks[val]
    except InvalidTaskBackendError as e:
        raise ArgumentTypeError(e.args[0]) from e
    if not isinstance(backend, DatabaseBackend):
        raise ArgumentTypeError(f"Backend '{val}' is not a database backend")
    return backend


def valid_positive_int(val: str) -> int:
    num = int(val)
    if num < 0:
        raise ArgumentTypeError("Must be greater than zero")
    return num


class Command(BaseCommand):
    help = "Run a database background worker"

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--backend",
            nargs="?",
            default=DEFAULT_TASK_BACKEND_ALIAS,
            type=valid_backend_name,
            dest="backend",
            help="The backend to operate on (default: %(default)r)",
        )
        parser.add_argument(
            "--queue-name",
            nargs="?",
            default=DEFAULT_QUEUE_NAME,
            type=str,
            help="The queues to process. Separate multiple with a comma. To process all queues, use '*' (default: %(default)r)",
        )
        parser.add_argument(
            "--min-age-days",
            nargs="?",
            default=14,
            type=valid_positive_int,
            help="The minimum age of a task to be culled (default: %(default)r)",
        )
        parser.add_argument(
            "--failed-min-age-days",
            nargs="?",
            default=None,
            type=valid_positive_int,
            help="The minimum age of a failed task to be culled (default: min-age-days)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Don't delete the tasks, just show how many would be deleted",
        )

    def configure_logging(self, verbosity: int) -> None:
        if verbosity == 0:
            logger.setLevel(logging.WARNING)
        elif verbosity == 1:
            logger.setLevel(logging.INFO)
        else:
            logger.setLevel(logging.DEBUG)

        # If no handler is configured, the logs won't show,
        # regardless of the set level.
        if not logger.hasHandlers():
            logger.addHandler(logging.StreamHandler(self.stdout))

    def handle(
        self,
        *,
        verbosity: int,
        backend: DatabaseBackend,
        min_age_days: int,
        failed_min_age_days: Optional[int],
        queue_name: str,
        dry_run: bool,
        **options: dict,
    ) -> None:
        self.configure_logging(verbosity)

        min_age = timezone.now() - timedelta(days=min_age_days)
        failed_min_age = (
            (timezone.now() - timedelta(days=failed_min_age_days))
            if failed_min_age_days
            else None
        )

        tasks = DBTaskResult.objects.finished().filter(backend_name=backend.alias)

        queue_names = queue_name.split(",")
        if "*" not in queue_names:
            tasks = tasks.filter(queue_name__in=queue_names)

        if failed_min_age is None:
            tasks = tasks.filter(finished_at__lte=min_age)
        else:
            tasks = tasks.filter(
                Q(status=ResultStatus.COMPLETE, finished_at__lte=min_age)
                | Q(status=ResultStatus.FAILED, finished_at__lte=failed_min_age)
            )

        if dry_run:
            logger.info("Would delete %d task(s)", tasks.count())
        else:
            deleted, _ = tasks.delete()
            logger.info("Deleted %d task(s)", deleted)
