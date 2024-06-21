import logging
import random
import signal
import time
from argparse import ArgumentParser, ArgumentTypeError
from types import FrameType
from typing import List, Optional

from django.core.management.base import BaseCommand
from django.db import transaction

from django_tasks import DEFAULT_TASK_BACKEND_ALIAS, tasks
from django_tasks.backends.database.models import DBTaskResult
from django_tasks.exceptions import InvalidTaskBackendError
from django_tasks.task import DEFAULT_QUEUE_NAME, ResultStatus

logger = logging.getLogger("django_tasks.backends.database.db_worker")


class Worker:
    def __init__(
        self, *, queue_names: List[str], interval: float, batch: bool, backend_name: str
    ):
        self.queue_names = queue_names
        self.process_all_queues = "*" in queue_names
        self.interval = interval
        self.batch = batch
        self.backend_name = backend_name

        self.running = True
        self.running_task = False

    def shutdown(self, signum: int, frame: Optional[FrameType]) -> None:
        logger.warning(
            "Received %s - shutting down gracefully.", signal.strsignal(signum)
        )

        self.running = False

        if not self.running_task:
            # If we're not currently running a task, exit immediately.
            # This is useful if we're currently in a `sleep`.
            exit(0)

    def configure_signals(self) -> None:
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        if hasattr(signal, "SIGQUIT"):
            signal.signal(signal.SIGQUIT, self.shutdown)

    def start(self) -> None:
        self.configure_signals()

        logger.info("Starting worker for queues=%s", ",".join(self.queue_names))

        if self.interval:
            # Add a random small delay before starting the loop to avoid a thundering herd
            time.sleep(random.random())

        while self.running:
            tasks = DBTaskResult.objects.ready().filter(backend_name=self.backend_name)
            if not self.process_all_queues:
                tasks = tasks.filter(queue_name__in=self.queue_names)

            try:
                self.running_task = True

                # During this transaction, all "ready" tasks are locked. Therefore, it's important
                # it be as efficient as possible.
                with transaction.atomic():
                    task_result = tasks.get_locked()

                    if task_result is not None:
                        # "claim" the task, so it isn't run by another worker process
                        task_result.claim()

                if task_result is not None:
                    self.run_task(task_result)

            finally:
                self.running_task = False

            if self.batch and task_result is None:
                # If we're running in "batch" mode, terminate the loop (and thus the worker)
                return None

            # Wait before checking for another task
            time.sleep(self.interval)

    def run_task(self, db_task_result: DBTaskResult) -> None:
        """
        Run the given task, marking it as complete or failed.
        """
        try:
            task = db_task_result.task
            task_result = db_task_result.task_result

            logger.info(
                "Task id=%s path=%s state=%s",
                db_task_result.id,
                db_task_result.task_path,
                ResultStatus.RUNNING,
            )
            return_value = task.call(*task_result.args, **task_result.kwargs)

            # Setting the return and success value inside the error handling,
            # So errors setting it (eg JSON encode) can still be recorded
            db_task_result.set_result(return_value)
            logger.info(
                "Task id=%s path=%s state=%s",
                db_task_result.id,
                db_task_result.task_path,
                ResultStatus.COMPLETE,
            )
        except BaseException as e:
            # Use `.exception` to integrate with error monitoring tools (eg Sentry)
            logger.exception(
                "Task id=%s path=%s state=%s",
                db_task_result.id,
                db_task_result.task_path,
                ResultStatus.FAILED,
            )
            db_task_result.set_failed()

            # If the user tried to terminate, let them
            if isinstance(e, KeyboardInterrupt):
                raise


def valid_backend_name(val: str) -> str:
    try:
        tasks[val]
    except InvalidTaskBackendError as e:
        raise ArgumentTypeError(e.args[0]) from e
    return val


def valid_interval(val: str) -> float:
    # Cast to an int first to catch invalid values like 'inf'
    int(val)

    num = float(val)
    if num < 0:
        raise ArgumentTypeError("Must be greater than zero")
    return num


class Command(BaseCommand):
    help = "Run a database background worker"

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--queue-name",
            nargs="?",
            default=DEFAULT_QUEUE_NAME,
            type=str,
            help="The queues to process. Separate multiple with a comma. To process all queues, use '*' (default: %(default)r)",
        )
        parser.add_argument(
            "--interval",
            nargs="?",
            default=1,
            type=valid_interval,
            help="The interval (in seconds) at which to check for tasks to process (default: %(default)r)",
        )
        parser.add_argument(
            "--batch",
            action="store_true",
            help="Process all outstanding tasks, then exit",
        )
        parser.add_argument(
            "--backend",
            nargs="?",
            default=DEFAULT_TASK_BACKEND_ALIAS,
            type=valid_backend_name,
            dest="backend_name",
            help="The backend to operate on (default: %(default)r)",
        )

    def configure_logging(self, verbosity: int) -> None:
        if verbosity == 0:
            logger.setLevel(logging.CRITICAL)
        elif verbosity == 1:
            logger.setLevel(logging.WARNING)
        elif verbosity == 2:
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
        queue_name: str,
        interval: float,
        batch: bool,
        backend_name: str,
        **options: dict,
    ) -> None:
        self.configure_logging(verbosity)

        worker = Worker(
            queue_names=queue_name.split(","),
            interval=interval,
            batch=batch,
            backend_name=backend_name,
        )

        worker.start()

        if batch:
            logger.info("No more tasks to run - exiting gracefully.")
