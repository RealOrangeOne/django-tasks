import logging
import math
import random
import signal
import sys
import time
from argparse import ArgumentParser, ArgumentTypeError
from types import FrameType
from typing import List, Optional

from django.core.exceptions import SuspiciousOperation
from django.core.management.base import BaseCommand
from django.db import connections
from django.db.utils import OperationalError

from django_tasks import DEFAULT_TASK_BACKEND_ALIAS, tasks
from django_tasks.backends.database.backend import DatabaseBackend
from django_tasks.backends.database.models import DBTaskResult
from django_tasks.backends.database.utils import exclusive_transaction
from django_tasks.exceptions import InvalidTaskBackendError
from django_tasks.signals import task_finished
from django_tasks.task import DEFAULT_QUEUE_NAME

package_logger = logging.getLogger("django_tasks")
logger = logging.getLogger("django_tasks.backends.database.db_worker")


class Worker:
    def __init__(
        self,
        *,
        queue_names: List[str],
        interval: float,
        batch: bool,
        backend_name: str,
        startup_delay: bool,
    ):
        self.queue_names = queue_names
        self.process_all_queues = "*" in queue_names
        self.interval = interval
        self.batch = batch
        self.backend_name = backend_name
        self.startup_delay = startup_delay

        self.running = True
        self.running_task = False

    def shutdown(self, signum: int, frame: Optional[FrameType]) -> None:
        if not self.running:
            logger.warning(
                "Received %s - terminating current task.", signal.strsignal(signum)
            )
            sys.exit(1)

        logger.warning(
            "Received %s - shutting down gracefully... (press Ctrl+C again to force)",
            signal.strsignal(signum),
        )
        self.running = False

        if not self.running_task:
            # If we're not currently running a task, exit immediately.
            # This is useful if we're currently in a `sleep`.
            sys.exit(0)

    def configure_signals(self) -> None:
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        if hasattr(signal, "SIGQUIT"):
            signal.signal(signal.SIGQUIT, self.shutdown)

    def start(self) -> None:
        self.configure_signals()

        logger.info("Starting worker for queues=%s", ",".join(self.queue_names))

        if self.startup_delay and self.interval:
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
                with exclusive_transaction(tasks.db):
                    try:
                        task_result = tasks.get_locked()
                    except OperationalError as e:
                        # Ignore locked databases and keep trying.
                        # It should unlock eventually.
                        if "is locked" in e.args[0]:
                            task_result = None
                        else:
                            raise

                    if task_result is not None:
                        # "claim" the task, so it isn't run by another worker process
                        task_result.claim()

                if task_result is not None:
                    self.run_task(task_result)

            finally:
                self.running_task = False

                for conn in connections.all(initialized_only=True):
                    conn.close()

            if self.batch and task_result is None:
                # If we're running in "batch" mode, terminate the loop (and thus the worker)
                return None

            # If ctrl-c has just interrupted a task, self.running was cleared,
            # and we should not sleep, but rather exit immediately.
            if self.running and not task_result:
                # Wait before checking for another task
                time.sleep(self.interval)

    def run_task(self, db_task_result: DBTaskResult) -> None:
        """
        Run the given task, marking it as succeeded or failed.
        """
        try:
            task = db_task_result.task
            task_result = db_task_result.task_result

            logger.info(
                "Task id=%s path=%s state=%s",
                db_task_result.id,
                db_task_result.task_path,
                task_result.status,
            )
            return_value = task.call(*task_result.args, **task_result.kwargs)

            # Setting the return and success value inside the error handling,
            # So errors setting it (eg JSON encode) can still be recorded
            db_task_result.set_succeeded(return_value)
            task_finished.send(
                sender=type(task.get_backend()), task_result=db_task_result.task_result
            )
        except BaseException as e:
            db_task_result.set_failed(e)
            try:
                sender = type(db_task_result.task.get_backend())
                task_result = db_task_result.task_result
            except (ModuleNotFoundError, SuspiciousOperation):
                logger.exception("Task id=%s failed unexpectedly", db_task_result.id)
            else:
                task_finished.send(
                    sender=sender,
                    task_result=task_result,
                )


def valid_backend_name(val: str) -> str:
    try:
        backend = tasks[val]
    except InvalidTaskBackendError as e:
        raise ArgumentTypeError(e.args[0]) from e
    if not isinstance(backend, DatabaseBackend):
        raise ArgumentTypeError(f"Backend '{val}' is not a database backend")
    return val


def valid_interval(val: str) -> float:
    num = float(val)
    if not math.isfinite(num):
        raise ArgumentTypeError("Must be a finite floating point value")
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
            help="The interval (in seconds) to wait, when there are no tasks in the queue, before checking for tasks again (default: %(default)r)",
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
        parser.add_argument(
            "--no-startup-delay",
            action="store_false",
            dest="startup_delay",
            help="Don't add a small delay at startup.",
        )

    def configure_logging(self, verbosity: int) -> None:
        if verbosity == 0:
            package_logger.setLevel(logging.CRITICAL)
        elif verbosity == 1:
            package_logger.setLevel(logging.WARNING)
        elif verbosity == 2:
            package_logger.setLevel(logging.INFO)
        else:
            package_logger.setLevel(logging.DEBUG)

        # If no handler is configured, the logs won't show,
        # regardless of the set level.
        if not package_logger.hasHandlers():
            package_logger.addHandler(logging.StreamHandler(self.stdout))

    def handle(
        self,
        *,
        verbosity: int,
        queue_name: str,
        interval: float,
        batch: bool,
        backend_name: str,
        startup_delay: bool,
        **options: dict,
    ) -> None:
        self.configure_logging(verbosity)

        worker = Worker(
            queue_names=queue_name.split(","),
            interval=interval,
            batch=batch,
            backend_name=backend_name,
            startup_delay=startup_delay,
        )

        worker.start()

        if batch:
            logger.info("No more tasks to run - exiting gracefully.")
