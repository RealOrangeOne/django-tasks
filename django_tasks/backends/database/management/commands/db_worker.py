import logging
import math
import random
import signal
import sys
import threading
import time
from argparse import ArgumentParser, ArgumentTypeError
from types import FrameType
from typing import Optional

from django.core.exceptions import SuspiciousOperation
from django.core.management.base import BaseCommand
from django.db import connections
from django.db.utils import OperationalError

from django_tasks import DEFAULT_TASK_BACKEND_ALIAS, tasks
from django_tasks.backends.database.backend import DatabaseBackend
from django_tasks.backends.database.models import DBTaskResult, DBWorkerPing
from django_tasks.backends.database.utils import exclusive_transaction
from django_tasks.exceptions import InvalidTaskBackendError
from django_tasks.signals import task_finished
from django_tasks.task import DEFAULT_QUEUE_NAME
from django_tasks.utils import get_random_id

package_logger = logging.getLogger("django_tasks")
logger = logging.getLogger("django_tasks.backends.database.db_worker")

PING_TIMEOUT = 10


class Worker:
    def __init__(
        self,
        *,
        queue_names: list[str],
        interval: float,
        batch: bool,
        backend_name: str,
        startup_delay: bool,
        worker_id: str,
    ):
        self.queue_names = queue_names
        self.process_all_queues = "*" in queue_names
        self.interval = interval
        self.batch = batch
        self.backend_name = backend_name
        self.startup_delay = startup_delay

        self.worker_id = worker_id

        self.ping_thread = threading.Thread(target=self.run_ping)

        self.should_exit = threading.Event()
        self.running_task = threading.Lock()

    def run_ping(self) -> None:
        try:
            while True:
                try:
                    DBWorkerPing.ping(self.worker_id)
                    logger.debug("Sent ping worker_id=%s", self.worker_id)
                except Exception:
                    logger.exception(
                        "Error updating worker ping worker_id=%s", self.worker_id
                    )

                if self.should_exit.wait(timeout=PING_TIMEOUT):
                    break
        finally:
            # Close any connections opened in this thread
            for conn in connections.all():
                conn.close()

    def shutdown(self, signum: int, frame: Optional[FrameType]) -> None:
        if self.should_exit.is_set():
            logger.warning(
                "Received %s - terminating current task.", signal.strsignal(signum)
            )
            sys.exit(1)

        logger.warning(
            "Received %s - shutting down gracefully... (press Ctrl+C again to force)",
            signal.strsignal(signum),
        )
        self.should_exit.set()

        if not self.running_task.locked():
            # If we're not currently running a task, exit immediately.
            # This is useful if we're currently in a `sleep`.
            sys.exit(0)

    def configure_signals(self) -> None:
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        if hasattr(signal, "SIGQUIT"):
            signal.signal(signal.SIGQUIT, self.shutdown)

    def run(self) -> None:
        self.configure_signals()

        logger.info(
            "Starting worker worker_id=%s for queues=%s",
            self.worker_id,
            ",".join(self.queue_names),
        )

        if self.startup_delay and self.interval:
            # Add a random small delay before starting to avoid a thundering herd
            time.sleep(random.random())

        self.ping_thread.start()

        while not self.should_exit.is_set():
            tasks = DBTaskResult.objects.ready().filter(backend_name=self.backend_name)
            if not self.process_all_queues:
                tasks = tasks.filter(queue_name__in=self.queue_names)

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
                    task_result.claim(self.worker_id)

            if task_result is not None:
                self.run_task(task_result)

            if self.batch and task_result is None:
                # If we're running in "batch" mode, terminate the loop (and thus the worker)
                return None

            # If ctrl-c has just interrupted a task, self.should_exit was cleared,
            # and we should not sleep, but rather exit immediately.
            if not self.should_exit.is_set() and not task_result:
                # Wait before checking for another task
                time.sleep(self.interval)

    def run_task(self, db_task_result: DBTaskResult) -> None:
        """
        Run the given task, marking it as succeeded or failed.
        """
        with self.running_task:
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
                    sender=type(task.get_backend()),
                    task_result=db_task_result.task_result,
                )
            except BaseException as e:
                db_task_result.set_failed(e)
                try:
                    sender = type(db_task_result.task.get_backend())
                    task_result = db_task_result.task_result
                except (ModuleNotFoundError, SuspiciousOperation):
                    logger.exception(
                        "Task id=%s failed unexpectedly",
                        db_task_result.id,
                    )
                else:
                    task_finished.send(
                        sender=sender,
                        task_result=task_result,
                    )

        for conn in connections.all(initialized_only=True):
            conn.close()

    def stop(self) -> None:
        self.should_exit.set()
        self.ping_thread.join()
        DBWorkerPing.cleanup_ping(self.worker_id)


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


def validate_worker_id(val: str) -> str:
    if not val:
        raise ArgumentTypeError("Worker id must not be empty")
    if len(val) > 64:
        raise ArgumentTypeError("Worker ids must be shorter than 64 characters")
    return val


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
        parser.add_argument(
            "--worker-id",
            nargs="?",
            type=validate_worker_id,
            help="Worker id. MUST be unique across worker pool (default: auto-generate)",
            default=get_random_id(),
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
        worker_id: str,
        **options: dict,
    ) -> None:
        self.configure_logging(verbosity)

        worker = Worker(
            queue_names=queue_name.split(","),
            interval=interval,
            batch=batch,
            backend_name=backend_name,
            startup_delay=startup_delay,
            worker_id=worker_id,
        )

        try:
            worker.run()
        finally:
            worker.stop()

        if batch:
            logger.info(
                "No more tasks to run for worker_id=%s - exiting gracefully.", worker_id
            )
