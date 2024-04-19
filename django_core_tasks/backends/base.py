from django_core_tasks.task import Task, TaskResult


class BaseTaskBackend:
    task_class = Task

    supports_defer = False

    def __init__(self, options):
        self.alias = options["ALIAS"]

    @classmethod
    def is_valid_task(cls, task: Task) -> bool:
        """
        Determine whether the provided task is one which can be executed by the backend.
        """
        return True

    def enqueue(self, task: Task, args, kwargs) -> TaskResult:
        """
        Queue up a task to be executed
        """
        ...

    def get_result(self, result_id: str) -> TaskResult:
        """
        Retrieve a result by its id (if one exists).
        If one doesn't, raises ResultDoesNotExist.
        """
        ...

    def close(self) -> None:
        """
        Close any connections opened as part of the constructor
        """
        ...
