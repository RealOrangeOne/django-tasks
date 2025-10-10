import time
from typing import Any

from django_tasks import TaskContext, task


@task()
def noop_task(*args: Any, **kwargs: Any) -> None:
    return None


@task
def noop_task_from_bare_decorator(*args: Any, **kwargs: Any) -> None:
    return None


@task()
async def noop_task_async(*args: Any, **kwargs: Any) -> None:
    return None


@task()
def calculate_meaning_of_life() -> int:
    return 42


@task()
def failing_task_value_error() -> None:
    raise ValueError("This task failed due to ValueError")


@task()
def failing_task_system_exit() -> None:
    raise SystemExit("This task failed due to SystemExit")


@task()
def failing_task_keyboard_interrupt() -> None:
    raise KeyboardInterrupt("This task failed due to KeyboardInterrupt")


@task()
def complex_exception() -> None:
    raise ValueError(ValueError("This task failed"))


@task()
def complex_return_value() -> Any:
    # Return something which isn't JSON serializable nor picklable
    return lambda: True


@task()
def exit_task() -> None:
    exit(1)


@task()
def hang() -> None:
    """
    Do nothing for 5 minutes
    """
    time.sleep(300)


@task()
def sleep_for(seconds: float) -> None:
    time.sleep(seconds)


@task(takes_context=True)
def get_task_id(context: TaskContext) -> str:
    return context.task_result.id


@task(takes_context=True)
def test_context(context: TaskContext, attempt: int) -> None:
    assert isinstance(context, TaskContext)
    assert context.attempt == attempt
