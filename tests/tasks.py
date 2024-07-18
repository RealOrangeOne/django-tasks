from django_tasks import task


@task()
def noop_task(*args: tuple, **kwargs: dict) -> None:
    return None


@task
def noop_task_from_bare_decorator(*args: tuple, **kwargs: dict) -> None:
    return None


@task()
async def noop_task_async(*args: tuple, **kwargs: dict) -> None:
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
def exit_task() -> None:
    exit(1)
