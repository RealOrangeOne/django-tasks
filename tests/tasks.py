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
def failing_task() -> None:
    raise ValueError("This task failed")


@task()
def complex_exception() -> None:
    raise ValueError(ValueError("This task failed"))


@task()
def exit_task() -> None:
    exit(1)
