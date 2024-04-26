from django_core_tasks import task


@task()
def noop_task(*args: tuple, **kwargs: dict) -> None:
    pass


@task()
async def noop_task_async(*args: tuple, **kwargs: dict) -> None:
    pass


@task()
def calculate_meaning_of_life() -> int:
    return 42


@task()
def failing_task() -> None:
    raise ValueError("This task failed")
