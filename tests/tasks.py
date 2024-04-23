from django_core_tasks import task


@task()
def noop_task(*args, **kwargs):
    pass


@task()
async def noop_task_async(*args, **kwargs):
    pass


@task()
def calculate_meaning_of_life():
    return 42


@task()
def failing_task():
    raise ValueError("This task failed")
