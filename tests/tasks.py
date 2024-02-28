"""
A collection of task functions, useful for testing
"""
from django_core_tasks import task_function


@task_function
def noop_task(*args, **kwargs):
    pass


@task_function
async def noop_task_async(*args, **kwargs):
    pass


@task_function
def erroring_task():
    1 / 0
