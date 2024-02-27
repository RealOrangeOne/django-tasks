"""
A collection of task functions, useful for testing
"""


def noop_task(*args, **kwargs):
    pass


async def noop_task_async(*args, **kwargs):
    pass


def erroring_task():
    1 / 0
