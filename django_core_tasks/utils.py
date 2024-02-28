from .exceptions import InvalidTask

TASK_FUNC_ATTR = "_is_task_func"


def task_function(func):
    try:
        setattr(func, TASK_FUNC_ATTR, True)
    except AttributeError as e:
        raise InvalidTask(func) from e

    return func


def is_marked_task_func(func):
    return getattr(func, TASK_FUNC_ATTR, None) is True
