from .exceptions import InvalidTaskError

TASK_FUNC_ATTR = "_is_task_func"


def task_function(func):
    try:
        setattr(func, TASK_FUNC_ATTR, True)
    except AttributeError:
        raise InvalidTaskError(func) from None

    return func


def is_marked_task_func(func):
    return getattr(func, TASK_FUNC_ATTR, None) is True
