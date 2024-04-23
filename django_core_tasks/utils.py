import inspect


def is_global_function(func) -> bool:
    if not inspect.isfunction(func) or inspect.isbuiltin(func):
        return False

    if "<locals>" in func.__qualname__:
        return False

    return True
