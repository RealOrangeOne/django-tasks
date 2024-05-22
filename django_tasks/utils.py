import inspect
import json
from collections import deque
from typing import Any, Callable


def is_global_function(func: Callable) -> bool:
    if not inspect.isfunction(func) or inspect.isbuiltin(func):
        return False

    if "<locals>" in func.__qualname__:
        return False

    return True


def is_json_serializable(obj: Any) -> bool:
    """
    Determine, as efficiently as possible, whether an object is JSON-serializable.
    """
    try:
        # HACK: JSON-encode an object, without loading it all into memory
        deque(json.JSONEncoder().iterencode(obj), maxlen=0)
        return True
    except (TypeError, OverflowError):
        return False
