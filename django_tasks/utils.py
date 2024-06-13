import inspect
import json
import time
from collections import deque
from functools import wraps
from typing import Any, Callable, TypeVar

from typing_extensions import ParamSpec

T = TypeVar("T")
P = ParamSpec("P")


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


def json_normalize(obj: Any) -> Any:
    """
    Round-trip encode object as JSON to normalize types.
    """
    return json.loads(json.dumps(obj))


def retry(*, retries: int = 3, backoff_delay: float = 0.1) -> Callable:
    """
    Retry the given code `retries` times, raising the final error.

    `backoff_delay` can be used to add a delay between attempts.
    """

    def wrapper(f: Callable[P, T]) -> Callable[P, T]:
        @wraps(f)
        def inner_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:  # type:ignore[return]
            for attempt in range(1, retries + 1):
                try:
                    return f(*args, **kwargs)
                except KeyboardInterrupt:
                    # Let the user ctrl-C out of the program without a retry
                    raise
                except BaseException:
                    if attempt == retries:
                        raise
                    time.sleep(backoff_delay)

        return inner_wrapper

    return wrapper
