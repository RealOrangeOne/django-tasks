import inspect
import json
import time
from collections import deque
from functools import wraps
from traceback import format_exception
from typing import Any, Callable, List, TypedDict, TypeVar

from django.utils.module_loading import import_string
from typing_extensions import ParamSpec


class SerializedExceptionDict(TypedDict):
    """Type for the dictionary holding exception informations in task result

    The task result either stores the result of the task, or the serialized exception
    information required to reconstitute part of the exception for debugging.
    """

    exc_type: str
    exc_args: List[Any]
    exc_traceback: str


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


def get_module_path(val: Any) -> str:
    return f"{val.__module__}.{val.__qualname__}"


def exception_to_dict(exc: BaseException) -> SerializedExceptionDict:
    return {
        "exc_type": get_module_path(type(exc)),
        "exc_args": json_normalize(exc.args),
        "exc_traceback": "".join(format_exception(type(exc), exc, exc.__traceback__)),
    }


def exception_from_dict(exc_data: SerializedExceptionDict) -> BaseException:
    exc_class = import_string(exc_data["exc_type"])

    if not inspect.isclass(exc_class) or not issubclass(exc_class, BaseException):
        raise TypeError(f"{type(exc_class)} is not an exception")

    return exc_class(*exc_data["exc_args"])
