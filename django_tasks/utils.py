import inspect
from collections.abc import Callable, Mapping, Sequence
from traceback import format_exception
from typing import Any, TypeVar

from django.utils.crypto import get_random_string
from typing_extensions import ParamSpec

T = TypeVar("T")
P = ParamSpec("P")


def is_module_level_function(func: Callable) -> bool:
    if not inspect.isfunction(func) or inspect.isbuiltin(func):
        return False

    if "<locals>" in func.__qualname__:
        return False

    return True


def normalize_json(obj: Any) -> Any:
    """Recursively normalize an object into JSON-compatible types."""
    match obj:
        case Mapping():
            return {normalize_json(k): normalize_json(v) for k, v in obj.items()}
        case bytes():
            try:
                return obj.decode("utf-8")
            except UnicodeDecodeError as e:
                raise ValueError(f"Unsupported value: {type(obj)}") from e
        case str() | int() | float() | bool() | None:
            return obj
        case Sequence():  # str and bytes were already handled.
            return [normalize_json(v) for v in obj]
        case _:  # Other types can't be serialized to JSON
            raise TypeError(f"Unsupported type: {type(obj)}")


def get_module_path(val: Any) -> str:
    return f"{val.__module__}.{val.__qualname__}"


def get_exception_traceback(exc: BaseException) -> str:
    return "".join(format_exception(exc))


def get_random_id() -> str:
    """
    Return a random string for use as a Task or worker id.

    Whilst 64 characters is the max, just use 32 as a sensible middle-ground.
    """
    return get_random_string(32)
