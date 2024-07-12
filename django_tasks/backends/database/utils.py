from contextlib import contextmanager
from typing import Any, Generator, Union
from uuid import UUID

from django.db import DEFAULT_DB_ALIAS, connections, transaction


@contextmanager
def exclusive_transaction(using: str = DEFAULT_DB_ALIAS) -> Generator[Any, Any, Any]:
    """
    Wrapper around `transaction.atomic` which ensures transactions on SQLite are exclusive.

    This functionality is built-in to Django 5.1+.
    """
    if connections[using].vendor == "sqlite":
        with connections[using].cursor() as c:
            c.execute("BEGIN EXCLUSIVE")
            try:
                yield
            finally:
                c.execute("COMMIT")
    else:
        with transaction.atomic(using=using):
            yield


def normalize_uuid(val: Union[str, UUID]) -> str:
    """
    Normalize a UUID into its dashed representation.

    This works around engines like MySQL which don't store values in a uuid field,
    and thus drops the dashes.
    """
    if isinstance(val, str):
        val = UUID(val)

    return str(val)
