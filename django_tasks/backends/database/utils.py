from contextlib import contextmanager
from typing import Any, Generator, Optional, Union
from uuid import UUID

import django
from django.db import transaction
from django.db.backends.base.base import BaseDatabaseWrapper


def connection_requires_manual_exclusive_transaction(
    connection: BaseDatabaseWrapper,
) -> bool:
    """
    Determine whether the backend requires manual transaction handling.

    Extracted from `exclusive_transaction` for unit testing purposes.
    """
    if connection.vendor != "sqlite":
        return False

    if django.VERSION < (5, 1):
        return True

    return connection.transaction_mode != "EXCLUSIVE"  # type:ignore[attr-defined,no-any-return]


@contextmanager
def exclusive_transaction(using: Optional[str] = None) -> Generator[Any, Any, Any]:
    """
    Wrapper around `transaction.atomic` which ensures transactions on SQLite are exclusive.

    This functionality is built-in to Django 5.1+.
    """
    connection: BaseDatabaseWrapper = transaction.get_connection(using)

    if connection_requires_manual_exclusive_transaction(connection):
        if django.VERSION >= (5, 1):
            raise RuntimeError("Transactions must be EXCLUSIVE")

        with connection.cursor() as c:
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
