# Django Tasks

[![CI](https://github.com/RealOrangeOne/django-tasks/actions/workflows/ci.yml/badge.svg)](https://github.com/RealOrangeOne/django-tasks/actions/workflows/ci.yml)
![PyPI](https://img.shields.io/pypi/v/django-tasks.svg)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/django-tasks.svg)
![PyPI - Status](https://img.shields.io/pypi/status/django-tasks.svg)
![PyPI - License](https://img.shields.io/pypi/l/django-tasks.svg)

A reference implementation and backport of background workers and tasks in Django, as defined in [DEP 0014](https://github.com/django/deps/blob/main/accepted/0014-background-workers.rst).

**Warning**: This package is under active development, and breaking changes may be released at any time. Be sure to pin to specific versions (even patch versions) if you're using this package in a production environment.

## Installation

```
python -m pip install django-tasks
```

The first step is to add `django_tasks` to your `INSTALLED_APPS`.

```python
INSTALLED_APPS = [
    # ...
    "django_tasks",
]
```

Secondly, you'll need to configure a backend. This connects the tasks to whatever is going to execute them.

If omitted, the following configuration is used:

```python
TASKS = {
    "default": {
        "BACKEND": "django_tasks.backends.immediate.ImmediateBackend"
    }
}
```

A few backends are included by default:

- `django_tasks.backends.dummy.DummyBackend`: Don't execute the tasks, just store them. This is especially useful for testing.
- `django_tasks.backends.immediate.ImmediateBackend`: Execute the task immediately in the current thread
- `django_tasks.backends.database.DatabaseBackend`: Store tasks in the database (via Django's ORM), and retrieve and execute them using the `db_worker` management command

Note: `DatabaseBackend` additionally requires `django_tasks.backends.database` adding to `INSTALLED_APPS`.

## Usage

**Note**: This documentation is still work-in-progress. Further details can also be found on the [DEP](https://github.com/django/deps/blob/main/accepted/0014-background-workers.rst). [The tests](./tests/tests/) are also a good exhaustive reference.

### Defining tasks

A task is created with the `task` decorator.

```python
from django_tasks import task


@task()
def calculate_meaning_of_life() -> int:
    return 42
```

The task decorator accepts a few arguments to customize the task:

- `priority`: The priority of the task (between -100 and 100. Larger numbers are higher priority. 0 by default)
- `queue_name`: Whether to run the task on a specific queue
- `backend`: Name of the backend for this task to use (as defined in `TASKS`)
- `enqueue_on_commit`: Whether the task is enqueued when the current transaction commits successfully, or enqueued immediately. By default, this is handled by the backend (see below). `enqueue_on_commit` may not be modified with `.using`.

These attributes (besides `enqueue_on_commit`) can also be modified at run-time with `.using`:

```python
modified_task = calculate_meaning_of_life.using(priority=10)
```

In addition to the above attributes, `run_after` can be passed to specify a specific time the task should run. Both a timezone-aware `datetime` or `timedelta` may be passed.

### Enqueueing tasks

To execute a task, call the `enqueue` method on it:

```python
result = calculate_meaning_of_life.enqueue()
```

The returned `TaskResult` can be interrogated to query the current state of the running task, as well as its return value.

If the task takes arguments, these can be passed as-is to `enqueue`.

#### Transactions

By default, tasks are enqueued after the current transaction (if there is one) commits successfully (using Django's `transaction.on_commit` method), rather than enqueueing immediately.

This can be configured using the `ENQUEUE_ON_COMMIT` setting. `True` and `False` force the behaviour.

```python
TASKS = {
    "default": {
        "BACKEND": "django_tasks.backends.immediate.ImmediateBackend",
        "ENQUEUE_ON_COMMIT": False
    }
}
```

This can also be configured per-task by passing `enqueue_on_commit` to the `task` decorator.

### Queue names

By default, tasks are enqueued onto the "default" queue. When using multiple queues, it can be useful to constrain the allowed names, so tasks aren't missed.

```python
TASKS = {
    "default": {
        "BACKEND": "django_tasks.backends.immediate.ImmediateBackend",
        "QUEUES": ["default", "special"]
    }
}
```

Enqueueing tasks to an unknown queue name raises `InvalidTaskError`.

To disable queue name validation, set `QUEUES` to `[]`.

### The database backend worker

First, you'll need to add `django_tasks.backends.database`  to `INSTALLED_APPS`:

```python
INSTALLED_APPS = [
    # ...
    "django_tasks",
    "django_tasks.backends.database",
]
```

Then, run migrations:

```shell
./manage.py migrate
```

Next, configure the database backend:

```python
TASKS = {
    "default": {
        "BACKEND": "django_tasks.backends.database.DatabaseBackend"
    }
}
```

Finally, you can run the `db_worker` command to run tasks as they're created. Check the `--help` for more options.

```shell
./manage.py db_worker
```

### Retrieving task result

When enqueueing a task, you get a `TaskResult`, however it may be useful to retrieve said result from somewhere else (another request, another task etc). This can be done with `get_result` (or `aget_result`):

```python
result_id = result.id

# Later, somewhere else...
calculate_meaning_of_life.get_result(result_id)
```

Only tasks of the same type can be retrieved this way. To retrieve the result of any task, you can call `get_result` on the backend:

```python
from django_tasks import default_task_backend

default_task_backend.get_result(result_id)
```

### Return values

If your task returns something, it can be retrieved from the `.result` attribute on a `TaskResult`. Accessing this property on an unfinished task (ie not `COMPLETE` or `FAILED`) will raise a `ValueError`.

```python
assert result.status == ResultStatus.COMPLETE
assert result.result == 42
```

If a result has been updated in the background, you can call `refresh` on it to update its values. Results obtained using `get_result` will always be up-to-date.

```python
assert result.status == ResultStatus.NEW
result.refresh()
assert result.status == ResultStatus.COMPLETE
```

#### Exceptions

If a task raised an exception, its `.result` will be the exception raised:

```python
assert isinstance(result.result, ValueError)
```

As part of the serialization process for exceptions, some information is lost. The traceback information is reduced to a string that you can print to help debugging:

```python
assert isinstance(result.traceback, str)
```

The stack frames, `globals()` and `locals()` are not available.

If the exception could not be serialized, the `.result` is `None`.

### Backend introspecting

Because `django-tasks` enables support for multiple different backends, those backends may not support all features, and it can be useful to determine this at runtime to ensure the chosen task queue meets the requirements, or to gracefully degrade functionality if it doesn't.

- `supports_defer`: Can tasks be enqueued with the `run_after` attribute?
- `supports_async_task`: Can coroutines be enqueued?
- `supports_get_result`: Can results be retrieved after the fact (from **any** thread / process)?

```python
from django_tasks import default_task_backend

assert default_task_backend.supports_get_result
```

This is particularly useful in combination with Django's [system check framework](https://docs.djangoproject.com/en/stable/topics/checks/).

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for information on how to contribute.
