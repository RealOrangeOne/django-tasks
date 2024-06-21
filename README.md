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

## Usage

**Note**: This documentation is still work-in-progress. Further details can also be found on the [DEP](https://github.com/django/deps/blob/main/accepted/0014-background-workers.rst). [The tests](./tests/tests/) are also a good exhaustive reference.

### Settings

The first step is to add `django_tasks` to your `INSTALLED_APPS`.

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

### Defining tasks

A task is created with the `task` decorator.

```python
from django_tasks import task


@task()
def calculate_meaning_of_life() -> int:
    return 42
```

The task decorator accepts a few arguments to customize the task:

- `priority`: The priority of the task (larger numbers are higher priority)
- `queue_name`: Whether to run the task on a specific queue
- `backend`: Name of the backend for this task to use (as defined in `TASKS`)

These attributes can also be modified at run-time with `.using`:

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

### Executing tasks with the database backend

First, you'll need to add `django_tasks.backends.database`  to `INSTALLED_APPS`, and run `manage.py migrate`.

Next, configure the database backend:

```python
TASKS = {
    "default": {
        "BACKEND": "django_tasks.backends.database.DatabaseBackend"
    }
}
```

Finally, you can run `manage.py db_worker` to run tasks as they're created. Check the `--help` for more options.

> [!CAUTION]
> The database backend does not work with SQLite when you are running multiple worker processes - tasks may be executed more than once. See [#33](https://github.com/RealOrangeOne/django-tasks/issues/33).

### Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for information on how to contribute.
