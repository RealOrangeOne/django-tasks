# Django Tasks

[![CI](https://github.com/RealOrangeOne/django-tasks/actions/workflows/ci.yml/badge.svg)](https://github.com/RealOrangeOne/django-tasks/actions/workflows/ci.yml)![PyPI](https://img.shields.io/pypi/v/django-tasks.svg)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/django-tasks.svg)
![PyPI - Status](https://img.shields.io/pypi/status/django-tasks.svg)
![PyPI - License](https://img.shields.io/pypi/l/django-tasks.svg)

A reference implementation and backport of background workers and tasks in Django, as defined in [DEP 0014](https://github.com/django/deps/pull/86).

**Warning**: This code is not intended to be installed, let alone deployed to production!

## Installation

```
pip install django-tasks
```

## Usage

**Note**: This documentation is still work-in-progress. Further details can also be found on the [DEP](https://github.com/django/deps/pull/86). [The tests](./tests/tests/) are also a good exhaustive reference.

### Settings

The first step is to configure a backend. This connects the tasks to whatever is going to execute them.

If omitted, the following configuration is used:

```python
TASKS = {
    "default": {
        "BACKEND": "django_tasks.backends.immediate.ImmediateBackend"
    }
}
```

A few backends are included by default:

- `DummyBackend`: Don't execute the tasks, just store them. This is especially useful for testing.
- `ImmediateBackend`: Execute the task immediately in the current thread

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

### Executing tasks

To execute a task, call the `enqueue` method on it:

```python
result = calculate_meaning_of_life.enqueue()
```

The returned `TaskResult` can be interrogated to query the current state of the running task, as well as its return value.

If the task takes arguments, these can be passed as-is to `enqueue`.

### Sending emails

To make sending emails simpler, a backend is provided to automatically create tasks for sending emails via SMTP:

```python
EMAIL_BACKEND = "django_tasks.mail.SMTPEmailBackend"
```

SMTP credentials are configured as usual.
