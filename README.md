# Django Tasks

[![CI](https://github.com/RealOrangeOne/django-tasks/actions/workflows/ci.yml/badge.svg)](https://github.com/RealOrangeOne/django-tasks/actions/workflows/ci.yml)
![PyPI](https://img.shields.io/pypi/v/django-tasks.svg)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/django-tasks.svg)
![PyPI - Status](https://img.shields.io/pypi/status/django-tasks.svg)
![PyPI - License](https://img.shields.io/pypi/l/django-tasks.svg)

An implementation and backport of background workers and tasks in Django, as defined in [DEP 0014](https://github.com/django/deps/blob/main/accepted/0014-background-workers.rst).

**Warning**: This package is under active development, and breaking changes may be released at any time. Be sure to pin to specific versions if you're using this package in a production environment.

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
- `django_tasks.backends.rq.RQBackend`: A backend which enqueues tasks using [RQ](https://python-rq.org/) via [`django-rq`](https://github.com/rq/django-rq) (requires installing `django-tasks[rq]`).

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

In addition to the above attributes, `run_after` can be passed to specify a specific time the task should run.

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

In `DEBUG`, the worker will automatically reload when code is changed (or by using `--reload`). This is not recommended in production environments as tasks may not be stopped cleanly.

### Pruning old tasks

After a while, tasks may start to build up in your database. This can be managed using the `prune_db_task_results` management command, which deletes completed tasks according to the given retention policy. Check the `--help` for the available options.

### Retrieving task result

When enqueueing a task, you get a `TaskResult`, however it may be useful to retrieve said result from somewhere else (another request, another task etc). This can be done with `get_result` (or `aget_result`):

```python
result_id = result.id

# Later, somewhere else...
calculate_meaning_of_life.get_result(result_id)
```

A result `id` should be considered an opaque string, whose length could be up to 64 characters. ID generation is backend-specific.

Only tasks of the same type can be retrieved this way. To retrieve the result of any task, you can call `get_result` on the backend:

```python
from django_tasks import default_task_backend

default_task_backend.get_result(result_id)
```

### Return values

If your task returns something, it can be retrieved from the `.return_value` attribute on a `TaskResult`. Accessing this property on an unsuccessful task (ie not `SUCCEEDED`) will raise a `ValueError`.

```python
assert result.status == ResultStatus.SUCCEEDED
assert result.return_value == 42
```

If a result has been updated in the background, you can call `refresh` on it to update its values. Results obtained using `get_result` will always be up-to-date.

```python
assert result.status == ResultStatus.NEW
result.refresh()
assert result.status == ResultStatus.SUCCEEDED
```

#### Exceptions

If a task raised an exception, its `.exception_class` will be the exception class raised:

```python
assert result.exception_class == ValueError
```

Note that this is just the type of exception, and contains no other values. The traceback information is reduced to a string that you can print to help debugging:

```python
assert isinstance(result.traceback, str)
```

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

### Signals

A few [Signals](https://docs.djangoproject.com/en/stable/topics/signals/) are provided to more easily respond to certain task events.

Whilst signals are available, they may not be the most maintainable approach.

- `django_tasks.signals.task_enqueued`: Called when a task is enqueued. The sender is the backend class. Also called with the enqueued `task_result`.
- `django_tasks.signals.task_finished`: Called when a task finishes (`SUCCEEDED` or `FAILED`). The sender is the backend class. Also called with the finished `task_result`.

## RQ

The RQ-based backend acts as an interface between `django_tasks` and `RQ`, allowing tasks to be defined and enqueued using `django_tasks`, but stored in Redis and executed using RQ's workers.

Once RQ is configured as necessary, the relevant `django_tasks` configuration can be added:

```python
TASKS = {
    "default": {
        "BACKEND": "django_tasks.backends.rq.RQBackend",
        "QUEUES": ["default"]
    }
}
```

Any queues defined in `QUEUES` must also be defined in `django-rq`'s `RQ_QUEUES` setting.

### Job class

To use `rq` with `django-tasks`, a custom `Job` class must be used. This can be passed to the worker using `--job-class`:

```shell
./manage.py rqworker --job-class django_tasks.backend.rq.Job
```

### Priorities

`rq` has no native concept of priorities - instead relying on workers to define which queues they should pop tasks from in order. Therefore, `task.priority` has little effect on execution priority.

If a task has a priority of `100`, it is enqueued at the top of the queue, and will be the next task executed by a worker. All other priorities will enqueue the task to the back of the queue. The queue value is not stored, and will always be `0`.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for information on how to contribute.
