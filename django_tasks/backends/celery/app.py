import os

from celery import Celery

from django_tasks.task import DEFAULT_QUEUE_NAME


# Set the default Django settings module for the 'celery' program.
django_settings = os.environ.get('DJANGO_SETTINGS_MODULE')
if django_settings is None:
    raise ValueError('DJANGO_SETTINGS_MODULE environment variable is not set')

app = Celery('django_tasks')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')

app.conf.task_default_queue = DEFAULT_QUEUE_NAME
