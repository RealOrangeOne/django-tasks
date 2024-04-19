from django_core_tasks import task


@task()
def noop_task(*args, **kwargs):
    pass
