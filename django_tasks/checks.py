from django_tasks import tasks


def check_tasks(app_configs=None, **kwargs):
    """Checks all registered task backends."""

    errors = []
    for backend in tasks.all():
        try:
            backend_errors = backend.check()
        except NotImplementedError:
            pass
        else:
            errors.extend(backend_errors)

    return errors
