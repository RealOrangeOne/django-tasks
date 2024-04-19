from django.core.exceptions import ImproperlyConfigured, ObjectDoesNotExist


class InvalidTaskError(Exception):
    """
    The provided task function is invalid.
    """

    def __init__(self, task):
        self.task = task


class InvalidTaskBackendError(ImproperlyConfigured):
    pass


class TaskDoesNotExist(ObjectDoesNotExist):
    pass
