from django.core.exceptions import ImproperlyConfigured, ObjectDoesNotExist


class InvalidTask(Exception):
    """
    The provided task function is invalid.
    """

    def __init__(self, func):
        self.func = func


class InvalidTaskBackendError(ImproperlyConfigured):
    pass


class TaskDoesNotExist(ObjectDoesNotExist):
    pass
