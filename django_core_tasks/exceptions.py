from django.core.exceptions import ImproperlyConfigured, ObjectDoesNotExist


class InvalidTaskError(Exception):
    """
    The provided task function is invalid.
    """


class InvalidTaskBackendError(ImproperlyConfigured):
    pass


class ResultDoesNotExist(ObjectDoesNotExist):
    pass
