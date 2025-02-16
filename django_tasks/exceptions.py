from django.core.exceptions import ImproperlyConfigured, ObjectDoesNotExist


class InvalidTaskError(Exception):
    """
    The provided task is invalid.
    """


class InvalidTaskBackendError(ImproperlyConfigured):
    pass


class ResultDoesNotExist(ObjectDoesNotExist):
    pass


class TimeoutException(BaseException):
    """
    Something timed out.
    """
