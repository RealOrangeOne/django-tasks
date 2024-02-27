class InvalidTask(Exception):
    """
    The provided task function is invalid.
    """

    def __init__(self, func):
        self.func = func
