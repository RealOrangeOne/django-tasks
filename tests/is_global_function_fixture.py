"""
This file is used to test function is considered global even if it's not defined yet
because it's covered by a decorator.
"""

from django_core_tasks.utils import is_global_function


@is_global_function
def really_global_function():
    pass


inner_func_is_global_function = None


def main():
    global inner_func_is_global_function

    @is_global_function
    def inner_func():
        pass

    inner_func_is_global_function = inner_func


main()
