from django.test import SimpleTestCase
from django_core_tasks.utils import is_marked_task_func, task_function
from . import tasks as test_tasks
from django_core_tasks.exceptions import InvalidTask


class TaskFunctionDecoratorTestCase(SimpleTestCase):
    def test_decorate_function(self):
        self.assertTrue(is_marked_task_func(test_tasks.noop_task))
        self.assertTrue(is_marked_task_func(test_tasks.noop_task_async))

    def test_decorate_builtin(self):
        for example in [any, isinstance]:
            with self.subTest(example):
                with self.assertRaises(InvalidTask):
                    task_function(example)

    def test_decorate_wrong_type(self):
        for example in [1, 1.5, "2"]:
            with self.subTest(example):
                with self.assertRaises(InvalidTask):
                    task_function(example)

    def test_marks_function(self):
        def private_function():
            pass

        private_task_function = task_function(private_function)

        private_function()
        private_task_function()

        self.assertIs(private_function, private_task_function)
        self.assertTrue(is_marked_task_func(private_task_function))

    async def test_marks_async_function(self):
        async def private_function():
            pass

        private_task_function = task_function(private_function)

        await private_function()
        await private_task_function()

        self.assertIs(private_function, private_task_function)
        self.assertTrue(is_marked_task_func(private_task_function))
