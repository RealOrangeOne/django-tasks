from django.test import SimpleTestCase

from django_core_tasks.backends.base import BaseTaskBackend
import subprocess
import sqlite3


def module_level_task_function():
    pass


async def module_level_task_coroutine():
    pass


class IsValidTaskFunctionTestCase(SimpleTestCase):
    def setUp(self):
        self.backend = BaseTaskBackend(options={})

    def test_invalid_type(self):
        for example in [2, "test", 1.5]:
            with self.subTest(example):
                self.assertFalse(self.backend.is_valid_task_function(example))

    def test_builtin(self):
        for example in [any, isinstance]:
            with self.subTest(example):
                self.assertTrue(self.backend.is_valid_task_function(example))

    def test_from_module(self):
        for example in [
            subprocess.run,
            subprocess.check_output,
            # `sqlite3.connect` is pure C in CPython
            sqlite3.connect,
        ]:
            with self.subTest(example):
                self.assertTrue(self.backend.is_valid_task_function(example))

    def test_lambda(self):
        self.assertFalse(self.backend.is_valid_task_function(lambda: True))

    def test_private_function(self):
        def private_task_function():
            pass

        self.assertFalse(self.backend.is_valid_task_function(private_task_function))

    def test_module_function(self):
        self.assertTrue(self.backend.is_valid_task_function(module_level_task_function))
        self.assertTrue(
            self.backend.is_valid_task_function(module_level_task_coroutine)
        )

    def test_class_function(self):
        self.assertFalse(self.backend.is_valid_task_function(self.setUp))

    def test_class(self):
        self.assertFalse(self.backend.is_valid_task_function(BaseTaskBackend))
