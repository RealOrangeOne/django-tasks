import datetime
import subprocess
from unittest.mock import Mock

from django.test import SimpleTestCase

from django_tasks import utils
from tests import tasks as test_tasks


class IsGlobalFunctionTestCase(SimpleTestCase):
    def test_builtin(self) -> None:
        self.assertFalse(utils.is_global_function(any))
        self.assertFalse(utils.is_global_function(isinstance))

    def test_from_module(self) -> None:
        self.assertTrue(utils.is_global_function(subprocess.run))
        self.assertTrue(utils.is_global_function(subprocess.check_output))
        self.assertTrue(utils.is_global_function(test_tasks.noop_task.func))

    def test_private_function(self) -> None:
        def private_function() -> None:
            pass

        self.assertFalse(utils.is_global_function(private_function))

    def test_coroutine(self) -> None:
        self.assertTrue(utils.is_global_function(test_tasks.noop_task_async.func))

    def test_method(self) -> None:
        self.assertFalse(utils.is_global_function(self.test_method))
        self.assertFalse(utils.is_global_function(self.setUp))

    def test_lambda(self) -> None:
        self.assertFalse(utils.is_global_function(lambda: True))

    def test_uninitialised_method(self) -> None:
        # This import has to be here, so the module is loaded during the test
        from . import is_global_function_fixture

        self.assertTrue(is_global_function_fixture.really_global_function)
        self.assertIsNotNone(is_global_function_fixture.inner_func_is_global_function)
        self.assertFalse(is_global_function_fixture.inner_func_is_global_function)


class IsJSONSerializableTestCase(SimpleTestCase):
    def test_serializable(self) -> None:
        for example in [123, 12.3, "123", {"123": 456}, [], None]:
            with self.subTest(example):
                self.assertTrue(utils.is_json_serializable(example))

    def test_not_serializable(self) -> None:
        for example in [
            self,
            any,
            datetime.datetime.now(),
        ]:
            with self.subTest(example):
                self.assertFalse(utils.is_json_serializable(example))


class JSONNormalizeTestCase(SimpleTestCase):
    def test_round_trip(self) -> None:
        self.assertEqual(utils.json_normalize({}), {})
        self.assertEqual(utils.json_normalize([]), [])
        self.assertEqual(utils.json_normalize(()), [])
        self.assertEqual(utils.json_normalize({"foo": ()}), {"foo": []})

    def test_encode_error(self) -> None:
        for example in [self, any, datetime.datetime.now()]:
            with self.subTest(example):
                self.assertFalse(utils.is_json_serializable(example))
                self.assertRaises(TypeError, utils.json_normalize, example)


class RetryTestCase(SimpleTestCase):
    def test_retry(self) -> None:
        sentinel = Mock(side_effect=ValueError(""))

        with self.assertRaises(ValueError):
            utils.retry()(sentinel)()

        self.assertEqual(sentinel.call_count, 3)

    def test_keeps_return_value(self) -> None:
        self.assertTrue(utils.retry()(lambda: True)())
        self.assertFalse(utils.retry()(lambda: False)())
