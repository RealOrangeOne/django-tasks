import datetime
import subprocess
from unittest.mock import Mock

from django.test import SimpleTestCase

from django_tasks import utils
from tests import tasks as test_tasks


class IsModuleLevelFunctionTestCase(SimpleTestCase):
    def test_builtin(self) -> None:
        self.assertFalse(utils.is_module_level_function(any))
        self.assertFalse(utils.is_module_level_function(isinstance))

    def test_from_module(self) -> None:
        self.assertTrue(utils.is_module_level_function(subprocess.run))
        self.assertTrue(utils.is_module_level_function(subprocess.check_output))
        self.assertTrue(utils.is_module_level_function(test_tasks.noop_task.func))

    def test_private_function(self) -> None:
        def private_function() -> None:
            pass

        self.assertFalse(utils.is_module_level_function(private_function))

    def test_coroutine(self) -> None:
        self.assertTrue(utils.is_module_level_function(test_tasks.noop_task_async.func))

    def test_method(self) -> None:
        self.assertFalse(utils.is_module_level_function(self.test_method))
        self.assertFalse(utils.is_module_level_function(self.setUp))

    def test_lambda(self) -> None:
        self.assertFalse(utils.is_module_level_function(lambda: True))

    def test_uninitialised_method(self) -> None:
        # This import has to be here, so the module is loaded during the test
        from . import is_module_level_function_fixture

        self.assertTrue(is_module_level_function_fixture.really_module_level_function)
        self.assertIsNotNone(
            is_module_level_function_fixture.inner_func_is_module_level_function
        )
        self.assertFalse(
            is_module_level_function_fixture.inner_func_is_module_level_function
        )


class JSONNormalizeTestCase(SimpleTestCase):
    def test_round_trip(self) -> None:
        self.assertEqual(utils.json_normalize({}), {})
        self.assertEqual(utils.json_normalize([]), [])
        self.assertEqual(utils.json_normalize(()), [])
        self.assertEqual(utils.json_normalize({"foo": ()}), {"foo": []})

    def test_encode_error(self) -> None:
        for example in [self, any, datetime.datetime.now()]:
            with self.subTest(example):
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

    def test_skip_retry_on_keyboard_interrupt(self) -> None:
        sentinel = Mock(side_effect=KeyboardInterrupt(""))

        with self.assertRaises(KeyboardInterrupt):
            utils.retry()(sentinel)()

        self.assertEqual(sentinel.call_count, 1)


class RandomIdTestCase(SimpleTestCase):
    def test_correct_length(self) -> None:
        self.assertEqual(len(utils.get_random_id()), 32)

    def test_random_ish(self) -> None:
        random_ids = [utils.get_random_id() for _ in range(1000)]

        self.assertEqual(len(random_ids), len(set(random_ids)))


class ExceptionTracebackTestCase(SimpleTestCase):
    def test_literal_exception(self) -> None:
        self.assertEqual(
            utils.get_exception_traceback(ValueError("Failure")),
            "ValueError: Failure\n",
        )

    def test_exception(self) -> None:
        try:
            1 / 0  # noqa:B018
        except ZeroDivisionError as e:
            traceback = utils.get_exception_traceback(e)
            self.assertIn("ZeroDivisionError: division by zero", traceback)
        else:
            self.fail("ZeroDivisionError not raised")

    def test_complex_exception(self) -> None:
        try:
            {}[datetime.datetime.now()]  # type: ignore
        except KeyError as e:
            traceback = utils.get_exception_traceback(e)
            self.assertIn("KeyError: datetime.datetime", traceback)
        else:
            self.fail("KeyError not raised")
