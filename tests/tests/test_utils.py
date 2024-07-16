import datetime
import hashlib
import optparse
import subprocess
from typing import List
from unittest.mock import Mock

from django.core.exceptions import ImproperlyConfigured
from django.test import SimpleTestCase

from django_tasks import utils
from django_tasks.exceptions import InvalidTaskError
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


class ExceptionSerializationTestCase(SimpleTestCase):
    def test_serialize_exceptions(self) -> None:
        for exc in [
            ValueError(10),
            SyntaxError("Wrong"),
            ImproperlyConfigured("It's wrong"),
            InvalidTaskError(""),
            SystemExit(),
        ]:
            with self.subTest(exc):
                data = utils.exception_to_dict(exc)
                self.assertEqual(utils.json_normalize(data), data)
                self.assertEqual(
                    set(data.keys()), {"exc_type", "exc_args", "exc_traceback"}
                )
                exception = utils.exception_from_dict(data)
                self.assertIsInstance(exception, type(exc))
                self.assertEqual(exception.args, exc.args)

                # Check that the exception traceback contains only one line,
                # the one with the name of the exception and the message.
                # The format varies depending of the presence, absence and
                # size of the exception message, and if the exception is
                # a builtin or not.
                dotted_path = utils.get_module_path(exc.__class__).replace(
                    "builtins.", ""
                )
                msg = exc.args[0] if exc.args else ""
                msg = f": {msg}\n" if msg else "\n"
                self.assertEqual(data["exc_traceback"], dotted_path + msg)

    def test_serialize_full_traceback(self) -> None:
        try:
            # Using optparse to generate an error because:
            # - it's pure python
            # - it's easy to trip down
            # - it's unlikely to change ever
            # - We can normalize the strack trace from it
            optparse.OptionParser(option_list=[1])  # type: ignore
        except Exception as e:
            traceback = utils.exception_to_dict(e)["exc_traceback"]

            # The first line and last few lines of the traceback variable parts
            # depend of the position of the optparse module, which comes with
            # few surprises.
            # Between them, there are a few lines that have variable parts
            # depending on our code base. We skip those to avoid maintenance burden.
            self.assertTrue(traceback.startswith("Traceback (most recent call last):"))
            self.assertTrue(
                traceback.endswith(
                    f'  File "{optparse.__file__}", line 1206, in __init__\n'
                    "    self._populate_option_list(option_list,\n"
                    f'  File "{optparse.__file__}", line 1249, in _populate_option_list\n'
                    "    self.add_options(option_list)\n"
                    f'  File "{optparse.__file__}", line 1027, in add_options\n'
                    "    self.add_option(option)\n"
                    f'  File "{optparse.__file__}", line 1004, in add_option\n'
                    '    raise TypeError("not an Option instance: %r" % option)\n'
                    "TypeError: not an Option instance: 1\n"
                )
            )

    def test_serialize_traceback_from_c_module(self) -> None:
        try:
            # Same as test_serialize_full_traceback, but uses hashlib
            # because it's in C, not in Python
            hashlib.md5(1)  # type: ignore
        except Exception as e:
            traceback = utils.exception_to_dict(e)["exc_traceback"]
            self.assertTrue(traceback.startswith("Traceback (most recent call last):"))
            self.assertTrue(
                traceback.endswith(
                    "TypeError: object supporting the buffer API required\n"
                )
            )
            # Check that it's indeed a short traceback that sees mostly
            # the error line
            self.assertIn("hashlib.md5(1)", traceback)
            self.assertEqual(len(traceback.strip("\n").split("\n")), 4)

    def test_cannot_deserialize_non_exception(self) -> None:
        serialized_exceptions: List[utils.SerializedExceptionDict] = [
            {
                "exc_type": "subprocess.check_output",
                "exc_args": ["exit", "1"],
                "exc_traceback": "",
            },
            {"exc_type": "True", "exc_args": [], "exc_traceback": ""},
            {"exc_type": "math.pi", "exc_args": [], "exc_traceback": ""},
            {"exc_type": __name__, "exc_args": [], "exc_traceback": ""},
            {
                "exc_type": utils.get_module_path(type(self)),
                "exc_args": [],
                "exc_traceback": "",
            },
            {
                "exc_type": utils.get_module_path(Mock),
                "exc_args": [],
                "exc_traceback": "",
            },
        ]

        for data in serialized_exceptions:
            with self.subTest(data):
                with self.assertRaises((TypeError, ImportError)):
                    utils.exception_from_dict(data)
