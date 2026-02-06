import datetime
import json
import subprocess
from collections import UserList, defaultdict
from decimal import Decimal

from django.test import SimpleTestCase

from django_tasks import utils
from tests import tasks as test_tasks


class IsModuleLevelFunctionTestCase(SimpleTestCase):
    @classmethod
    def _class_method(cls) -> None:
        return None

    @staticmethod
    def _static_method() -> None:
        return None

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

    def test_unbound_method(self) -> None:
        self.assertTrue(
            utils.is_module_level_function(self.__class__.test_unbound_method)
        )
        self.assertTrue(utils.is_module_level_function(self.__class__.setUp))

    def test_lambda(self) -> None:
        self.assertFalse(utils.is_module_level_function(lambda: True))

    def test_class_and_static_method(self) -> None:
        self.assertTrue(utils.is_module_level_function(self._static_method))
        self.assertFalse(utils.is_module_level_function(self._class_method))


class JSONNormalizeTestCase(SimpleTestCase):
    def test_converts_json_types(self) -> None:
        for test_case, expected in [  # type: ignore
            (None, "null"),
            (True, "true"),
            (False, "false"),
            (2, "2"),
            (3.0, "3.0"),
            (1e23 + 1, "1e+23"),
            ("1", '"1"'),
            (b"hello", '"hello"'),
            ([], "[]"),
            (UserList([1, 2]), "[1, 2]"),
            ({}, "{}"),
            ({1: "a"}, '{"1": "a"}'),
            ({"foo": (1, 2, 3)}, '{"foo": [1, 2, 3]}'),
            (defaultdict(list), "{}"),
            (float("nan"), "NaN"),
            (float("inf"), "Infinity"),
            (float("-inf"), "-Infinity"),
        ]:
            with self.subTest(test_case):
                normalized = utils.normalize_json(test_case)
                # Ensure that the normalized result is serializable.
                self.assertEqual(json.dumps(normalized), expected)

    def test_bytes_decode_error(self) -> None:
        with self.assertRaisesMessage(ValueError, "Unsupported value"):
            utils.normalize_json(b"\xff")

    def test_encode_error(self) -> None:
        for test_case in [
            self,
            any,
            object(),
            datetime.datetime.now(),
            set(),
            Decimal("3.42"),
        ]:
            with (
                self.subTest(test_case),
                self.assertRaisesMessage(TypeError, "Unsupported type"),
            ):
                utils.normalize_json(test_case)


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
