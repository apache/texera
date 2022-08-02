from functools import wraps
from threading import Lock
from typing import Any, Callable, TypeVar, Union

import pytest
from pytest import Item

# TypeVar for the `func` parameter of the `Reraise.wrap()` method
F = TypeVar("F", bound=Callable[..., Any])


class Reraise:
    def __init__(self):
        self._catch = False
        self._origin = self
        self._exception = None
        self._exception_lock = Lock()

    def __enter__(self):
        return None

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            self.exception = exc_value
        return self._catch  # Raise the exception if catch is false

    @property
    def exception(self):
        """
        The exception that has been captured by this `Reraise` instance, if any.
        """
        with self._origin._exception_lock:
            return self._origin._exception

    @exception.setter
    def exception(self, e: Exception):
        """
        Set the exception that this `Reraise` instance raises, if no exception has been
        captured or set yet.
        """
        origin = self._origin
        with origin._exception_lock:
            if origin._exception is None:
                e._is_from_pytest_reraise = True  # Annotate the exception
                origin._exception = e

    def reset(self):
        """
        Resets the exception that was previously captured, if any, and returns it.
        """
        origin = self._origin
        with origin._exception_lock:
            if origin._exception is not None:
                e = origin._exception
                origin._exception = None
                return e

    def __call__(self, catch: bool = None) -> Union["Reraise", None]:
        """
        If called without arguments, raises the first exception that has been captured
        by this `Reraise` context manager since the last call to `reraise()`. Note that
        this is automatically done at the end of each test case that uses `reraise`, and
        during the teardown phase of these test cases. You can manually do it if you
        wish to interrupt your main thread instead.

        If called with the `catch` argument, returns a context manager that catches
        exceptions, or forwards them, depending on the value of the catch argument.
        """
        if catch is None:
            e = self.reset()
            if e is not None:
                raise e
        else:
            child = Reraise()
            child._catch = catch
            child._origin = self._origin
            return child

    def wrap(self, func: F) -> F:
        """
        Wraps the provided function in a `reraise` context manager and returns the
        resulting wrapper function.

        Example:
        ```python
        def run():
            with reraise:
                assert False

        Thread(target=run).start()
        ```
        can be written as
        ```python
        def run():
            assert False

        Thread(target=reraise.wrap(run)).start()
        ```
        or
        ```python
        @reraise.wrap
        def run():
            assert False

        Thread(target=run).start()
        ```
        """

        @wraps(func)
        def wrapped(*args, **kwargs):
            with self:
                return func(*args, **kwargs)

        return wrapped


@pytest.fixture
def reraise():
    reraise = Reraise()
    yield reraise
    # If `reraise` has captured an exception during the teardown phase, it was not
    # handled by the pytest_runtest_call wrapper anymore.
    reraise()


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_call(item: Item):
    result = yield
    if hasattr(item, "funcargs") and "reraise" in item.funcargs:
        reraise = item.funcargs["reraise"]

        # Override any non-re-raised exception in the main thread by calling `reraise()`
        if result.excinfo is None or not hasattr(
            result.excinfo[1], "_is_from_pytest_reraise"
        ):
            reraise()
        else:
            # The test case re-raised an exception already, so we don't mask it by
            # re-raising any other exception captured since then. If anything, we drop
            # any other exception so it will not be raised in the teardown phase.
            reraise.reset()
