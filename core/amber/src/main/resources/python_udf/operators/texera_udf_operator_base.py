import functools
import inspect
import logging
from typing import Dict, Optional, Tuple, List

import pandas


def get_class_that_defined_method(meth):
    if isinstance(meth, functools.partial):
        return get_class_that_defined_method(meth.func)
    if inspect.ismethod(meth) or (
            inspect.isbuiltin(meth) and getattr(meth, '__self__', None) is not None and getattr(meth.__self__, '__class__', None)):
        for cls in inspect.getmro(meth.__self__.__class__):
            if meth.__name__ in cls.__dict__:
                return cls
        meth = getattr(meth, '__func__', meth)  # fallback to __qualname__ parsing
    if inspect.isfunction(meth):
        cls = getattr(inspect.getmodule(meth),
                      meth.__qualname__.split('.<locals>', 1)[0].rsplit('.', 1)[0],
                      None)
        if isinstance(cls, type):
            return cls
    return getattr(meth, '__objclass__', None)  # handle special descriptor objects


class exception:
    """
    a decorator to log the exception and re-raise the exception.

    """

    def __init__(self, fn):
        functools.update_wrapper(self, fn)
        self.fn = fn

    def __set_name__(self, owner, name):
        # do something with owner, i.e.
        print(f"decorating {self.fn} and using {owner}")
        self.fn.class_name = owner.__name__

        # then replace ourself with the original method
        setattr(owner, name, self.fn)

    def __call__(self, *args, **kwargs):
        try:
            return self.fn(*args, **kwargs)
        except Exception:
            err = "exception in " + self.fn.__name__ + "\n"
            err += "-------------------------------------------------------------------------\n"
            self.fn.class_name.__logger.exception(err)
            raise


class MetaBase(type):
    def __init__(cls, *args):
        super().__init__(*args)

        # Explicit name mangling
        logger_attribute_name = '_' + cls.__name__ + '__logger'

        setattr(cls, logger_attribute_name, logging.getLogger(cls.__name__))


class TexeraUDFOperator(metaclass=MetaBase):
    """
    Base class for row-oriented one-table input, one-table output user-defined operators. This must be implemented
    before using.
    """
    __logger = None

    @exception
    def __init__(self):
        self._args: Tuple = tuple()
        self._kwargs: Optional[Dict] = None
        self._result_tuples: List = []

    @exception
    def open(self, *args) -> None:
        """
        Specify here what the UDF should do before executing on tuples. For example, you may want to open a model file
        before using the model for prediction.

            :param args: a tuple of possible arguments that might be used. This is specified in Texera's UDFOperator's
                configuration panel. The order of these arguments is input attributes, output attributes, outer file
                 paths. Whoever uses these arguments are supposed to know the order.
        """
        self._args = args

    @exception
    def accept(self, row: pandas.Series, nth_child: int = 0) -> None:
        """
        This is what the UDF operator should do for every row. Do not return anything here, just accept it. The result
        should be retrieved with next().

            :param row: The input row to accept and do custom execution.
            :param nth_child: In the future might be useful.
        """
        pass

    @exception
    def has_next(self) -> bool:
        """
        Return a boolean value that indicates whether there will be a next result.
        """
        return bool(self._result_tuples)

    @exception
    def next(self) -> pandas.Series:
        """
        Get the next result row. This will be called after accept(), so result should be prepared.
        """
        return self._result_tuples.pop(0)

    @exception
    def close(self) -> None:
        """
        Close this operator, releasing any resources. For example, you might want to close a model file.
        """
        pass

    @exception
    def input_exhausted(self, *args, **kwargs):
        """
        Executes when the input is exhausted, useful for some blocking execution like training.
        """
        pass
