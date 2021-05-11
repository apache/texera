import functools
import logging
from abc import ABC
from typing import Dict, Optional, Tuple, List

import pandas


def exception(logger):
    """
    a decorator to log the exception and re-raise the exception.
    :param logger: the target logger to use, can be different level.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception:
                err = "exception in " + func.__name__ + "\n"
                err += "-------------------------------------------------------------------------\n"
                logger.exception(err)
                raise

        return wrapper

    return decorator


class TexeraUDFOperator(ABC):
    """
    Base class for row-oriented one-table input, one-table output user-defined operators. This must be implemented
    before using.
    """

    logger = logging.getLogger("PythonUDF.TexeraUDFOperator")

    @exception(logger)
    def __init__(self):
        self._args: Tuple = tuple()
        self._kwargs: Optional[Dict] = None
        self._result_tuples: List = []

    @exception(logger)
    def open(self, *args) -> None:
        """
        Specify here what the UDF should do before executing on tuples. For example, you may want to open a model file
        before using the model for prediction.

            :param args: a tuple of possible arguments that might be used. This is specified in Texera's UDFOperator's
                configuration panel. The order of these arguments is input attributes, output attributes, outer file
                 paths. Whoever uses these arguments are supposed to know the order.
        """
        self._args = args

    @exception(logger)
    def accept(self, row: pandas.Series, nth_child: int = 0) -> None:
        """
        This is what the UDF operator should do for every row. Do not return anything here, just accept it. The result
        should be retrieved with next().

            :param row: The input row to accept and do custom execution.
            :param nth_child: In the future might be useful.
        """
        pass

    @exception(logger)
    def has_next(self) -> bool:
        """
        Return a boolean value that indicates whether there will be a next result.
        """
        return bool(self._result_tuples)

    @exception(logger)
    def next(self) -> pandas.Series:
        """
        Get the next result row. This will be called after accept(), so result should be prepared.
        """
        return self._result_tuples.pop(0)

    @exception(logger)
    def close(self) -> None:
        """
        Close this operator, releasing any resources. For example, you might want to close a model file.
        """
        pass

    @exception(logger)
    def input_exhausted(self, *args, **kwargs):
        """
        Executes when the input is exhausted, useful for some blocking execution like training.
        """
        pass


class TexeraBlockingUnsupervisedTrainerOperator(TexeraUDFOperator):
    logger = logging.getLogger("PythonUDF.TexeraBlockingUnsupervisedTrainerOperator")

    @exception(logger)
    def __init__(self):
        super().__init__()
        self._data = []
        self._train_args = dict()

    @exception(logger)
    def accept(self, row: pandas.Series, nth_child: int = 0) -> None:
        self._data.append(row[0])

    @exception(logger)
    def close(self) -> None:
        pass

    @staticmethod
    @exception(logger)
    def train(data, *args, **kwargs):
        raise NotImplementedError

    @exception(logger)
    def report(self, model) -> None:
        pass

    @exception(logger)
    def input_exhausted(self, *args):
        model = self.train(self._data, **self._train_args)
        self.report(model)
