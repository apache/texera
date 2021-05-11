import functools
import logging
import pickle
from abc import ABC
from typing import Dict, Optional, Tuple, Callable, List

import pandas
from sklearn.model_selection import train_test_split


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


class TexeraFilterOperator(TexeraUDFOperator):
    """
    Base class for filter operators. Either inherit this class (in case you want to
    override open() and close(), e.g., open and close a model file.) or init this class object with a filter function.
    The filter function should return a boolean value indicating whether the input tuple meets the filter criteria.
    If use inherit, then script should have an attribute named `operator_instance` that is an instance of the
    inherited class; If only use filter function, simply define a `filter_function` in the script.
    """
    logger = logging.getLogger("PythonUDF.TexeraFilterOperator")

    @exception(logger)
    def __init__(self, filter_function: Callable):
        super().__init__()
        if filter_function is None:
            raise NotImplementedError
        self._filter_function: Callable = filter_function

    @exception(logger)
    def accept(self, row: pandas.Series, nth_child: int = 0) -> None:
        if self._filter_function(row, *self._args):
            self._result_tuples.append(row)


class TexeraBlockingSupervisedTrainerOperator(TexeraUDFOperator):
    logger = logging.getLogger("PythonUDF.TexeraBlockingSupervisedTrainerOperator")

    @exception(logger)
    def __init__(self):
        super().__init__()
        self._x = []
        self._y = []
        self._test_ratio = None
        self._train_args = dict()
        self._model_file_path = None

    @exception(logger)
    def input_exhausted(self, *args, **kwargs):
        x_train, x_test, y_train, y_test = train_test_split(self._x, self._y, test_size=self._test_ratio, random_state=1)
        model = self.train(x_train, y_train, **self._train_args)
        with open(self._model_file_path, "wb") as file:
            pickle.dump(model, file)

        if x_test:
            y_pred = self.test(model, x_test, y_test)
            self.report(y_test, y_pred)

    @exception(logger)
    def accept(self, row: pandas.Series, nth_child: int = 0) -> None:
        self._x.append(row[0])
        self._y.append(row[1])

    @staticmethod
    @exception(logger)
    def train(x_train, y_train, *args, **kwargs):
        raise NotImplementedError

    @staticmethod
    @exception(logger)
    def test(model, x_test, y_test, *args, **kwargs):
        pass

    @exception(logger)
    def report(self, y_test, y_pred, *args, **kwargs):
        from sklearn.metrics import classification_report
        matrix = pandas.DataFrame(classification_report(y_test, y_pred, output_dict=True)).transpose()
        matrix['class'] = [label for label, row in matrix.iterrows()]
        cols = matrix.columns.to_list()
        cols = [cols[-1]] + cols[:-1]
        matrix = matrix[cols].round(3)
        for index, row in matrix.iterrows():
            if index != 1:
                self._result_tuples.append(row)


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
