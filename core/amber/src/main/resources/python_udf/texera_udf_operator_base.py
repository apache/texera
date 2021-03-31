import pickle
from abc import ABC
from enum import Enum
from typing import Dict, Optional, Tuple, Callable, List

import pandas


class TexeraUDFOperator(ABC):
    """
    Base class for row-oriented one-table input, one-table output user-defined operators. This must be implemented
    before using.
    """

    def __init__(self):
        self._args: Tuple = tuple()
        self._kwargs: Optional[Dict] = None

    def open(self, *args) -> None:
        """
        Specify here what the UDF should do before executing on tuples. For example, you may want to open a model file
        before using the model for prediction.

            :param args: a tuple of possible arguments that might be used. This is specified in Texera's UDFOperator's
                configuration panel. The order of these arguments is input attributes, output attributes, outer file
                 paths. Whoever uses these arguments are supposed to know the order.
        """
        self._args = args

    def accept(self, row: pandas.Series, nth_child: int = 0) -> None:
        """
        This is what the UDF operator should do for every row. Do not return anything here, just accept it. The result
        should be retrieved with next().

            :param row: The input row to accept and do custom execution.
            :param nth_child: In the future might be useful.
        """
        pass

    def has_next(self) -> bool:
        """
        Return a boolean value that indicates whether there will be a next result.
        """
        pass

    def next(self) -> pandas.Series:
        """
        Get the next result row. This will be called after accept(), so result should be prepared.
        """
        pass

    def close(self) -> None:
        """
        Close this operator, releasing any resources. For example, you might want to close a model file.
        """
        pass


class TexeraMapOperator(TexeraUDFOperator):
    """
    Base class for one-input-tuple to one-output-tuple mapping operator. Either inherit this class (in case you want to
    override open() and close(), e.g., open and close a model file.) or init this class object with a map function.
    The map function should return the result tuple. If use inherit, then script should have an attribute named
    `operator_instance` that is an instance of the inherited class; If only use filter function, simply define a
    `map_function` in the script.
    """

    def __init__(self, map_function: Callable):
        super().__init__()
        if map_function is None:
            raise NotImplementedError
        self._map_function: Callable = map_function
        self._result_tuples: List = []

    def accept(self, row: pandas.Series, nth_child: int = 0) -> None:
        self._result_tuples.append(self._map_function(row, *self._args))  # must take args

    def has_next(self) -> bool:
        return len(self._result_tuples) != 0

    def next(self) -> pandas.Series:
        return self._result_tuples.pop()

    def close(self) -> None:
        pass


class TexeraFilterOperator(TexeraUDFOperator):
    """
        Base class for filter operators. Either inherit this class (in case you want to
        override open() and close(), e.g., open and close a model file.) or init this class object with a filter function.
        The filter function should return a boolean value indicating whether the input tuple meets the filter criteria.
        If use inherit, then script should have an attribute named `operator_instance` that is an instance of the
        inherited class; If only use filter function, simply define a `filter_function` in the script.
        """

    def __init__(self, filter_function: Callable):
        super().__init__()
        if filter_function is None:
            raise NotImplementedError
        self._filter_function: Callable = filter_function
        self._result_tuples: List = []

    def accept(self, row: pandas.Series, nth_child: int = 0) -> None:
        if self._filter_function(row, *self._args):
            self._result_tuples.append(row)

    def has_next(self) -> bool:
        return len(self._result_tuples) != 0

    def next(self) -> pandas.Series:
        return self._result_tuples.pop()

    def close(self) -> None:
        pass


class TexeraBlockingTrainerOperator(TexeraUDFOperator):
    class STATUS(Enum):
        IDLE = 0
        CONSUMING = 1
        TRAINED = 2

    def __init__(self):
        super().__init__()
        self._X_train: List = []
        self._Y_train: List = []
        self._X_test: List = []
        self._Y_test: List = []
        self._status = TexeraBlockingTrainerOperator.STATUS.IDLE
        self._result_tuples: List = []
        self._train_size = None
        self._test_size = None
        self._train_args = dict()
        self.model_filename = None
        self.vc_filename = None

    def accept(self, row: pandas.Series, nth_child: int = 0) -> None:
        if self._train_size > 0:
            self._status = TexeraBlockingTrainerOperator.STATUS.CONSUMING
            self._X_train.append(row[0])
            self._Y_train.append(row[1])
            self._train_size -= 1
        elif self._test_size > 0:
            self._status = TexeraBlockingTrainerOperator.STATUS.CONSUMING
            self._X_test.append(row[0])
            self._Y_test.append(row[1])
            self._test_size -= 1
        elif self._status == TexeraBlockingTrainerOperator.STATUS.TRAINED:
            # consumes the input and do nothing.
            pass
        if self._train_size == 0 and self._test_size == 0 and self._status != TexeraBlockingTrainerOperator.STATUS.TRAINED:
            vc, model = self.train(self._X_train, self._Y_train, **self._train_args)
            with open(self.model_filename, 'wb') as file:
                pickle.dump(model, file)
            with open(self.vc_filename, 'wb') as file:
                pickle.dump(vc, file)
            if self._X_test:
                Y_pred = model.predict(vc.transform(self._X_test))
                self.report_matrix(self._Y_test, Y_pred)

            self._status = TexeraBlockingTrainerOperator.STATUS.TRAINED

    def has_next(self) -> bool:
        return (self._status == TexeraBlockingTrainerOperator.STATUS.TRAINED) and bool(self._result_tuples)

    def next(self) -> pandas.Series:
        return self._result_tuples.pop()

    def close(self) -> None:
        pass

    @staticmethod
    def train(X_train, Y_train, **kwargs):
        raise NotImplementedError

    def report_matrix(self, Y_test, Y_pred, *args):
        from sklearn.metrics import classification_report
        matrix = pandas.DataFrame(classification_report(Y_test, Y_pred, output_dict=True)).transpose()
        matrix['class'] = [label for label, row in matrix.iterrows()]
        cols = matrix.columns.to_list()
        cols = [cols[-1]] + cols[:-1]
        matrix = matrix[cols]
        for index, row in list(matrix.iterrows())[::-1]:
            if index != 1:
                self._result_tuples.append(row)
