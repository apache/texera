import shelve
from abc import ABC
from typing import Dict, Optional, Tuple, Callable, List

import pandas
from sklearn.model_selection import train_test_split


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

    def input_exhausted(self, *args):
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
        return bool(self._result_tuples)

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


class TexeraBlockingSupervisedTrainerOperator(TexeraUDFOperator):

    def __init__(self):
        super().__init__()
        self._x = []
        self._y = []
        self._result_tuples: List = []
        self._test_ratio = None
        self._train_args = dict()
        self._model_file_path = None

    def input_exhausted(self, *args):
        x_train, x_test, y_train, y_test = train_test_split(self._x, self._y, test_size=self._test_ratio, random_state=1)
        vc, model = self.train(x_train, y_train, **self._train_args)

        with shelve.open(self._model_file_path) as db:
            db['model'] = model
            db['vc'] = vc
        if x_test:
            y_pred = model.predict(vc.transform(x_test))
            self.report_matrix(y_test, y_pred)

    def accept(self, row: pandas.Series, nth_child: int = 0) -> None:
        self._x.append(row[0])
        self._y.append(row[1])

    def has_next(self) -> bool:
        return bool(self._result_tuples)

    def next(self) -> pandas.Series:
        return self._result_tuples.pop()

    def close(self) -> None:
        pass

    @staticmethod
    def train(x_train, y_train, **kwargs):
        raise NotImplementedError

    def report_matrix(self, y_test, y_pred, *args):
        from sklearn.metrics import classification_report
        matrix = pandas.DataFrame(classification_report(y_test, y_pred, output_dict=True)).transpose()
        matrix['class'] = [label for label, row in matrix.iterrows()]
        cols = matrix.columns.to_list()
        cols = [cols[-1]] + cols[:-1]
        matrix = matrix[cols].round(3)
        for index, row in list(matrix.iterrows())[::-1]:
            if index != 1:
                self._result_tuples.append(row)

class TexeraBlockingUnsupervisedTrainerOperator(TexeraUDFOperator):

    def __init__(self):
        super().__init__()
        self._data = []
        self._result_tuples: List = []
        self._train_args = dict()

    def input_exhausted(self, *args):
        model = self.train(self._data, **self._train_args)
        self.report(model)

    def accept(self, row: pandas.Series, nth_child: int = 0) -> None:
        self._data.append(row[0])

    def has_next(self) -> bool:
        return bool(self._result_tuples)

    def next(self) -> pandas.Series:
        return self._result_tuples.pop()

    def close(self) -> None:
        pass

    @staticmethod
    def train(data, **kwargs):
        raise NotImplementedError

    def report(self, model) -> None:
        pass

