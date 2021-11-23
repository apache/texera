from abc import abstractmethod
from typing import Iterator, Optional, Union

from pyamber import *


class UDFOperator(TupleOperator):
    """
    Base class for tuple-oriented user-defined operators. A concrete implementation must
    be provided upon using.
    """

    def open(self) -> None:
        """
        Open a context of the operator. Usually can be used for loading/initiating some
        resources, such as a file, a model, or an API client.
        """
        pass

    @abstractmethod
    def process_tuple(self, tuple_: Union[Tuple, InputExhausted], input_: int) -> Iterator[Optional[TupleLike]]:
        """
        Process an input Tuple from the given link. The Tuple is represented as pandas.Series.
        :param tuple_: Union[Tuple, InputExhausted], either
                        1. a Tuple from a link to be processed;
                        2. an InputExhausted indicating no more data from this link.

        :param input_: int, input index of the current Tuple.
        :return: Iterator[Optional[TupleLike]], producing one TupleLike object at a time, or None.

        example:
            class EchoOperator(UDFOperator):
                def process_tuple(
                    self,
                    tuple_: Union[Tuple, InputExhausted],
                    input_: int
                ) -> Iterator[Optional[TupleLike]]:
                    if isinstance(tuple_, Tuple):
                        yield tuple_

        See .examples/ for more example operators.
        """
        pass

    def close(self) -> None:
        """
        Close the context of the operator.
        """
        pass


class UDFTableOperator(TableOperator):
    """
    Base class for table-oriented user-defined operators. A concrete implementation must
    be provided upon using.
    """

    def open(self) -> None:
        """
        Open a context of the operator. Usually can be used for loading/initiating some
        resources, such as a file, a model, or an API client.
        """
        pass

    @abstractmethod
    def process_table(self, table: Table, input_: int) -> Iterator[Optional[TableLike]]:
        """
        Process an input Table from the given link. The Table is represented as pandas.DataFrame.
        :param table: Table, a table to be processed.
        :param input_: int, input index of the current Table.
        :return: Iterator[Optional[TableLike]], producing one TableLike object at a time, or None.
        """
        pass

    def close(self) -> None:
        """
        Close the context of the operator.
        """
        pass
