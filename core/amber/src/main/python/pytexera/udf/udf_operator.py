from abc import abstractmethod
from typing import Iterator, Optional, Union
from deprecated import deprecated
from pyamber import *


@deprecated(reason="Use UDFOperatorV2 instead")
class UDFOperator(TupleOperator):
    def open(self) -> None:
        """
        Open a context of the operator. Usually can be used for loading/initiating some
        resources, such as a file, a model, or an API client.
        """
        pass

    @abstractmethod
    def process_tuple(
        self, tuple_: Union[Tuple, InputExhausted], input_: int
    ) -> Iterator[Optional[TupleLike]]:
        """
        Process an input Tuple from the given link.

        :param tuple_: Union[Tuple, InputExhausted], either
                        1. a Tuple from a link to be processed;
                        2. an InputExhausted indicating no more data from this link.
        :param input_: int, input index of the current Tuple.
        :return: Iterator[Optional[TupleLike]], producing one TupleLike object at a
            time, or None.
        """
        yield

    def close(self) -> None:
        """
        Close the context of the operator.
        """
        pass


class UDFOperatorV2(TupleOperatorV2):
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
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        """
        Process an input Tuple from the given link.

        :param tuple_: Tuple, a Tuple from an input port to be processed.
        :param port: int, input port index of the current Tuple.
        :return: Iterator[Optional[TupleLike]], producing one TupleLike object at a
            time, or None.

        See .examples/ for example operators.
        """
        yield

    def on_finish(self, port: int) -> Iterator[Optional[TupleLike]]:
        """
        Callback when one input port is exhausted.

        :param port: int, input port index of the current exhausted port.
        :return: Iterator[Optional[TupleLike]], producing one TupleLike object at a
            time, or None.
        """
        yield

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
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        """
        Process an input Table from the given link. The Table is represented as
        pandas.DataFrame.

        :param table: Table, a table to be processed.
        :param port: int, input index of the current Table.
        :return: Iterator[Optional[TableLike]], producing one TableLike object at a
            time, or None.
        """
        yield

    def close(self) -> None:
        """
        Close the context of the operator.
        """
        pass


class UDFBatchOperator(BatchOperator):
    """
    Base class for batch-oriented user-defined operators. A concrete implementation must
    be provided upon using.
    """

    def open(self) -> None:
        """
        Open a context of the operator. Usually can be used for loading/initiating some
        resources, such as a file, a model, or an API client.
        """
        pass

    @abstractmethod
    def process_batch(self, batch: Batch, port: int) -> Iterator[Optional[BatchLike]]:
        """
        Process an input Batch from the given link. The Batch is represented as
        pandas.DataFrame.

        :param batch: Batch, a batch to be processed.
        :param port: int, input index of the current Batch.
        :return: Iterator[Optional[BatchLike]], producing one BatchLike object at a
            time, or None.
        """
        yield

    def close(self) -> None:
        """
        Close the context of the operator.
        """
        pass
