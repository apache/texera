from abc import abstractmethod
from typing import Iterator, Optional, Union

from pyamber import InputExhausted, Operator, Tuple, TupleLike


class UDFOperator(Operator):
    """
    Base class for row-oriented user-defined operators. A concrete implementation must
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
