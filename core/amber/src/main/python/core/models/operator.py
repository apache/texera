import overrides
from abc import ABC, abstractmethod
from typing import Iterator, Optional, Union

from . import InputExhausted, Tuple, TupleLike


class Operator(ABC):
    """
    Base class for row-oriented operators. A concrete implementation must
    be provided upon using.
    """

    def __init__(self):
        self.__internal_is_source: bool = False

    @property
    @overrides.final
    def is_source(self) -> bool:
        """
        Whether the operator is a source operator. Source operators generates output
        Tuples without having input Tuples.
        :return:
        """
        return self.__internal_is_source

    @is_source.setter
    @overrides.final
    def is_source(self, value: bool) -> None:
        self.__internal_is_source = value

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
        """
        pass

    def close(self) -> None:
        """
        Close the context of the operator.
        """
        pass
