from abc import ABC, abstractmethod
from typing import Iterator, Union

import overrides

from core.models.tuple import InputExhausted, Tuple
from proto.edu.uci.ics.amber.engine.common import LinkIdentity


class UDFOperator(ABC):
    """
    Base class for row-oriented user-defined operators. A concrete implementation must
    be provided upon using.
    """

    def __init__(self, is_source=False):
        self._is_source: bool = is_source

    @overrides.final
    @property
    def is_source(self) -> bool:
        """
        Whether the operator is a source operator. Source operators generates output
        Tuples without input Tuples.
        :return:
        """
        return self._is_source

    def open(self) -> None:
        """
        Open a context of the operator. Usually can be used for loading/initiating some
        resources, such as a file, a model, or an API client.
        """
        pass

    @abstractmethod
    def process_texera_tuple(self, tuple_: Union[Tuple, InputExhausted], link: LinkIdentity) -> Iterator[Tuple]:
        """
        Process an input Tuple from the given link. The Tuple is represented as pandas.Series.
        :param tuple_: Union[Tuple, InputExhausted], either
                        1. a Tuple from a link to be processed;
                        2. an InputExhausted indicating no more data from this link.

                        Tuple is implemented as pandas.Series.

        :param link: LinkIdentity, indicating where the Tuple came from.
        :return: Iterator[Tuple], producing one Tuple/pandas.Series at a time.

        example:
            class EchoOperator(UDFOperator):
                def process_texera_tuple(
                    self,
                    tuple_: Union[pd.Series, InputExhausted],
                    link: LinkIdentity
                ) -> Iterator[pd.Series]:
                    if isinstance(tuple_, Tuple):
                        yield tuple_
        """
        # TODO: add example operators somewhere else.
        pass

    def close(self) -> None:
        """
        Close the context of the operator.
        """
        pass
