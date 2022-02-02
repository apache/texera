from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Iterator, List, Mapping, Optional, Union

import overrides
import pandas
from pyarrow.lib import Schema

from . import InputExhausted, Table, TableLike, Tuple, TupleLike
from ..util.arrow_utils import to_arrow_schema


class Operator(ABC):
    """
    Abstract base class for all operators.
    """

    def __init__(self):
        self.__internal_is_source: bool = False
        self.__internal_output_schema: Optional[Schema] = None

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

    @property
    @overrides.final
    def output_schema(self) -> Schema:
        assert self.__internal_output_schema is not None
        return self.__internal_output_schema

    @output_schema.setter
    @overrides.final
    def output_schema(self, raw_output_schema: Union[Schema, Mapping[str, str]]) -> None:
        self.__internal_output_schema = raw_output_schema if isinstance(raw_output_schema, Schema) else \
            to_arrow_schema(raw_output_schema)

    def open(self) -> None:
        """
        Open a context of the operator. Usually can be used for loading/initiating some
        resources, such as a file, a model, or an API client.
        """
        pass

    def close(self) -> None:
        """
        Close the context of the operator.
        """
        pass


class TupleOperator(Operator):
    """
    Base class for tuple-oriented operators. A concrete implementation must
    be provided upon using.
    """

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


class TableOperator(TupleOperator):
    """
    Base class for table-oriented operators. A concrete implementation must
    be provided upon using.
    """

    def __init__(self):
        super().__init__()
        self.__internal_is_source: bool = False
        self.__table_data: Mapping[int, List[Tuple]] = defaultdict(list)

    @overrides.final
    def process_tuple(self, tuple_: Union[Tuple, InputExhausted], input_: int) -> Iterator[Optional[TupleLike]]:
        """
        Process an input Tuple from the given link. The Tuple is represented as pandas.Series.
        :param tuple_: Union[Tuple, InputExhausted], either
                        1. a Tuple from a link to be processed;
                        2. an InputExhausted indicating no more data from this link.

        :param input_: int, input index of the current Tuple.
        :return: Iterator[Optional[TupleLike]], producing one TupleLike object at a time, or None.
        """
        if isinstance(tuple_, Tuple):
            self.__table_data[input_].append(tuple_)
        else:
            table = Table(pandas.DataFrame([i.as_series() for i in self.__table_data[input_]]))
            for output_table in self.process_table(table, input_):
                if output_table is not None:
                    for _, output_tuple in output_table.iterrows():
                        yield output_tuple

    @abstractmethod
    def process_table(self, table: Table, input_: int) -> Iterator[Optional[TableLike]]:
        """
        Process an input Table from the given link. The Table is represented as pandas.DataFrame.
        :param table: Table, a table to be processed.
        :param input_: int, input index of the current Table.
        :return: Iterator[Optional[TableLike]], producing one TableLike object at a time, or None.
        """
        pass
