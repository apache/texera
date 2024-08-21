from inspect import Traceback
from typing import NamedTuple

from .internal_queue import InternalQueue
from .internal_marker import EndOfAll, InternalMarker, SenderChange, InputExhausted
from .tuple import Tuple, TupleLike, ArrowTableTupleProvider
from .table import Table, TableLike
from .batch import Batch, BatchLike
from .schema import AttributeType, Field, Schema
from .operator import (
    Operator,
    TupleOperator,
    TableOperator,
    TupleOperatorV2,
    BatchOperator,
    SourceOperator,
)
from .payload import DataFrame, DataPayload, MarkerFrame


class ExceptionInfo(NamedTuple):
    exc: type
    value: Exception
    tb: Traceback


__all__ = [
    "InternalQueue",
    "EndOfAll",
    "InternalMarker",
    "SenderChange",
    "InputExhausted",
    "Tuple",
    "TupleLike",
    "ArrowTableTupleProvider",
    "Table",
    "TableLike",
    "Batch",
    "BatchLike",
    "Operator",
    "TupleOperator",
    "TupleOperatorV2",
    "TableOperator",
    "BatchOperator",
    "SourceOperator",
    "DataFrame",
    "DataPayload",
    "MarkerFrame",
    "ExceptionInfo",
    "AttributeType",
    "Field",
    "Schema",
]
