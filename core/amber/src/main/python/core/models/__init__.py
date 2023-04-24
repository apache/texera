from inspect import Traceback
from typing import NamedTuple

from .internal_queue import InternalQueue
from .marker import EndOfAllMarker, Marker, SenderChangeMarker
from .tuple import InputExhausted, Tuple, TupleLike, ArrowTableTupleProvider
from .table import Table, TableLike
from .batch import Batch, BatchLike
from .operator import (
    Operator,
    TupleOperator,
    TableOperator,
    TupleOperatorV2,
    BatchOperator,
)
from .payload import InputDataFrame, OutputDataFrame, DataPayload, EndOfUpstream


class ExceptionInfo(NamedTuple):
    exc: type
    value: Exception
    tb: Traceback


__all__ = [
    "InternalQueue",
    "EndOfAllMarker",
    "Marker",
    "SenderChangeMarker",
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
    "InputDataFrame",
    "OutputDataFrame",
    "DataPayload",
    "EndOfUpstream",
    "ExceptionInfo",
]
