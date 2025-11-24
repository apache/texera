# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from inspect import Traceback
from typing import NamedTuple

from .internal_queue import InternalQueue
from .internal_marker import InternalMarker
from .tuple import Tuple, TupleLike, ArrowTableTupleProvider
from .table import Table, TableLike
from .batch import Batch, BatchLike
from .schema import AttributeType, Field, Schema
from .state import State
from .operator import (
    Operator,
    TableOperator,
    TupleOperatorV2,
    BatchOperator,
    SourceOperator,
)
from .payload import DataFrame, DataPayload, StateFrame


class ExceptionInfo(NamedTuple):
    exc: type
    value: Exception
    tb: Traceback


__all__ = [
    "InternalQueue",
    "InternalMarker",
    "Tuple",
    "TupleLike",
    "ArrowTableTupleProvider",
    "Table",
    "TableLike",
    "Batch",
    "BatchLike",
    "Operator",
    "TupleOperatorV2",
    "TableOperator",
    "BatchOperator",
    "SourceOperator",
    "DataFrame",
    "DataPayload",
    "StateFrame",
    "ExceptionInfo",
    "AttributeType",
    "Field",
    "Schema",
    "State",
]
