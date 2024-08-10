from dataclasses import dataclass
from pyarrow.lib import Table
from typing import List, Optional

from core.models.schema.schema import Schema
from core.models.tuple import Tuple


@dataclass
class DataPayload:
    pass


@dataclass
class InputDataFrame(DataPayload):
    frame: Table


@dataclass
class OutputDataFrame(DataPayload):
    frame: List[Tuple]
    schema: Optional[Schema] = None


@dataclass
class EndOfUpstream(DataPayload):
    frame: Optional[Table] = None
