from dataclasses import dataclass
from pyarrow.lib import Table
from typing import List

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
    schema: List[str] = None


@dataclass
class EndOfUpstream(DataPayload):
    pass
