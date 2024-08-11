from dataclasses import dataclass
from pyarrow.lib import Table
from typing import List, Optional

from core.models.schema.schema import Schema
from core.models.tuple import Tuple


@dataclass
class DataPayload:
    pass


@dataclass
class DataFrame(DataPayload):
    frame: Table


@dataclass
class EndOfUpstream(DataPayload):
    frame: Optional[Table] = None
