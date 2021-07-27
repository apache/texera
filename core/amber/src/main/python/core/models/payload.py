from dataclasses import dataclass
from typing import List

from core.models.tuple import Tuple


@dataclass
class DataPayload:
    pass


@dataclass
class DataFrame(DataPayload):
    frame: List[Tuple]


@dataclass
class EndOfUpstream(DataPayload):
    pass
