from dataclasses import dataclass

from core.models.tuple import Tuple


@dataclass
class DataPayload:
    pass


@dataclass
class DataFrame(DataPayload):
    frame: list[Tuple]


@dataclass
class EndOfUpstream(DataPayload):
    pass
