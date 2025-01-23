from dataclasses import dataclass
from pyarrow.lib import Table
from core.models.marker import Marker


@dataclass
class DataPayload:
    pass


@dataclass
class DataFrame(DataPayload):
    frame: Table


@dataclass
class MarkerFrame(DataPayload):
    frame: Marker
