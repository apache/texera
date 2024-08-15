from dataclasses import dataclass
from pyarrow.lib import Table
from typing import Optional
from core.models.marker import Marker


@dataclass
class DataPayload:
    pass


@dataclass
class DataFrame(DataPayload):
    pass


@dataclass
class MarkerFrame(DataPayload):
    frame: Marker
