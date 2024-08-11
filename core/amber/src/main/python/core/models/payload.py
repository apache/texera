from dataclasses import dataclass
from pyarrow.lib import Table
from typing import Optional


@dataclass
class DataPayload:
    frame: Optional[Table] = None


DataFrame = DataPayload
EndOfUpstream = DataPayload
