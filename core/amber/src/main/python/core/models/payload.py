from dataclasses import dataclass
from pyarrow.lib import Table
from typing import Optional

@dataclass
class DataPayload:
    frame: Optional[Table] = None

@dataclass
class DataFrame(DataPayload):
    pass

@dataclass
class EndOfUpstream(DataPayload):
    pass
