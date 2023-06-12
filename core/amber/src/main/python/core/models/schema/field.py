import datetime
from typing import TypeVar, Optional

Field = TypeVar(
    "Field",
    Optional[str],
    Optional[int],  # for both INT and LONG
    Optional[float],
    Optional[bool],
    Optional[datetime.datetime],
    Optional[bytes],
)
