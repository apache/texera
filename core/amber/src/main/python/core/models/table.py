from typing import List, TypeVar

import pandas

from . import TupleLike

TableLike = TypeVar('TableLike', pandas.DataFrame, List[TupleLike])


class Table(pandas.DataFrame):
    pass
