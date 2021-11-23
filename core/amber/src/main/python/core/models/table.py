from typing import List, TypeVar

import pandas

from . import TupleLike

TableLike = TypeVar('TableLike', pandas.DataFrame, pandas.DataFrame)


class Table(pandas.DataFrame):
    def __init__(self, table_like: TableLike):
        super().__init__(table_like)
