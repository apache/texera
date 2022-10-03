from typing import TypeVar

import pandas

TableLike = TypeVar("TableLike", pandas.DataFrame, pandas.DataFrame)


class Table(pandas.DataFrame):
    def __init__(self, table_like: TableLike):
        super().__init__(table_like)
