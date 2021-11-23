import pandas
from typing import TypeVar, List

from core.models import TupleLike

TableLike = TypeVar('TableLike', pandas.DataFrame, List[TupleLike])


class Table(pandas.DataFrame):
    pass
