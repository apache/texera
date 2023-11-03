from typing import TypeVar

import pandas

BatchLike = TypeVar("BatchLike", pandas.DataFrame, pandas.DataFrame)


class Batch(pandas.DataFrame):
    def __init__(self, batch_like: BatchLike):
        super().__init__(batch_like)
