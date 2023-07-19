from typing import Protocol, Sized, Container, Iterator

import pandas

from core.models import Tuple


class TableLike(
    Protocol,
    Sized,
    Container,
):
    def __getitem__(self, item):
        ...

    def __setitem__(self, key, value):
        ...


class Table(TableLike):
    def __init__(self, table_like: TableLike):
        if isinstance(table_like, pandas.DataFrame):
            self.__data_frame = table_like
        elif isinstance(table_like, list):
            self.__data_frame = pandas.DataFrame.from_records(
                (i.as_dict() for i in table_like)
            )
        else:
            raise TypeError(f"unsupported table_like type :{type(table_like)}")

    def __getattr__(self, item):
        return self.__data_frame.__getattr__(item)

    def __contains__(self, item) -> bool:
        return self.__data_frame.__contains__(item)

    def __len__(self) -> int:
        return self.__data_frame.__len__()

    def __str__(self) -> str:
        return f"Table[{self.__data_frame}]"

    def __getitem__(self, item):
        return self.__data_frame.__getitem__(item)

    def __setitem__(self, key, value) -> None:
        return self.__data_frame.__setitem__(key, value)

    def as_tuples(self) -> Iterator[Tuple]:
        """
        Convert rows of the table into Tuples, and returning an iterator of Tuples
        following their row index order.
        :return:
        """
        for t in self.__data_frame.itertuples(index=False):
            # TODO: Converting to NamedTuple will cause the field names be renamed to
            #   valid Python identifiers.
            yield Tuple(t._asdict())

    def as_dataframe(self) -> pandas.DataFrame:
        """
        Return a deepcopy of the internal pandas.DataFrame.
        :return:
        """
        return self.__data_frame.__deepcopy__()
