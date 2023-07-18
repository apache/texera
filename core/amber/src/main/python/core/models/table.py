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
            self.__data_frame = pandas.DataFrame.from_records((i.as_dict() for i in
                                                               table_like))

    def __getattr__(self, item):
        return self.__data_frame.__getattr__(item)

    def __contains__(self, item):
        return self.__data_frame.__contains__(item)

    def __len__(self):
        return self.__data_frame.__len__()

    def __str__(self):
        return "Table " + self.__data_frame.__str__()

    def as_tuples(self) -> Iterator[Tuple]:
        for t in self.__data_frame.itertuples():
            d = t._asdict()
            del d['Index']
            yield Tuple(d)

    def as_dataframe(self):
        return self.__data_frame.__deepcopy__()

    def __getitem__(self, item):
        return self.__data_frame.__getitem__(item)

    def __setitem__(self, key, value):
        self.__data_frame.__setitem__(key, value)
