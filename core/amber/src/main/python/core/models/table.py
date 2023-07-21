from typing import Sized, Container, Iterator
from typing_extensions import Protocol
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
        if isinstance(table_like, Table):
            self.__data_frame = table_like.as_dataframe()
        elif isinstance(table_like, pandas.DataFrame):
            self.__data_frame = table_like
        elif isinstance(table_like, list):
            # only supports List[TupleLike] now.
            self.column_names = None

            def iterate_tuples(tuple_like_iter):
                for tuple_like in tuple_like_iter:
                    tuple_ = Tuple(tuple_like)
                    field_names = tuple_.get_field_names()

                    if self.column_names is not None:
                        assert field_names == self.column_names
                    self.column_names = field_names

                    yield tuple_.get_fields()

            self.__data_frame = pandas.DataFrame.from_records(
                list(iterate_tuples(table_like)), columns=self.column_names
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
        for t in self.__data_frame.itertuples(index=False, name=None):
            yield Tuple(dict(zip(self.__data_frame.columns, t)))

    def as_dataframe(self) -> pandas.DataFrame:
        """
        Return a deepcopy of the internal pandas.DataFrame.
        :return:
        """
        return self.__data_frame.__deepcopy__()

    def __eq__(self, other: "Table") -> bool:
        return all(a == b for a, b in zip(self.as_tuples(), other.as_tuples()))
