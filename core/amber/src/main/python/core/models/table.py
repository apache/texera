from typing import Sized, Container, Iterator

from typing_extensions import Protocol
import pandas

from core.models import Tuple, TupleLike


class TableLike(
    Protocol,
    Sized,
    Container,
):
    def __getitem__(self, item):
        ...

    def __setitem__(self, key, value):
        ...


class Table(pandas.DataFrame):
    @staticmethod
    def from_table(table):
        return table

    @staticmethod
    def from_data_frame(df):
        return df

    @staticmethod
    def from_tuple_likes(tuple_likes: Iterator[TupleLike]):
        column_names = None
        records = []
        for tuple_like in tuple_likes:
            tuple_ = Tuple(tuple_like)
            field_names = tuple_.get_field_names()

            if column_names is not None:
                assert field_names == column_names
            else:
                column_names = field_names

            records.append(tuple_.get_fields())

        return pandas.DataFrame.from_records(records, columns=column_names)

    def __init__(self, table_like):
        df: pandas.DataFrame

        if isinstance(table_like, Table):
            df = self.from_table(table_like)
        if isinstance(table_like, pandas.DataFrame):
            df = self.from_data_frame(table_like)
        elif isinstance(table_like, list):
            # only supports List[TupleLike]
            df = self.from_tuple_likes(table_like)
        super().__init__(df)

    def as_tuples(self) -> Iterator[Tuple]:
        """
        Convert rows of the table into Tuples, and returning an iterator of Tuples
        following their row index order.
        :return:
        """
        for raw_tuple in self.itertuples(index=False, name=None):
            yield Tuple(dict(zip(self.columns, raw_tuple)))

    def __eq__(self, other: "Table") -> bool:
        if isinstance(other, Table):
            return all(a == b for a, b in zip(self.as_tuples(), other.as_tuples()))
        else:
            return super().__eq__(other).all()
