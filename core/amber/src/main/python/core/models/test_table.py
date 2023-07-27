import datetime
import pickle

import pandas
import pytest
from pandas import RangeIndex

from core.models import Table, Tuple


class TestTable:
    @pytest.fixture
    def a_timestamp(self):
        return datetime.datetime.now()

    @pytest.fixture
    def target_raw_tuples(self, a_timestamp):
        return [
            {
                "field1": 1,
                "field2": "hello",
                "field3": 2.3,
                "field4": True,
                "field5": a_timestamp,
                "field6": b"some binary",
                "7_special-name": None,
                "none": None,
            },
            {
                "field1": 2,
                "field2": "world",
                "field3": 0.0,
                "field4": False,
                "field5": datetime.datetime.fromtimestamp(1000000000),
                "field6": pickle.dumps([1, 2, 3]),
                "7_special-name": "a strange value",
                "none": None,
            },
        ]

    @pytest.fixture
    def target_tuples(self, target_raw_tuples):
        return [Tuple(raw_tuple) for raw_tuple in target_raw_tuples]

    @pytest.fixture
    def target_table(self, target_raw_tuples):
        return Table(target_raw_tuples)

    @pytest.fixture
    def target_data_frame(self, a_timestamp):
        return pandas.DataFrame(
            {
                "field1": [1, 2],
                "field2": ["hello", "world"],
                "field3": [2.3, 0.0],
                "field4": [True, False],
                "field5": [
                    a_timestamp,
                    datetime.datetime.fromtimestamp(1000000000),
                ],
                "field6": [b"some binary", pickle.dumps([1, 2, 3])],
                "7_special-name": [None, "a strange value"],
                "none": [None, None],
            },
            columns=[
                "field1",
                "field2",
                "field3",
                "field4",
                "field5",
                "field6",
                "7_special-name",
                "none",
            ],
        )

    def test_table_creation(self, target_table, a_timestamp):
        assert target_table["field1"][0] == 1
        assert target_table["field1"][1] == 2
        assert target_table["field2"][0] == "hello"
        assert target_table["field2"][1] == "world"
        assert target_table["field3"][0] == 2.3
        assert target_table["field3"][1] == 0.0
        assert target_table["field4"][0]
        assert not target_table["field4"][1]
        assert target_table["field5"][0] == a_timestamp
        assert target_table["field5"][1] == datetime.datetime.fromtimestamp(1000000000)
        assert target_table["field6"][0] == b"some binary"
        assert target_table["field6"][1] == pickle.dumps([1, 2, 3])
        assert target_table["7_special-name"][0] is None
        assert target_table["7_special-name"][1] == "a strange value"
        assert target_table["none"][0] is None
        assert target_table["none"][1] is None

    def test_as_tuples_preserve_types(self, target_table, target_tuples):
        assert list(target_table.as_tuples()) == target_tuples

    def test_table_from_data_frame(self, target_table, target_data_frame):
        assert Table(target_data_frame) == target_table

    def test_table_from_list_of_tuples(self, target_table, target_tuples):
        table = Table(target_tuples)
        assert table == target_table
        assert list(table.as_tuples()) == target_tuples

    def test_table_from_list_of_series(
        self, target_table, a_timestamp, target_raw_tuples, target_tuples
    ):
        table = Table([pandas.Series(raw_tuple) for raw_tuple in target_raw_tuples])

        assert table == target_table
        assert list(table.as_tuples()) == target_tuples

    def test_table_from_table(self, target_table, target_tuples):
        table = Table(target_table)
        assert table == target_table
        assert list(table.as_tuples()) == target_tuples

    def test_use_table_as_data_frame(self, target_table, target_data_frame):
        df = target_table
        assert (df.index == RangeIndex(start=0, stop=2, step=1)).all()
        concat_df = pandas.concat([df, df])
        assert len(concat_df) == 4
        assert target_table.equals(target_data_frame)

    def test_validation_of_schema(self):
        with pytest.raises(AssertionError):
            Table([{"text": "hello"}, {"book": "harry"}])
