# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import datetime
import pandas
import pyarrow
import pytest
import numpy as np
from copy import deepcopy
from loguru import logger

from core.models import Table, Tuple, ArrowTableTupleProvider
from core.models.schema.schema import Schema


class TestTuple:
    @pytest.fixture
    def target_tuple(self):
        return Tuple({"x": 1, "y": "a"})

    def test_tuple_from_list(self, target_tuple):
        assert Tuple([("x", 1), ("y", "a")]) == target_tuple

    def test_tuple_from_dict(self, target_tuple):
        assert Tuple({"x": 1, "y": "a"}) == target_tuple

    def test_tuple_from_series(self, target_tuple):
        assert Tuple(pandas.Series({"x": 1, "y": "a"})) == target_tuple

    def test_tuple_as_key_value_pairs(self, target_tuple):
        assert target_tuple.as_key_value_pairs() == [("x", 1), ("y", "a")]

    def test_tuple_as_dict(self, target_tuple):
        assert target_tuple.as_dict() == {"x": 1, "y": "a"}

    def test_tuple_as_dict_deepcopy_isolates_values(self, target_tuple):
        d = target_tuple.as_dict(deepcopy=True)
        d["scores"] = [1, 2]
        d["scores"].append(3)
        # mutating the deep-copied dict must not affect the tuple
        assert "scores" not in target_tuple.as_dict()

    def test_tuple_as_dict_is_shallow_by_default(self, target_tuple):
        d = target_tuple.as_dict()
        assert d == {"x": 1, "y": "a"}
        # reassigning keys on the copy must not leak back into the tuple
        d["x"] = 999
        del d["y"]
        assert target_tuple.as_dict() == {"x": 1, "y": "a"}

    def test_tuple_as_series(self, target_tuple):
        assert (target_tuple.as_series() == pandas.Series({"x": 1, "y": "a"})).all()

    def test_tuple_get_fields(self, target_tuple):
        assert target_tuple.get_fields() == (1, "a")

    def test_tuple_get_field_names(self, target_tuple):
        assert target_tuple.get_field_names() == ("x", "y")

    def test_tuple_get_item(self, target_tuple):
        assert target_tuple["x"] == 1
        assert target_tuple["y"] == "a"
        assert target_tuple[0] == 1
        assert target_tuple[1] == "a"

    def test_tuple_set_item(self, target_tuple):
        target_tuple["x"] = 3
        assert target_tuple["x"] == 3
        assert target_tuple["y"] == "a"
        assert target_tuple[0] == 3
        assert target_tuple[1] == "a"
        target_tuple["z"] = 1.1
        assert target_tuple[2] == 1.1
        assert target_tuple["z"] == 1.1

    def test_tuple_str(self, target_tuple):
        assert str(target_tuple) == "Tuple['x': 1, 'y': 'a']"

    def test_tuple_repr(self, target_tuple):
        assert repr(target_tuple) == "Tuple['x': 1, 'y': 'a']"

    def test_tuple_eq(self, target_tuple):
        assert target_tuple == target_tuple
        assert not Tuple({"x": 2, "y": "a"}) == target_tuple

    def test_tuple_ne(self, target_tuple):
        assert not target_tuple != target_tuple
        assert Tuple({"x": 1, "y": "b"}) != target_tuple

    def test_reject_empty_tuplelike(self):
        with pytest.raises(AssertionError):
            Tuple([])
        with pytest.raises(AssertionError):
            Tuple({})
        with pytest.raises(AssertionError):
            Tuple(pandas.Series(dtype=pandas.StringDtype()))

    def test_reject_invalid_tuplelike(self):
        with pytest.raises(TypeError):
            Tuple(1)
        with pytest.raises(TypeError):
            Tuple([1])
        with pytest.raises(TypeError):
            Tuple([None])

    def test_tuple_lazy_get_from_arrow(self):
        def field_accessor(field_name):
            return chr(96 + int(field_name))

        chr_tuple = Tuple({"1": "a", "3": "c"})
        tuple_ = Tuple({"1": field_accessor, "3": field_accessor})
        assert tuple_ == Tuple({"1": "a", "3": "c"})
        tuple_ = Tuple({"1": field_accessor, "3": field_accessor})
        assert deepcopy(tuple_) == chr_tuple

    def test_retrieve_tuple_from_empty_arrow_table(self):
        arrow_schema = pyarrow.schema([])
        arrow_table = arrow_schema.empty_table()
        tuple_provider = ArrowTableTupleProvider(arrow_table)
        tuples = [
            Tuple({name: field_accessor for name in arrow_table.column_names})
            for field_accessor in tuple_provider
        ]
        assert tuples == []

    def test_finalize_tuple(self):
        tuple_ = Tuple(
            {"name": "texera", "age": 21, "scores": [85, 94, 100], "height": np.nan}
        )
        schema = Schema(
            raw_schema={
                "name": "STRING",
                "age": "INTEGER",
                "scores": "BINARY",
                "height": "DOUBLE",
            }
        )
        tuple_.finalize(schema)
        assert isinstance(tuple_["scores"], bytes)
        assert tuple_["height"] is None

    # Pandas-based operators (e.g. TableOperator via Table.from_tuple_likes)
    # promote an int column containing nulls to float64, so an INT field can
    # arrive at finalize() as 119.0. finalize() must coerce such integral
    # floats back to int when they fit the target type's range, while still
    # rejecting non-integral, infinite, and out-of-range floats.

    @pytest.mark.parametrize(
        "raw_value, expected",
        [
            (119.0, 119),
            (-3.0, -3),
            (-0.0, 0),
            # int32 boundaries are exactly representable as float64
            (2147483647.0, 2**31 - 1),
            (-2147483648.0, -(2**31)),
            # np.float64 subclasses float and must be coerced the same way
            (np.float64(119.0), 119),
        ],
    )
    def test_finalize_coerces_integral_float_to_int(self, raw_value, expected):
        tuple_ = Tuple({"weight": raw_value})
        tuple_.finalize(Schema(raw_schema={"weight": "INTEGER"}))
        assert tuple_["weight"] == expected
        assert type(tuple_["weight"]) is int

    @pytest.mark.parametrize(
        "raw_value, expected",
        [
            (3000000000.0, 3000000000),
            # np.float64 subclasses float and must be coerced the same way
            (np.float64(3000000000.0), 3000000000),
            # boundaries of the float64 exact-integer window: every integer
            # in [-(2**53) + 1, 2**53 - 1] maps to a unique float64
            (float(2**53 - 1), 2**53 - 1),
            (float(-(2**53) + 1), -(2**53) + 1),
        ],
    )
    def test_finalize_coerces_integral_float_to_long(self, raw_value, expected):
        tuple_ = Tuple({"count": raw_value})
        tuple_.finalize(Schema(raw_schema={"count": "LONG"}))
        assert tuple_["count"] == expected
        assert type(tuple_["count"]) is int

    def test_finalize_tuples_from_pandas_promoted_int_column(self):
        # Mirrors the real pipeline: pandas promotes the INT column to
        # float64 inside Table.from_tuple_likes because of the null row
        # (119 -> 119.0, None -> NaN). finalize() must restore the int
        # and map NaN back to None.
        table = Table([{"weight": 119}, {"weight": None}])
        assert table["weight"].dtype == "float64"
        schema = Schema(raw_schema={"weight": "INTEGER"})
        finalized = []
        for tuple_ in table.as_tuples():
            tuple_.finalize(schema)
            finalized.append(tuple_)
        assert finalized[0]["weight"] == 119
        assert type(finalized[0]["weight"]) is int
        assert finalized[1]["weight"] is None

    @pytest.mark.parametrize(
        "null_value",
        [None, float("nan"), np.float64("nan")],
        ids=["none", "nan", "np-nan"],
    )
    def test_finalize_keeps_null_int_field_as_none(self, null_value):
        tuple_ = Tuple({"weight": null_value})
        tuple_.finalize(Schema(raw_schema={"weight": "INTEGER"}))
        assert tuple_["weight"] is None

    @pytest.mark.parametrize(
        "attr_type, raw_value",
        [
            # non-integral floats must never be silently truncated
            ("INTEGER", 119.5),
            ("LONG", 3.5),
            ("INTEGER", float("inf")),
            ("INTEGER", float("-inf")),
            # integral floats outside the target range must not be coerced
            # into ints that would overflow Arrow int32
            ("INTEGER", 3e9),
            ("INTEGER", 2147483648.0),  # int32 max + 1
            ("INTEGER", -2147483649.0),  # int32 min - 1
            # for LONG, floats beyond the float64 exact-integer window must
            # be rejected even though they fit int64: float64 rounds above
            # 2**53, so the received float may already be a corrupted
            # rendition of the original integer. The endpoint 2**53 itself
            # is ambiguous (2**53 + 1 also rounds to float 2**53).
            ("LONG", float(2**53)),
            ("LONG", float(-(2**53))),
            ("LONG", -9223372036854775808.0),  # -(2**63), fits int64
            ("LONG", 9223372036854775808.0),  # 2**63, above long max
            ("LONG", 1e20),
            # coercion only applies to INT/LONG targets
            ("STRING", 119.0),
        ],
    )
    def test_finalize_rejects_uncoercible_float(self, attr_type, raw_value):
        tuple_ = Tuple({"weight": raw_value})
        with pytest.raises(TypeError, match="Unmatched type"):
            tuple_.finalize(Schema(raw_schema={"weight": attr_type}))

    def test_cast_to_schema_warns_on_out_of_range_integral_float(self):
        # An integral float outside the coercible window must be left
        # unchanged, and a guidance warning emitted: the follow-up
        # validation error alone would not explain the pandas float64
        # promotion or how to work around it.
        messages = []
        handler_id = logger.add(messages.append, level="WARNING")
        try:
            tuple_ = Tuple({"weight": 3e9})
            tuple_.cast_to_schema(Schema(raw_schema={"weight": "INTEGER"}))
        finally:
            logger.remove(handler_id)
        assert tuple_["weight"] == 3e9
        assert type(tuple_["weight"]) is float
        assert any("outside the safely coercible range" in str(m) for m in messages)

    @pytest.mark.parametrize("raw_value", [0.5, 2.0])
    def test_finalize_leaves_double_field_untouched(self, raw_value):
        tuple_ = Tuple({"ratio": raw_value})
        tuple_.finalize(Schema(raw_schema={"ratio": "DOUBLE"}))
        assert tuple_["ratio"] == raw_value
        assert type(tuple_["ratio"]) is float

    def test_finalize_keeps_plain_int_unchanged(self):
        tuple_ = Tuple({"weight": 119})
        tuple_.finalize(Schema(raw_schema={"weight": "INTEGER"}))
        assert tuple_["weight"] == 119
        assert type(tuple_["weight"]) is int

    def test_cast_to_schema_coerces_integral_float(self):
        # The coercion must live in cast_to_schema(), not validate_schema()
        tuple_ = Tuple({"weight": 119.0})
        tuple_.cast_to_schema(Schema(raw_schema={"weight": "INTEGER"}))
        assert tuple_["weight"] == 119
        assert type(tuple_["weight"]) is int

    def test_validate_schema_still_rejects_integral_float(self):
        # validate_schema() alone must stay strict: coercing there instead
        # of in cast_to_schema() would let unfinalized floats slip through
        tuple_ = Tuple({"weight": 119.0})
        with pytest.raises(TypeError, match="Unmatched type"):
            tuple_.validate_schema(Schema(raw_schema={"weight": "INTEGER"}))

    def test_finalize_maps_nan_in_binary_field_to_none(self):
        # NaN in a BINARY field must become None, not a pickled NaN.
        # Guards the cast_to_schema() branch structure: after the NaN->None
        # conversion, the stale pre-conversion value must not be re-pickled.
        tuple_ = Tuple({"payload": float("nan")})
        tuple_.finalize(Schema(raw_schema={"payload": "BINARY"}))
        assert tuple_["payload"] is None

    def test_hash(self):
        schema = Schema(
            raw_schema={
                "col-int": "INTEGER",
                "col-string": "STRING",
                "col-bool": "BOOLEAN",
                "col-long": "LONG",
                "col-double": "DOUBLE",
                "col-timestamp": "TIMESTAMP",
                "col-binary": "BINARY",
            }
        )

        tuple_ = Tuple(
            {
                "col-int": 922323,
                "col-string": "string-attr",
                "col-bool": True,
                "col-long": 1123213213213,
                "col-double": 214214.9969346,
                "col-timestamp": datetime.datetime.fromtimestamp(100000000),
                "col-binary": b"hello",
            },
            schema,
        )
        assert hash(tuple_) == -1335416166  # calculated with Java

        tuple2 = Tuple(
            {
                "col-int": 0,
                "col-string": "",
                "col-bool": False,
                "col-long": 0,
                "col-double": 0.0,
                "col-timestamp": datetime.datetime.fromtimestamp(0),
                "col-binary": b"",
            },
            schema,
        )

        assert hash(tuple2) == -1409761483  # calculated with Java

        tuple3 = Tuple(
            {
                "col-int": None,
                "col-string": None,
                "col-bool": None,
                "col-long": None,
                "col-double": None,
                "col-timestamp": None,
                "col-binary": None,
            },
            schema,
        )

        assert hash(tuple3) == 1742810335  # calculated with Java

        tuple4 = Tuple(
            {
                "col-int": -3245763,
                "col-string": "\n\r\napple",
                "col-bool": True,
                "col-long": -8965536434247,
                "col-double": 1 / 3,
                "col-timestamp": datetime.datetime.fromtimestamp(-1990),
                "col-binary": None,
            },
            schema,
        )
        assert hash(tuple4) == -592643630  # calculated with Java

        tuple5 = Tuple(
            {
                "col-int": 0x7FFFFFFF,
                "col-string": "",
                "col-bool": True,
                "col-long": 0x7FFFFFFFFFFFFFFF,
                "col-double": 7 / 17,
                "col-timestamp": datetime.datetime.fromtimestamp(1234567890),
                "col-binary": b"o" * 4097,
            },
            schema,
        )
        assert hash(tuple5) == -2099556631  # calculated with Java

    def test_tuple_with_large_binary(self):
        """Test tuple with largebinary field."""
        from core.models.type.large_binary import largebinary

        schema = Schema(
            raw_schema={
                "regular_field": "STRING",
                "large_binary_field": "LARGE_BINARY",
            }
        )

        large_binary = largebinary("s3://test-bucket/path/to/object")
        tuple_ = Tuple(
            {
                "regular_field": "test string",
                "large_binary_field": large_binary,
            },
            schema=schema,
        )

        assert tuple_["regular_field"] == "test string"
        assert tuple_["large_binary_field"] == large_binary
        assert isinstance(tuple_["large_binary_field"], largebinary)
        assert tuple_["large_binary_field"].uri == "s3://test-bucket/path/to/object"

    def test_tuple_from_arrow_with_large_binary(self):
        """Test creating tuple from Arrow table with LARGE_BINARY metadata."""
        import pyarrow as pa
        from core.models.type.large_binary import largebinary

        # Create Arrow schema with LARGE_BINARY metadata
        arrow_schema = pa.schema(
            [
                pa.field("regular_field", pa.string()),
                pa.field(
                    "large_binary_field",
                    pa.string(),
                    metadata={b"texera_type": b"LARGE_BINARY"},
                ),
            ]
        )

        # Create Arrow table with URI string for large_binary_field
        arrow_table = pa.Table.from_pydict(
            {
                "regular_field": ["test"],
                "large_binary_field": ["s3://test-bucket/path/to/object"],
            },
            schema=arrow_schema,
        )

        # Create tuple from Arrow table
        tuple_provider = ArrowTableTupleProvider(arrow_table)
        tuples = [
            Tuple({name: field_accessor for name in arrow_table.column_names})
            for field_accessor in tuple_provider
        ]

        assert len(tuples) == 1
        tuple_ = tuples[0]
        assert tuple_["regular_field"] == "test"
        assert isinstance(tuple_["large_binary_field"], largebinary)
        assert tuple_["large_binary_field"].uri == "s3://test-bucket/path/to/object"

    def test_tuple_with_null_large_binary(self):
        """Test tuple with null largebinary field."""
        import pyarrow as pa

        # Create Arrow schema with LARGE_BINARY metadata
        arrow_schema = pa.schema(
            [
                pa.field(
                    "large_binary_field",
                    pa.string(),
                    metadata={b"texera_type": b"LARGE_BINARY"},
                ),
            ]
        )

        # Create Arrow table with null value
        arrow_table = pa.Table.from_pydict(
            {
                "large_binary_field": [None],
            },
            schema=arrow_schema,
        )

        # Create tuple from Arrow table
        tuple_provider = ArrowTableTupleProvider(arrow_table)
        tuples = [
            Tuple({name: field_accessor for name in arrow_table.column_names})
            for field_accessor in tuple_provider
        ]

        assert len(tuples) == 1
        tuple_ = tuples[0]
        assert tuple_["large_binary_field"] is None
