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
import pytest
from copy import deepcopy
import pyarrow
from numpy import NaN

from core.models import Tuple, ArrowTableTupleProvider
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
            {"name": "texera", "age": 21, "scores": [85, 94, 100], "height": NaN}
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
        assert isinstance(tuple_["scores"], list)
        assert tuple_["height"] is None

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

    def test_binary_field_processing(self):
        """Test the processing of fields with BINARY type during schema finalization."""
        # Create a schema with a BINARY field
        schema = Schema(
            raw_schema={
                "bytes_list": "BINARY",
                "single_bytes": "BINARY",
                "regular_object": "BINARY",
                "large_bytes": "BINARY",
            }
        )

        # Create test data with different formats
        bytes_list = [b"hello", b"world"]
        single_bytes = b"single byte string"
        regular_object = {"key": "value", "numbers": [1, 2, 3]}
        large_bytes = b"x" * (1024 * 1024 + 100)

        # Create a tuple with these values
        tuple_ = Tuple(
            {
                "bytes_list": bytes_list,
                "single_bytes": single_bytes,
                "regular_object": regular_object,
                "large_bytes": large_bytes,
            }
        )

        # Finalize the tuple with the schema (this will trigger cast_to_schema)
        tuple_.finalize(schema)

        # Test case 1: Already a list of bytes objects
        assert isinstance(tuple_["bytes_list"], list)
        assert all(isinstance(item, bytes) for item in tuple_["bytes_list"])
        assert tuple_["bytes_list"] == [b"hello", b"world"]

        # Test case 2: Single bytes object converted to a list with one item
        assert isinstance(tuple_["single_bytes"], list)
        assert len(tuple_["single_bytes"]) == 1
        assert tuple_["single_bytes"][0] == b"single byte string"

        # Test case 3: Regular object pickled with pickle prefix
        assert isinstance(tuple_["regular_object"], list)
        assert len(tuple_["regular_object"]) == 1
        assert tuple_["regular_object"][0].startswith(b"pickle    ")

        # Test case 4: Large bytes object (but still under 1GB chunking threshold)
        assert isinstance(tuple_["large_bytes"], list)
        assert len(tuple_["large_bytes"]) == 1
        assert len(tuple_["large_bytes"][0]) == len(large_bytes)
        assert tuple_["large_bytes"][0] == large_bytes

    def test_binary_field_chunking(self):
        """Test that binary fields are properly chunked when exceeding chunk size."""
        # Create a test tuple
        test_tuple = Tuple({"test_field": b"test_data"})

        # Manually call _process_binary_field with a small chunk size (3 bytes)
        # This should split "test_data" into chunks: "tes", "t_d", "ata"
        test_tuple._process_binary_field("test_field", b"test_data", 3, b"pickle    ")

        # Verify chunking behavior
        assert isinstance(test_tuple["test_field"], list)
        assert len(test_tuple["test_field"]) == 3
        assert test_tuple["test_field"] == [b"tes", b"t_d", b"ata"]

        # Verify we can reconstruct the original data
        assert b"".join(test_tuple["test_field"]) == b"test_data"

        # Test with pickle prefix for non-bytes data
        test_tuple2 = Tuple({"complex_field": "complex_value"})
        test_tuple2._process_binary_field(
            "complex_field", "complex_value", 10, b"pickle    "
        )

        # Verify pickle behavior
        assert isinstance(test_tuple2["complex_field"], list)
        assert len(test_tuple2["complex_field"]) > 0
        assert test_tuple2["complex_field"][0].startswith(b"pickle    ")

    def test_schema_validation(self):
        """Test the schema validation logic for different field types."""
        # Create a schema with various field types
        schema = Schema(
            raw_schema={
                "int_field": "INTEGER",
                "string_field": "STRING",
                "binary_field": "BINARY",
                "nullable_field": "DOUBLE",
            }
        )

        # Test 1: Valid tuple that should pass validation
        valid_tuple = Tuple(
            {
                "int_field": 42,
                "string_field": "hello",
                "binary_field": [b"data1", b"data2"],
                "nullable_field": None,
            }
        )

        # This should not raise an exception
        valid_tuple.validate_schema(schema)

        # Test 2: Invalid type for non-binary field
        invalid_int_tuple = Tuple(
            {
                "int_field": "not an integer",  # Wrong type
                "string_field": "hello",
                "binary_field": [b"data"],
                "nullable_field": None,
            }
        )

        with pytest.raises(TypeError) as excinfo:
            invalid_int_tuple.validate_schema(schema)
        assert "Type mismatch for field 'int_field'" in str(excinfo.value)
        assert "AttributeType.INT" in str(excinfo.value)
        assert "got str instead" in str(excinfo.value)

        # Test 3: Invalid binary field - not a list
        invalid_binary_tuple = Tuple(
            {
                "int_field": 42,
                "string_field": "hello",
                "binary_field": b"not a list",  # Should be a list of bytes
                "nullable_field": None,
            }
        )

        with pytest.raises(TypeError) as excinfo:
            invalid_binary_tuple.validate_schema(schema)
        assert "Type mismatch for field 'binary_field'" in str(excinfo.value)
        assert "expected AttributeType.BINARY (list)" in str(excinfo.value)
        assert "got bytes instead" in str(excinfo.value)

        # Test 4: Invalid binary field - list contains non-bytes items
        invalid_binary_items_tuple = Tuple(
            {
                "int_field": 42,
                "string_field": "hello",
                "binary_field": [
                    b"valid",
                    "invalid string",
                    b"valid again",
                ],  # Contains a non-bytes item
                "nullable_field": None,
            }
        )

        with pytest.raises(TypeError) as excinfo:
            invalid_binary_items_tuple.validate_schema(schema)
        assert "Type mismatch in BINARY list field 'binary_field' at index 1" in str(
            excinfo.value
        )
        assert "expected 'bytes'" in str(excinfo.value)
        assert "got 'str'" in str(excinfo.value)

        # Test 5: Empty binary list should be valid
        empty_binary_tuple = Tuple(
            {
                "int_field": 42,
                "string_field": "hello",
                "binary_field": [],  # Empty list is valid for BINARY type
                "nullable_field": None,
            }
        )

        # This should not raise an exception
        empty_binary_tuple.validate_schema(schema)

    def test_schema_validation_with_complex_binary_data(self):
        """Test schema validation with more complex binary data structures."""
        schema = Schema(
            raw_schema={
                "binary_field": "BINARY",
            }
        )

        # Deeply nested list structure (still valid as long as all items are bytes)
        complex_tuple = Tuple({"binary_field": [b"chunk1", b"chunk2", b"chunk3" * 100]})

        # Should validate without error
        complex_tuple.validate_schema(schema)

        # Test with a very large list of binary data
        large_list_tuple = Tuple({"binary_field": [b"data"] * 1000})  # 1000 items

        # Should validate without error
        large_list_tuple.validate_schema(schema)

        # Test with non-ASCII bytes content
        binary_tuple = Tuple(
            {"binary_field": [bytes([255, 128, 64, 32, 16, 8, 4, 2, 1])]}
        )

        # Should validate without error
        binary_tuple.validate_schema(schema)
