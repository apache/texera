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

import pyarrow as pa
import pytest

from core.models.schema.attribute_type import AttributeType
from core.models.schema.schema import Schema


class TestSchema:
    @pytest.fixture
    def raw_schema(self):
        return {
            "field-1": "STRING",
            "field-2": "INTEGER",
            "field-3": "LONG",
            "field-4": "DOUBLE",
            "field-5": "BOOLEAN",
            "field-6": "TIMESTAMP",
            "field-7": "BINARY",
        }

    @pytest.fixture
    def arrow_schema(self):
        return pa.schema(
            [
                pa.field("field-1", pa.string()),
                pa.field("field-2", pa.int32()),
                pa.field("field-3", pa.int64()),
                pa.field("field-4", pa.float64()),
                pa.field("field-5", pa.bool_()),
                pa.field("field-6", pa.timestamp("us")),
                pa.field("field-7", pa.binary()),
            ]
        )

    @pytest.fixture
    def schema(self):
        s = Schema()
        s.add("field-1", AttributeType.STRING)
        s.add("field-2", AttributeType.INT)
        s.add("field-3", AttributeType.LONG)
        s.add("field-4", AttributeType.DOUBLE)
        s.add("field-5", AttributeType.BOOL)
        s.add("field-6", AttributeType.TIMESTAMP)
        s.add("field-7", AttributeType.BINARY)
        return s

    def test_accessors_and_mutators(self, schema):
        assert schema.get_attr_names() == [f"field-{i}" for i in range(1, 8)]
        assert schema.get_attr_type("field-2") == AttributeType.INT
        assert schema.get_attr_type("field-6") == AttributeType.TIMESTAMP
        assert schema.as_key_value_pairs() == [
            ("field-1", AttributeType.STRING),
            ("field-2", AttributeType.INT),
            ("field-3", AttributeType.LONG),
            ("field-4", AttributeType.DOUBLE),
            ("field-5", AttributeType.BOOL),
            ("field-6", AttributeType.TIMESTAMP),
            ("field-7", AttributeType.BINARY),
        ]
        with pytest.raises(KeyError):
            schema.get_attr_type("does not exist")
        with pytest.raises(TypeError):
            schema["illegal_assign"] = "value"
        with pytest.raises(TypeError):
            _ = schema["illegal_access"]
        with pytest.raises(KeyError):
            schema.add("field-2", AttributeType.LONG)

    def test_convert_from_raw_schema(self, raw_schema, schema):
        assert schema == Schema(raw_schema=raw_schema)

    def test_convert_from_arrow_schema(self, arrow_schema, schema):
        assert schema == Schema(arrow_schema=arrow_schema)
        assert schema.as_arrow_schema() == arrow_schema

    def test_large_binary_in_raw_schema(self):
        """Test creating schema with LARGE_BINARY from raw schema."""
        raw_schema = {
            "regular_field": "STRING",
            "large_binary_field": "LARGE_BINARY",
        }
        schema = Schema(raw_schema=raw_schema)
        assert schema.get_attr_type("regular_field") == AttributeType.STRING
        assert schema.get_attr_type("large_binary_field") == AttributeType.LARGE_BINARY

    def test_large_binary_in_arrow_schema_with_metadata(self):
        """Test creating schema with LARGE_BINARY from Arrow schema with metadata."""
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
        schema = Schema(arrow_schema=arrow_schema)
        assert schema.get_attr_type("regular_field") == AttributeType.STRING
        assert schema.get_attr_type("large_binary_field") == AttributeType.LARGE_BINARY

    def test_large_binary_as_arrow_schema_includes_metadata(self):
        """Test that LARGE_BINARY fields include metadata in Arrow schema."""
        schema = Schema()
        schema.add("regular_field", AttributeType.STRING)
        schema.add("large_binary_field", AttributeType.LARGE_BINARY)

        arrow_schema = schema.as_arrow_schema()

        # Regular field should have no metadata
        regular_field = arrow_schema.field("regular_field")
        assert (
            regular_field.metadata is None
            or b"texera_type" not in regular_field.metadata
        )

        # LARGE_BINARY field should have metadata
        large_binary_field = arrow_schema.field("large_binary_field")
        assert large_binary_field.metadata is not None
        assert large_binary_field.metadata.get(b"texera_type") == b"LARGE_BINARY"
        assert (
            large_binary_field.type == pa.string()
        )  # LARGE_BINARY is stored as string

    def test_round_trip_large_binary_schema(self):
        """Test round-trip conversion of schema with LARGE_BINARY."""
        original_schema = Schema()
        original_schema.add("field1", AttributeType.STRING)
        original_schema.add("field2", AttributeType.LARGE_BINARY)
        original_schema.add("field3", AttributeType.INT)

        # Convert to Arrow and back
        arrow_schema = original_schema.as_arrow_schema()
        round_trip_schema = Schema(arrow_schema=arrow_schema)

        assert round_trip_schema == original_schema
        assert round_trip_schema.get_attr_type("field1") == AttributeType.STRING
        assert round_trip_schema.get_attr_type("field2") == AttributeType.LARGE_BINARY
        assert round_trip_schema.get_attr_type("field3") == AttributeType.INT
