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
from pyiceberg import types as iceberg_types
from pyiceberg.schema import Schema as IcebergSchema
from core.models import Schema, Tuple
from core.models.schema.attribute_type import AttributeType
from core.models.type.large_binary import largebinary
from core.storage.iceberg.iceberg_utils import (
    encode_large_binary_field_name,
    decode_large_binary_field_name,
    iceberg_schema_to_amber_schema,
    amber_schema_to_iceberg_schema,
    amber_tuples_to_arrow_table,
    arrow_table_to_amber_tuples,
)


class TestIcebergUtilsLargeBinary:
    def test_encode_large_binary_field_name(self):
        """Test encoding LARGE_BINARY field names with suffix."""
        assert (
            encode_large_binary_field_name("my_field", AttributeType.LARGE_BINARY)
            == "my_field__texera_large_binary_ptr"
        )
        assert (
            encode_large_binary_field_name("my_field", AttributeType.STRING)
            == "my_field"
        )

    def test_decode_large_binary_field_name(self):
        """Test decoding LARGE_BINARY field names by removing suffix."""
        assert (
            decode_large_binary_field_name("my_field__texera_large_binary_ptr")
            == "my_field"
        )
        assert decode_large_binary_field_name("my_field") == "my_field"
        assert decode_large_binary_field_name("regular_field") == "regular_field"

    def test_amber_schema_to_iceberg_schema_with_large_binary(self):
        """Test converting Amber schema with LARGE_BINARY to Iceberg schema."""
        amber_schema = Schema()
        amber_schema.add("regular_field", AttributeType.STRING)
        amber_schema.add("large_binary_field", AttributeType.LARGE_BINARY)
        amber_schema.add("int_field", AttributeType.INT)

        iceberg_schema = amber_schema_to_iceberg_schema(amber_schema)

        # Check field names are encoded
        field_names = [field.name for field in iceberg_schema.fields]
        assert "regular_field" in field_names
        assert "large_binary_field__texera_large_binary_ptr" in field_names
        assert "int_field" in field_names

        # Check types
        large_binary_field = next(
            f for f in iceberg_schema.fields if "large_binary" in f.name
        )
        assert isinstance(large_binary_field.field_type, iceberg_types.StringType)

    def test_iceberg_schema_to_amber_schema_with_large_binary(self):
        """Test converting Iceberg schema with LARGE_BINARY to Amber schema."""
        iceberg_schema = IcebergSchema(
            iceberg_types.NestedField(
                1, "regular_field", iceberg_types.StringType(), required=False
            ),
            iceberg_types.NestedField(
                2,
                "large_binary_field__texera_large_binary_ptr",
                iceberg_types.StringType(),
                required=False,
            ),
            iceberg_types.NestedField(
                3, "int_field", iceberg_types.IntegerType(), required=False
            ),
        )

        amber_schema = iceberg_schema_to_amber_schema(iceberg_schema)

        assert amber_schema.get_attr_type("regular_field") == AttributeType.STRING
        assert (
            amber_schema.get_attr_type("large_binary_field")
            == AttributeType.LARGE_BINARY
        )
        assert amber_schema.get_attr_type("int_field") == AttributeType.INT

        # Check Arrow schema has metadata for LARGE_BINARY
        arrow_schema = amber_schema.as_arrow_schema()
        large_binary_field = arrow_schema.field("large_binary_field")
        assert large_binary_field.metadata is not None
        assert large_binary_field.metadata.get(b"texera_type") == b"LARGE_BINARY"

    def test_amber_tuples_to_arrow_table_with_large_binary(self):
        """Test converting Amber tuples with largebinary to Arrow table."""
        amber_schema = Schema()
        amber_schema.add("regular_field", AttributeType.STRING)
        amber_schema.add("large_binary_field", AttributeType.LARGE_BINARY)

        large_binary1 = largebinary("s3://bucket/path1")
        large_binary2 = largebinary("s3://bucket/path2")

        tuples = [
            Tuple(
                {"regular_field": "value1", "large_binary_field": large_binary1},
                schema=amber_schema,
            ),
            Tuple(
                {"regular_field": "value2", "large_binary_field": large_binary2},
                schema=amber_schema,
            ),
        ]

        iceberg_schema = amber_schema_to_iceberg_schema(amber_schema)
        arrow_table = amber_tuples_to_arrow_table(iceberg_schema, tuples)

        # Check that largebinary values are converted to URI strings
        regular_values = arrow_table.column("regular_field").to_pylist()
        large_binary_values = arrow_table.column(
            "large_binary_field__texera_large_binary_ptr"
        ).to_pylist()

        assert regular_values == ["value1", "value2"]
        assert large_binary_values == ["s3://bucket/path1", "s3://bucket/path2"]

    def test_arrow_table_to_amber_tuples_with_large_binary(self):
        """Test converting Arrow table with LARGE_BINARY to Amber tuples."""
        # Create Iceberg schema with encoded field name
        iceberg_schema = IcebergSchema(
            iceberg_types.NestedField(
                1, "regular_field", iceberg_types.StringType(), required=False
            ),
            iceberg_types.NestedField(
                2,
                "large_binary_field__texera_large_binary_ptr",
                iceberg_types.StringType(),
                required=False,
            ),
        )

        # Create Arrow table with URI strings
        arrow_table = pa.Table.from_pydict(
            {
                "regular_field": ["value1", "value2"],
                "large_binary_field__texera_large_binary_ptr": [
                    "s3://bucket/path1",
                    "s3://bucket/path2",
                ],
            }
        )

        tuples = list(arrow_table_to_amber_tuples(iceberg_schema, arrow_table))

        assert len(tuples) == 2
        assert tuples[0]["regular_field"] == "value1"
        assert isinstance(tuples[0]["large_binary_field"], largebinary)
        assert tuples[0]["large_binary_field"].uri == "s3://bucket/path1"

        assert tuples[1]["regular_field"] == "value2"
        assert isinstance(tuples[1]["large_binary_field"], largebinary)
        assert tuples[1]["large_binary_field"].uri == "s3://bucket/path2"

    def test_round_trip_large_binary_tuples(self):
        """Test round-trip conversion of tuples with largebinary."""
        amber_schema = Schema()
        amber_schema.add("regular_field", AttributeType.STRING)
        amber_schema.add("large_binary_field", AttributeType.LARGE_BINARY)

        large_binary = largebinary("s3://bucket/path/to/object")
        original_tuples = [
            Tuple(
                {"regular_field": "value1", "large_binary_field": large_binary},
                schema=amber_schema,
            ),
        ]

        # Convert to Iceberg and Arrow
        iceberg_schema = amber_schema_to_iceberg_schema(amber_schema)
        arrow_table = amber_tuples_to_arrow_table(iceberg_schema, original_tuples)

        # Convert back to Amber tuples
        retrieved_tuples = list(
            arrow_table_to_amber_tuples(iceberg_schema, arrow_table)
        )

        assert len(retrieved_tuples) == 1
        assert retrieved_tuples[0]["regular_field"] == "value1"
        assert isinstance(retrieved_tuples[0]["large_binary_field"], largebinary)
        assert retrieved_tuples[0]["large_binary_field"].uri == large_binary.uri

    def test_arrow_table_to_amber_tuples_with_null_large_binary(self):
        """Test converting Arrow table with null largebinary values."""
        iceberg_schema = IcebergSchema(
            iceberg_types.NestedField(
                1, "regular_field", iceberg_types.StringType(), required=False
            ),
            iceberg_types.NestedField(
                2,
                "large_binary_field__texera_large_binary_ptr",
                iceberg_types.StringType(),
                required=False,
            ),
        )

        arrow_table = pa.Table.from_pydict(
            {
                "regular_field": ["value1"],
                "large_binary_field__texera_large_binary_ptr": [None],
            }
        )

        tuples = list(arrow_table_to_amber_tuples(iceberg_schema, arrow_table))

        assert len(tuples) == 1
        assert tuples[0]["regular_field"] == "value1"
        assert tuples[0]["large_binary_field"] is None
