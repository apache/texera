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
from core.models.schema.big_object import BigObject
from core.storage.iceberg.iceberg_utils import (
    encode_big_object_field_name,
    decode_big_object_field_name,
    iceberg_schema_to_amber_schema,
    amber_schema_to_iceberg_schema,
    amber_tuples_to_arrow_table,
    arrow_table_to_amber_tuples,
)


class TestIcebergUtilsBigObject:
    def test_encode_big_object_field_name(self):
        """Test encoding BIG_OBJECT field names with suffix."""
        assert (
            encode_big_object_field_name("my_field", AttributeType.BIG_OBJECT)
            == "my_field__texera_big_obj_ptr"
        )
        assert (
            encode_big_object_field_name("my_field", AttributeType.STRING) == "my_field"
        )

    def test_decode_big_object_field_name(self):
        """Test decoding BIG_OBJECT field names by removing suffix."""
        assert (
            decode_big_object_field_name("my_field__texera_big_obj_ptr") == "my_field"
        )
        assert decode_big_object_field_name("my_field") == "my_field"
        assert decode_big_object_field_name("regular_field") == "regular_field"

    def test_amber_schema_to_iceberg_schema_with_big_object(self):
        """Test converting Amber schema with BIG_OBJECT to Iceberg schema."""
        amber_schema = Schema()
        amber_schema.add("regular_field", AttributeType.STRING)
        amber_schema.add("big_object_field", AttributeType.BIG_OBJECT)
        amber_schema.add("int_field", AttributeType.INT)

        iceberg_schema = amber_schema_to_iceberg_schema(amber_schema)

        # Check field names are encoded
        field_names = [field.name for field in iceberg_schema.fields]
        assert "regular_field" in field_names
        assert "big_object_field__texera_big_obj_ptr" in field_names
        assert "int_field" in field_names

        # Check types
        big_object_field = next(
            f for f in iceberg_schema.fields if "big_object" in f.name
        )
        assert isinstance(big_object_field.field_type, iceberg_types.StringType)

    def test_iceberg_schema_to_amber_schema_with_big_object(self):
        """Test converting Iceberg schema with BIG_OBJECT to Amber schema."""
        iceberg_schema = IcebergSchema(
            iceberg_types.NestedField(
                1, "regular_field", iceberg_types.StringType(), required=False
            ),
            iceberg_types.NestedField(
                2,
                "big_object_field__texera_big_obj_ptr",
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
            amber_schema.get_attr_type("big_object_field") == AttributeType.BIG_OBJECT
        )
        assert amber_schema.get_attr_type("int_field") == AttributeType.INT

        # Check Arrow schema has metadata for BIG_OBJECT
        arrow_schema = amber_schema.as_arrow_schema()
        big_object_field = arrow_schema.field("big_object_field")
        assert big_object_field.metadata is not None
        assert big_object_field.metadata.get(b"texera_type") == b"BIG_OBJECT"

    def test_amber_tuples_to_arrow_table_with_big_object(self):
        """Test converting Amber tuples with BigObject to Arrow table."""
        amber_schema = Schema()
        amber_schema.add("regular_field", AttributeType.STRING)
        amber_schema.add("big_object_field", AttributeType.BIG_OBJECT)

        big_object1 = BigObject("s3://bucket/path1")
        big_object2 = BigObject("s3://bucket/path2")

        tuples = [
            Tuple(
                {"regular_field": "value1", "big_object_field": big_object1},
                schema=amber_schema,
            ),
            Tuple(
                {"regular_field": "value2", "big_object_field": big_object2},
                schema=amber_schema,
            ),
        ]

        iceberg_schema = amber_schema_to_iceberg_schema(amber_schema)
        arrow_table = amber_tuples_to_arrow_table(iceberg_schema, tuples)

        # Check that BigObject values are converted to URI strings
        regular_values = arrow_table.column("regular_field").to_pylist()
        big_object_values = arrow_table.column(
            "big_object_field__texera_big_obj_ptr"
        ).to_pylist()

        assert regular_values == ["value1", "value2"]
        assert big_object_values == ["s3://bucket/path1", "s3://bucket/path2"]

    def test_arrow_table_to_amber_tuples_with_big_object(self):
        """Test converting Arrow table with BIG_OBJECT to Amber tuples."""
        # Create Iceberg schema with encoded field name
        iceberg_schema = IcebergSchema(
            iceberg_types.NestedField(
                1, "regular_field", iceberg_types.StringType(), required=False
            ),
            iceberg_types.NestedField(
                2,
                "big_object_field__texera_big_obj_ptr",
                iceberg_types.StringType(),
                required=False,
            ),
        )

        # Create Arrow table with URI strings
        arrow_table = pa.Table.from_pydict(
            {
                "regular_field": ["value1", "value2"],
                "big_object_field__texera_big_obj_ptr": [
                    "s3://bucket/path1",
                    "s3://bucket/path2",
                ],
            }
        )

        tuples = list(arrow_table_to_amber_tuples(iceberg_schema, arrow_table))

        assert len(tuples) == 2
        assert tuples[0]["regular_field"] == "value1"
        assert isinstance(tuples[0]["big_object_field"], BigObject)
        assert tuples[0]["big_object_field"].uri == "s3://bucket/path1"

        assert tuples[1]["regular_field"] == "value2"
        assert isinstance(tuples[1]["big_object_field"], BigObject)
        assert tuples[1]["big_object_field"].uri == "s3://bucket/path2"

    def test_round_trip_big_object_tuples(self):
        """Test round-trip conversion of tuples with BigObject."""
        amber_schema = Schema()
        amber_schema.add("regular_field", AttributeType.STRING)
        amber_schema.add("big_object_field", AttributeType.BIG_OBJECT)

        big_object = BigObject("s3://bucket/path/to/object")
        original_tuples = [
            Tuple(
                {"regular_field": "value1", "big_object_field": big_object},
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
        assert isinstance(retrieved_tuples[0]["big_object_field"], BigObject)
        assert retrieved_tuples[0]["big_object_field"].uri == big_object.uri

    def test_arrow_table_to_amber_tuples_with_null_big_object(self):
        """Test converting Arrow table with null BigObject values."""
        iceberg_schema = IcebergSchema(
            iceberg_types.NestedField(
                1, "regular_field", iceberg_types.StringType(), required=False
            ),
            iceberg_types.NestedField(
                2,
                "big_object_field__texera_big_obj_ptr",
                iceberg_types.StringType(),
                required=False,
            ),
        )

        arrow_table = pa.Table.from_pydict(
            {
                "regular_field": ["value1"],
                "big_object_field__texera_big_obj_ptr": [None],
            }
        )

        tuples = list(arrow_table_to_amber_tuples(iceberg_schema, arrow_table))

        assert len(tuples) == 1
        assert tuples[0]["regular_field"] == "value1"
        assert tuples[0]["big_object_field"] is None
