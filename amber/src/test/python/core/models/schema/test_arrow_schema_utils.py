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

from collections import OrderedDict

import pyarrow as pa
import pytest

from core.models.schema.arrow_schema_utils import (
    arrow_schema_to_attr_types,
    attr_types_to_arrow_schema,
)
from core.models.schema.attribute_type import AttributeType
from core.models.schema.attribute_type_utils import (
    TEXERA_TYPE_METADATA_KEY,
    LARGE_BINARY_METADATA_VALUE,
)


class TestArrowSchemaUtils:
    @pytest.fixture
    def attr_types(self):
        return OrderedDict(
            [
                ("field-1", AttributeType.STRING),
                ("field-2", AttributeType.INT),
                ("field-3", AttributeType.LONG),
                ("field-4", AttributeType.DOUBLE),
                ("field-5", AttributeType.BOOL),
                ("field-6", AttributeType.TIMESTAMP),
                ("field-7", AttributeType.BINARY),
            ]
        )

    def test_attr_types_to_arrow_schema_maps_each_type(self, attr_types):
        schema = attr_types_to_arrow_schema(attr_types)
        assert schema.field("field-1").type == pa.string()
        assert schema.field("field-2").type == pa.int32()
        assert schema.field("field-3").type == pa.int64()
        assert schema.field("field-4").type == pa.float64()
        assert schema.field("field-5").type == pa.bool_()
        assert schema.field("field-6").type == pa.timestamp("us")
        assert schema.field("field-7").type == pa.binary()

    def test_attr_types_to_arrow_schema_preserves_order(self, attr_types):
        schema = attr_types_to_arrow_schema(attr_types)
        assert schema.names == list(attr_types.keys())

    def test_arrow_schema_to_attr_types_maps_each_type(self):
        arrow_schema = pa.schema(
            [
                pa.field("a", pa.string()),
                pa.field("b", pa.int32()),
                pa.field("c", pa.int64()),
                pa.field("d", pa.float64()),
                pa.field("e", pa.bool_()),
                pa.field("f", pa.timestamp("us")),
                pa.field("g", pa.binary()),
            ]
        )
        attr_types = arrow_schema_to_attr_types(arrow_schema)
        assert attr_types == {
            "a": AttributeType.STRING,
            "b": AttributeType.INT,
            "c": AttributeType.LONG,
            "d": AttributeType.DOUBLE,
            "e": AttributeType.BOOL,
            "f": AttributeType.TIMESTAMP,
            "g": AttributeType.BINARY,
        }

    def test_round_trip_non_large_binary(self, attr_types):
        """Converting attr_types -> arrow -> attr_types is an identity for
        primitive (non LARGE_BINARY) types."""
        arrow_schema = attr_types_to_arrow_schema(attr_types)
        result = arrow_schema_to_attr_types(arrow_schema)
        assert result == dict(attr_types)

    def test_large_binary_serialized_as_string_with_metadata(self):
        attr_types = OrderedDict([("blob", AttributeType.LARGE_BINARY)])
        arrow_schema = attr_types_to_arrow_schema(attr_types)
        field = arrow_schema.field("blob")
        # LARGE_BINARY is physically serialized as a string (URI).
        assert field.type == pa.string()
        # But it is tagged with the texera metadata marker.
        assert field.metadata is not None
        assert field.metadata[TEXERA_TYPE_METADATA_KEY] == LARGE_BINARY_METADATA_VALUE

    def test_large_binary_round_trip_via_metadata(self):
        """LARGE_BINARY survives the round trip only because of the metadata
        marker; without it a plain string would be detected."""
        attr_types = OrderedDict([("blob", AttributeType.LARGE_BINARY)])
        arrow_schema = attr_types_to_arrow_schema(attr_types)
        result = arrow_schema_to_attr_types(arrow_schema)
        assert result == {"blob": AttributeType.LARGE_BINARY}

    def test_plain_string_without_metadata_is_string(self):
        """A string field lacking the LARGE_BINARY marker is a STRING,
        distinguishing it from the metadata-tagged LARGE_BINARY case."""
        arrow_schema = pa.schema([pa.field("s", pa.string())])
        result = arrow_schema_to_attr_types(arrow_schema)
        assert result == {"s": AttributeType.STRING}

    def test_string_with_unrelated_metadata_is_string(self):
        """Metadata present but with a non-matching value must not be
        misdetected as LARGE_BINARY."""
        field = pa.field("s", pa.string(), metadata={b"other": b"value"})
        arrow_schema = pa.schema([field])
        result = arrow_schema_to_attr_types(arrow_schema)
        assert result == {"s": AttributeType.STRING}

    def test_large_string_maps_to_string(self):
        """Arrow LARGE_STRING collapses to Amber STRING."""
        arrow_schema = pa.schema([pa.field("s", pa.large_string())])
        result = arrow_schema_to_attr_types(arrow_schema)
        assert result == {"s": AttributeType.STRING}

    def test_large_binary_arrow_type_maps_to_binary(self):
        """Arrow's LARGE_BINARY physical type (no texera metadata) maps to
        the Amber BINARY type, not LARGE_BINARY."""
        arrow_schema = pa.schema([pa.field("b", pa.large_binary())])
        result = arrow_schema_to_attr_types(arrow_schema)
        assert result == {"b": AttributeType.BINARY}

    def test_empty_schema(self):
        assert arrow_schema_to_attr_types(pa.schema([])) == {}
        assert attr_types_to_arrow_schema(OrderedDict()) == pa.schema([])

    def test_mixed_schema_with_large_binary_preserves_all(self):
        attr_types = OrderedDict(
            [
                ("name", AttributeType.STRING),
                ("payload", AttributeType.LARGE_BINARY),
                ("count", AttributeType.INT),
            ]
        )
        arrow_schema = attr_types_to_arrow_schema(attr_types)
        result = arrow_schema_to_attr_types(arrow_schema)
        assert result == dict(attr_types)
        # non-LARGE_BINARY fields carry no texera metadata marker
        assert arrow_schema.field("name").metadata is None
        assert arrow_schema.field("count").metadata is None
