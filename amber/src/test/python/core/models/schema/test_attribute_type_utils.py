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
from core.models.schema.attribute_type_utils import (
    LARGE_BINARY_METADATA_VALUE,
    TEXERA_TYPE_METADATA_KEY,
    create_arrow_field_with_metadata,
    detect_attribute_type_from_arrow_field,
)

LARGE_BINARY_METADATA = {TEXERA_TYPE_METADATA_KEY: LARGE_BINARY_METADATA_VALUE}


class TestDetectAttributeTypeFromArrowField:
    @pytest.mark.parametrize(
        "arrow_type, expected",
        [
            (pa.int32(), AttributeType.INT),
            (pa.int64(), AttributeType.LONG),
            (pa.string(), AttributeType.STRING),
            (pa.large_string(), AttributeType.STRING),
            (pa.float64(), AttributeType.DOUBLE),
            (pa.bool_(), AttributeType.BOOL),
            (pa.binary(), AttributeType.BINARY),
            (pa.large_binary(), AttributeType.BINARY),
            (pa.timestamp("us"), AttributeType.TIMESTAMP),
        ],
    )
    def test_detects_each_supported_arrow_type(self, arrow_type, expected):
        assert detect_attribute_type_from_arrow_field(pa.field("f", arrow_type)) is (
            expected
        )

    @pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
    def test_timestamp_detected_regardless_of_unit(self, unit):
        """Detection keys off the Arrow type id, which ignores the time unit."""
        field = pa.field("f", pa.timestamp(unit))
        assert detect_attribute_type_from_arrow_field(field) == AttributeType.TIMESTAMP

    def test_large_binary_metadata_overrides_the_arrow_type(self):
        """LARGE_BINARY travels as a plain string, so only metadata identifies it."""
        field = pa.field("f", pa.string(), metadata=LARGE_BINARY_METADATA)
        assert (
            detect_attribute_type_from_arrow_field(field) == AttributeType.LARGE_BINARY
        )

    def test_string_without_metadata_is_not_large_binary(self):
        field = pa.field("f", pa.string())
        assert detect_attribute_type_from_arrow_field(field) == AttributeType.STRING

    def test_metadata_wins_over_a_non_string_arrow_type(self):
        """The metadata check runs first and short-circuits the type mapping."""
        field = pa.field("f", pa.binary(), metadata=LARGE_BINARY_METADATA)
        assert (
            detect_attribute_type_from_arrow_field(field) == AttributeType.LARGE_BINARY
        )

    @pytest.mark.parametrize(
        "metadata",
        [
            pytest.param({}, id="empty"),
            pytest.param({b"unrelated": b"LARGE_BINARY"}, id="wrong-key"),
            pytest.param({TEXERA_TYPE_METADATA_KEY: b"STRING"}, id="wrong-value"),
            pytest.param({TEXERA_TYPE_METADATA_KEY: b""}, id="empty-value"),
            pytest.param(
                {TEXERA_TYPE_METADATA_KEY: b"large_binary"}, id="wrong-case-value"
            ),
        ],
    )
    def test_metadata_that_does_not_mark_large_binary_falls_back_to_type(
        self, metadata
    ):
        field = pa.field("f", pa.string(), metadata=metadata)
        assert detect_attribute_type_from_arrow_field(field) == AttributeType.STRING

    def test_metadata_accepts_str_keys_since_arrow_encodes_them_as_bytes(self):
        """pyarrow normalises str metadata to bytes, so a str key still matches."""
        field = pa.field("f", pa.string(), metadata={"texera_type": "LARGE_BINARY"})
        assert (
            detect_attribute_type_from_arrow_field(field) == AttributeType.LARGE_BINARY
        )

    @pytest.mark.parametrize(
        "arrow_type",
        [
            pytest.param(pa.float32(), id="float32"),
            pytest.param(pa.int8(), id="int8"),
            pytest.param(pa.uint64(), id="uint64"),
            pytest.param(pa.date32(), id="date32"),
            pytest.param(pa.null(), id="null"),
            pytest.param(pa.list_(pa.int32()), id="list"),
            pytest.param(pa.decimal128(10, 2), id="decimal"),
        ],
    )
    def test_unsupported_arrow_type_raises_key_error(self, arrow_type):
        """Unmapped Arrow types surface as KeyError rather than a wrong type."""
        field = pa.field("f", arrow_type)
        with pytest.raises(KeyError):
            detect_attribute_type_from_arrow_field(field)

    def test_unsupported_arrow_type_still_detected_when_marked_large_binary(self):
        """The metadata short-circuit runs before the unsupported-type lookup."""
        field = pa.field("f", pa.float32(), metadata=LARGE_BINARY_METADATA)
        assert (
            detect_attribute_type_from_arrow_field(field) == AttributeType.LARGE_BINARY
        )


class TestCreateArrowFieldWithMetadata:
    @pytest.mark.parametrize(
        "attr_type, expected_arrow_type",
        [
            (AttributeType.INT, pa.int32()),
            (AttributeType.LONG, pa.int64()),
            (AttributeType.STRING, pa.string()),
            (AttributeType.DOUBLE, pa.float64()),
            (AttributeType.BOOL, pa.bool_()),
            (AttributeType.BINARY, pa.binary()),
            (AttributeType.TIMESTAMP, pa.timestamp("us")),
            (AttributeType.LARGE_BINARY, pa.string()),
        ],
    )
    def test_maps_each_attribute_type_to_its_arrow_type(
        self, attr_type, expected_arrow_type
    ):
        field = create_arrow_field_with_metadata("f", attr_type)
        assert field.name == "f"
        assert field.type == expected_arrow_type

    def test_large_binary_field_carries_marker_metadata(self):
        field = create_arrow_field_with_metadata("payload", AttributeType.LARGE_BINARY)
        assert field.metadata[TEXERA_TYPE_METADATA_KEY] == LARGE_BINARY_METADATA_VALUE

    @pytest.mark.parametrize(
        "attr_type",
        [t for t in AttributeType if t is not AttributeType.LARGE_BINARY],
        ids=lambda t: t.name,
    )
    def test_non_large_binary_fields_carry_no_metadata(self, attr_type):
        assert create_arrow_field_with_metadata("f", attr_type).metadata is None

    @pytest.mark.parametrize(
        "attr_name",
        [
            pytest.param("", id="empty"),
            pytest.param(" ", id="whitespace"),
            pytest.param("列名", id="unicode"),
            pytest.param("emoji-🎉", id="emoji"),
            pytest.param("with space", id="with-space"),
            pytest.param("a" * 1000, id="very-long"),
        ],
    )
    def test_attribute_name_is_preserved_verbatim(self, attr_name):
        field = create_arrow_field_with_metadata(attr_name, AttributeType.STRING)
        assert field.name == attr_name

    def test_unicode_name_survives_on_a_large_binary_field(self):
        field = create_arrow_field_with_metadata("列名", AttributeType.LARGE_BINARY)
        assert field.name == "列名"
        assert field.metadata[TEXERA_TYPE_METADATA_KEY] == LARGE_BINARY_METADATA_VALUE

    @pytest.mark.parametrize(
        "invalid_type",
        [
            pytest.param("STRING", id="raw-string"),
            pytest.param(None, id="none"),
            pytest.param(1, id="int"),
            pytest.param(pa.string(), id="arrow-type"),
        ],
    )
    def test_non_attribute_type_raises_type_or_key_error(self, invalid_type):
        """Non-AttributeType inputs fail fast; the exact exception is an
        implementation detail of the mapping lookup."""
        with pytest.raises((KeyError, TypeError)):
            create_arrow_field_with_metadata("f", invalid_type)


class TestRoundTrip:
    @pytest.mark.parametrize("attr_type", list(AttributeType), ids=lambda t: t.name)
    def test_every_attribute_type_survives_a_round_trip(self, attr_type):
        field = create_arrow_field_with_metadata("f", attr_type)
        assert detect_attribute_type_from_arrow_field(field) == attr_type

    def test_large_binary_would_degrade_to_string_without_its_metadata(self):
        """Guards the reason the metadata exists: the Arrow type alone is lossy."""
        field = create_arrow_field_with_metadata("f", AttributeType.LARGE_BINARY)
        stripped = pa.field(field.name, field.type)
        assert detect_attribute_type_from_arrow_field(stripped) == AttributeType.STRING

    def test_round_trip_through_an_arrow_schema_preserves_types(self):
        """Metadata must survive being packed into and read back from a schema."""
        attr_types = list(AttributeType)
        schema = pa.schema(
            [
                create_arrow_field_with_metadata(f"field-{i}", attr_type)
                for i, attr_type in enumerate(attr_types)
            ]
        )
        detected = [
            detect_attribute_type_from_arrow_field(schema.field(i))
            for i in range(len(attr_types))
        ]
        assert detected == attr_types
