import pytest
import pyarrow as pa

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
                pa.field("field-6", pa.timestamp("ms", tz="UTC")),
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
