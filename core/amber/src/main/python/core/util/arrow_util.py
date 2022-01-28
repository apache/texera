from typing import Mapping
import pyarrow as pa

ARROW_TYPE_MAPPING = {
    'string': pa.string(),
    'integer': pa.int32(),
    'long': pa.int64(),
    'double': pa.float64(),
    'boolean': pa.bool_(),
    'any': pa.string()
}


def to_arrow_schema(raw_schema: Mapping[str, str]) -> pa.Schema:
    return pa.schema(
        [pa.field(name, ARROW_TYPE_MAPPING[attribute_type]) for name, attribute_type in raw_schema.items()])
