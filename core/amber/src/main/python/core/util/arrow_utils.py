from typing import Mapping
import pyarrow as pa

"""
The definitions are mapped and following the AttributeType.java
(src/main/scala/edu/uci/ics/texera/workflow/common/tuple/schema/AttributeType.java)
"""

ARROW_TYPE_MAPPING = {
    'string': pa.string(),
    'integer': pa.int32(),
    'long': pa.int64(),
    'double': pa.float64(),
    'boolean': pa.bool_(),
    'timestamp': pa.timestamp('ms', tz="UTC"),
    'binary': pa.binary(),
    'ANY': pa.string()
}


def to_arrow_schema(raw_schema: Mapping[str, str]) -> pa.Schema:
    return pa.schema(
        [pa.field(name, ARROW_TYPE_MAPPING[attribute_type]) for name, attribute_type in raw_schema.items()])
