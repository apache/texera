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

"""
Utilities for converting between Arrow schemas and Amber schemas,
handling LARGE_BINARY metadata preservation.
"""

import pyarrow as pa
from typing import Mapping

from core.models.schema.attribute_type import AttributeType
from core.models.schema.attribute_type_utils import (
    detect_attribute_type_from_arrow_field,
    create_arrow_field_with_metadata,
)


def arrow_schema_to_attr_types(arrow_schema: pa.Schema) -> dict[str, AttributeType]:
    """
    Converts an Arrow schema to a dictionary of attribute name to AttributeType.
    Handles LARGE_BINARY metadata detection.

    :param arrow_schema: PyArrow schema that may contain LARGE_BINARY metadata
    :return: Dictionary mapping attribute names to AttributeTypes
    """
    attr_types = {}
    for attr_name in arrow_schema.names:
        field = arrow_schema.field(attr_name)
        attr_types[attr_name] = detect_attribute_type_from_arrow_field(field)
    return attr_types


def attr_types_to_arrow_schema(
    attr_types: Mapping[str, AttributeType],
) -> pa.Schema:
    """
    Converts a mapping of attribute name to AttributeType into an Arrow schema.
    Adds metadata for LARGE_BINARY types.
    Preserves the order of attributes from the input mapping.

    :param attr_types: Mapping of attribute names to AttributeTypes (e.g., OrderedDict)
    :return: PyArrow schema with metadata for LARGE_BINARY types
    """
    fields = [
        create_arrow_field_with_metadata(attr_name, attr_type)
        for attr_name, attr_type in attr_types.items()
    ]
    return pa.schema(fields)
