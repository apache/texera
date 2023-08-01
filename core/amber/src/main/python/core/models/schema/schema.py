from collections import OrderedDict
from typing import MutableMapping, Optional, Mapping, List, Tuple

import pyarrow as pa

from core.models.schema.attribute_type import (
    AttributeType,
    RAW_TYPE_MAPPING,
    FROM_ARROW_MAPPING,
    TO_ARROW_MAPPING,
)


class Schema:
    """
    Schema describes a sequence of attributes, maintaining a name-to-type mapping.

    Schema is mapped to PyArrow's Schema (pyarrow.Schema), with each
    AttributeType mapped to a pyarrow.DataType.

    Schema is mapped to a Tuple Field (which is a collection TypeVar of
    python objects).

    See AttributeType for detailed mappings.

    Note: Schema is to be used by the engine only, and should be invisible to the
    users of Tuple. It only gets assigned to a Tuple to finalize the Tuple for
    serialization purpose.
    """

    def __init__(
        self,
        arrow_schema: Optional[pa.Schema] = None,
        raw_schema: Optional[Mapping[str, str]] = None,
    ):
        self._name_type_mapping: MutableMapping[str, AttributeType] = OrderedDict()

        if arrow_schema is not None:
            self._from_arrow_schema(arrow_schema)
        if raw_schema is not None:
            self._from_raw_schema(raw_schema)

    def add(self, attr_name: str, attr_type: AttributeType) -> None:
        """
        Append a new attribute with its name and type to the Schema.
        :param attr_name: new attribute's name, must not be in the Schema already.
        :param attr_type: the type of the attribute.
        :return:
        """
        if attr_name in self._name_type_mapping:
            raise KeyError(f"Adding a duplicated attribute {repr(attr_name)}.")
        self._name_type_mapping[attr_name] = attr_type

    def _from_raw_schema(self, raw_schema: Mapping[str, str]) -> None:
        """
        Resets the Schema by converting a raw schema.
        :param raw_schema: a map of attr_name -> type_str.
        :return:
        """
        self._name_type_mapping = OrderedDict()
        for attr_name, raw_type in raw_schema.items():
            attr_type = RAW_TYPE_MAPPING[raw_type]
            self.add(attr_name, attr_type)

    def _from_arrow_schema(self, arrow_schema: pa.Schema) -> None:
        """
        Resets the Schema by converting a pyarrow.Schema.
        :param arrow_schema: a pyarrow.Schema.
        :return:
        """
        self._name_type_mapping = OrderedDict()
        for attr_name in arrow_schema.names:
            arrow_type = arrow_schema.field(attr_name).type  # type: ignore
            attr_type = FROM_ARROW_MAPPING[arrow_type.id]
            self.add(attr_name, attr_type)

    def as_arrow_schema(self) -> pa.Schema:
        """
        Creates a new pyarrow.Schema according to the current Schema.
        :return: pyarrow.Schema
        """
        return pa.schema(
            [
                pa.field(attr_name, TO_ARROW_MAPPING[attr_type])
                for attr_name, attr_type in self._name_type_mapping.items()
            ]
        )

    def get_attr_names(self) -> List[str]:
        """
        Get all the attributes' names.
        :return: a list of attribute names.
        """
        return list(self._name_type_mapping.keys())

    def get_attr_type(self, attr_name: str) -> AttributeType:
        """
        Get an attribute's type specified by an attribute name.
        :param attr_name: the name of the target attribute.
        :return: the AttributeType of the target attribute.
        """
        return self._name_type_mapping[attr_name]

    def as_key_value_pairs(self) -> List[Tuple[str, AttributeType]]:
        """
        Creates all attributes information according to the current Schema.
        :return: A list of (name, type) tuples.
        """
        return [(k, v) for k, v in self._name_type_mapping.items()]

    def get_partial_schema(self, indices: List[int]) -> "Schema":
        """
        Create a partial Schema with fields specified by the indices.
        :param indices: A list of index values.
        :return: A new Schema with the selected fields, with the same order specified
        by the indices.
        """
        raw_schema = OrderedDict()
        for index, (key, value) in enumerate(self.as_key_value_pairs(), 0):
            if index in indices:
                raw_schema[key] = RAW_TYPE_MAPPING.inverse[value]
        return Schema(raw_schema=raw_schema)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Schema):
            return False
        left_pairs = self.as_key_value_pairs()
        right_pairs = other.as_key_value_pairs()
        return left_pairs == right_pairs

    def __str__(self) -> str:
        content = ",\n".join(
            f"({index}){repr(attr_name)} -> {attr_type}"
            for index, (attr_name, attr_type) in enumerate(self.as_key_value_pairs(), 0)
        )
        return f"Schema[\n{content}\n]"
