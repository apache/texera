from typing import Dict, List, Any, Optional, OrderedDict, Iterator, Union

from model.DataSchema import DataSchema, Attribute


class Tuple:
    """
    A simple implementation of a tuple for data storage.
    """

    def __init__(
        self, data: Optional[Dict[str, Any]] = None, schema: Optional[DataSchema] = None
    ):
        """
        Initialize a Tuple with given data and optional schema.

        :param data: Dictionary mapping field names to values
        :param schema: Optional schema that defines the structure of this tuple
        """
        self._data: Dict[str, Any] = data or {}
        self._schema = schema

    def __getitem__(self, key: Union[str, int]) -> Any:
        """
        Get a field value by name or index.

        :param key: Field name (str) or index (int)
        :return: Value of the field
        """
        if isinstance(key, int):
            key = list(self._data.keys())[key]
        return self._data.get(key)

    def __setitem__(self, key: str, value: Any) -> None:
        """
        Set a field value.

        :param key: Field name
        :param value: Field value
        """
        self._data[key] = value

    def __contains__(self, key: str) -> bool:
        """
        Check if the tuple contains a field.

        :param key: Field name
        :return: True if the field exists in the tuple
        """
        return key in self._data

    def __len__(self) -> int:
        """
        Get the number of fields in the tuple.

        :return: Number of fields
        """
        return len(self._data)

    def __iter__(self) -> Iterator[Any]:
        """
        Iterate over field values.

        :return: Iterator over field values
        """
        return iter(self._data.values())

    def __str__(self) -> str:
        """
        String representation of the tuple.

        :return: String representation
        """
        return f"Tuple({self._data})"

    def __repr__(self) -> str:
        return self.__str__()

    def __eq__(self, other: object) -> bool:
        """
        Check equality with another tuple.

        :param other: Another tuple
        :return: True if tuples are equal
        """
        if not isinstance(other, Tuple):
            return False
        return self._data == other._data

    def get_field_names(self) -> List[str]:
        """
        Get list of field names in the tuple.

        :return: List of field names
        """
        return list(self._data.keys())

    def get_fields(self) -> Dict[str, Any]:
        """
        Get dictionary representation of the tuple.

        :return: Dictionary with field names as keys and values as values
        """
        return self._data.copy()

    def get_field(self, field_name: str) -> Any:
        """
        Get value of a specific field.

        :param field_name: Name of the field
        :return: Value of the field
        """
        return self._data.get(field_name)

    def set_field(self, field_name: str, value: Any) -> None:
        """
        Set value of a specific field.

        :param field_name: Name of the field
        :param value: Value to set
        """
        self._data[field_name] = value

    def as_dict(self) -> Dict[str, Any]:
        """
        Convert tuple to dictionary.

        :return: Dictionary representation of the tuple
        """
        return self._data.copy()

    def set_schema(self, schema: DataSchema) -> None:
        """
        Set schema for the tuple.

        :param schema: Schema to set
        """
        self._schema = schema

    def get_schema(self) -> Optional[DataSchema]:
        """
        Get schema of the tuple.

        :return: Schema of the tuple
        """
        return self._schema

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Tuple":
        """
        Create a tuple from a dictionary.

        :param data: Dictionary with field names and values
        :return: New Tuple instance
        """
        return cls(data)

    @classmethod
    def from_list(cls, field_names: List[str], values: List[Any]) -> "Tuple":
        """
        Create a tuple from lists of field names and values.

        :param field_names: List of field names
        :param values: List of values
        :return: New Tuple instance
        """
        if len(field_names) != len(values):
            raise ValueError("Number of field names must match number of values")
        return cls(dict(zip(field_names, values)))
