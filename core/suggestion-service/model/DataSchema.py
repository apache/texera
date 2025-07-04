# the abstraction of data schema and attributes

## define some constants related to type
### maybe define a class/enum called: AttributeType
### the possible values are: Integer, Long, Double, String, Boolean, binary


## the method of Attribute(not an abstract class),
## member variables:
## - name: the name of the attribute, a str
## - type: AttributeType
## methods: Getters for the name and type
## constructor: take the name and type to construct


## the method of DataSchema, which should also be a concrete class
## member variables:
## - a set of Attribute

## methods:
## - the getter to return this set of attributes

## constructors
## take a list of Attriubte
from enum import Enum
from typing import List, Set, Dict

# Define the AttributeType enum
from enum import Enum
from typing import List, FrozenSet


# Define the AttributeType enum
class AttributeType(Enum):
    INTEGER = "integer"
    LONG = "long"
    DOUBLE = "double"
    STRING = "string"
    BOOLEAN = "boolean"
    BINARY = "binary"
    TIMESTAMP = "timestamp"


# Define the Attribute class
class Attribute:
    def __init__(self, name: str, attr_type: AttributeType):
        self._name = name
        self._type = attr_type

    @property
    def name(self) -> str:
        return self._name

    @property
    def type(self) -> AttributeType:
        return self._type

    def __str__(self) -> str:
        return f"Attribute(name={self.name}, type={self.type.value})"

    def __repr__(self) -> str:
        return f"Attribute(name={self.name!r}, type={self.type!r})"

    def __eq__(self, other) -> bool:
        if isinstance(other, Attribute):
            return self._name == other._name and self._type == other._type
        return False

    def __hash__(self) -> int:
        return hash((self._name, self._type))


# Define the DataSchema class
class DataSchema:
    def __init__(self, attributes: List[Dict]):
        attributeList = [
            Attribute(a.get("attributeName"), a.get("attributeType"))
            for a in attributes
        ]
        self._attributes: FrozenSet[Attribute] = frozenset(attributeList)

    @property
    def attributes(self) -> FrozenSet[Attribute]:
        return self._attributes

    def __str__(self) -> str:
        attributes_str = ", ".join(str(attr) for attr in self.attributes)
        return f"DataSchema(attributes=[{attributes_str}])"

    def __repr__(self) -> str:
        attributes_repr = ", ".join(repr(attr) for attr in self.attributes)
        return f"DataSchema(attributes=[{attributes_repr}])"

    def __eq__(self, other) -> bool:
        if isinstance(other, DataSchema):
            return self._attributes == other._attributes
        return False

    def __hash__(self) -> int:
        return hash(self._attributes)
