from dataclasses import dataclass
from pandas import DataFrame
from pyarrow import Table
from typing import Optional

from .schema import Schema, AttributeType
from .schema.attribute_type import FROM_PYOBJECT_MAPPING


@dataclass
class Marker:
    pass


@dataclass
class StartOfInputChannel(Marker):
    pass


@dataclass
class EndOfInputChannel(Marker):
    pass


@dataclass
class State(Marker):
    def __init__(
        self, table: Optional[Table] = None, pass_to_all_downstream: bool = False
    ):
        self.schema = Schema()
        self.passToAllDownstream = pass_to_all_downstream
        if table is not None:
            self.__dict__.update(table.to_pandas().iloc[0].to_dict())
            self.schema = Schema(table.schema)

    def add(
        self, key: str, value: any, value_type: Optional[AttributeType] = None
    ) -> None:
        self.__dict__[key] = value
        if value_type is not None:
            self.schema.add(key, value_type)
        elif key != "schema":
            self.schema.add(key, FROM_PYOBJECT_MAPPING[type(value)])

    def get(self, key: str) -> any:
        return self.__dict__[key]

    def to_table(self) -> Table:
        return Table.from_pandas(
            df=DataFrame([self.__dict__]),
            schema=self.schema.as_arrow_schema(),
        )

    def __setattr__(self, key: str, value: any) -> None:
        self.add(key, value)

    def __setitem__(self, key: str, value: any) -> None:
        self.add(key, value)

    def __getitem__(self, key: str) -> any:
        return self.get(key)

    def __str__(self) -> str:
        content = ", ".join(
            [
                repr(key) + ": " + repr(value)
                for key, value in self.__dict__.items()
                if key != "schema"
            ]
        )
        return f"State[{content}]"

    __repr__ = __str__
