from typing import List

from model.DataSchema import DataSchema
from model.Operator import Operator
from model.Port import Port
from model.llm.interpretation import (
    PortInterpretation,
    SchemaInterpretation,
    AttributeInterpretation,
)


class TexeraPort(Port):
    def __init__(
        self,
        port_dict: dict,
        is_input_port: bool,
        operator: Operator,
        schema: DataSchema = DataSchema([]),
    ):
        self.port_id = port_dict.get("portID", "")
        self.display_name = port_dict.get("displayName", "")
        self.allow_multi_inputs = port_dict.get("allowMultiInputs", False)
        self.is_dynamic_port = port_dict.get("isDynamicPort", False)
        self.dependencies = port_dict.get("dependencies", {})
        self.is_input_port = is_input_port
        self.affiliate_operator = operator
        self.data_schema = schema

    def IsInputPort(self) -> bool:
        return self.is_input_port

    def IsOutputPort(self) -> bool:
        return not self.is_input_port

    def GetId(self) -> str:
        return self.port_id

    def GetDisplayName(self) -> str:
        return self.display_name

    def AllowMultiInputs(self) -> bool:
        return self.allow_multi_inputs

    def IsDynamicPort(self) -> bool:
        return self.is_dynamic_port

    def GetDependencies(self) -> List[str]:
        return self.dependencies

    def GetSourcePorts(self) -> List["Port"]:
        if self.IsOutputPort():
            raise RuntimeError("output port doesn't have the source ports!")
        return self.source_ports

    def GetTargetPorts(self) -> List["Port"]:
        if self.IsInputPort():
            raise RuntimeError("Input port doesn't have the target ports")
        return self.target_ports

    def GetDataSchema(self) -> "DataSchema":
        return self.data_schema

    def GetAffiliateOperator(self) -> "Operator":
        return self.affiliate_operator

    def ToPydantic(self) -> PortInterpretation:
        attributeList = [
            AttributeInterpretation(attributeName=attr.name, attributeType=attr.type)
            for attr in self.GetDataSchema().attributes
        ]
        return PortInterpretation(
            portID=self.GetId(),
            inputSchema=SchemaInterpretation(attributes=attributeList),
        )

    def __str__(self) -> str:
        return (
            f"TexeraPort(\n"
            f"  ID={self.port_id}, \n"
            f"  DataSchema={self.data_schema}, \n"
            f")"
        )
