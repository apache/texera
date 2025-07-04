from typing import Dict, List, Tuple

from model.DataSchema import DataSchema
from model.Operator import Operator
from model.llm.interpretation import PortInterpretation, OperatorInterpretation
from model.texera.TexeraPort import TexeraPort


class TexeraOperator(Operator):
    def __init__(
        self,
        operator_dict: dict,
        port_indexed_input_schemas: List["DataSchema"] = [],
        error: dict = {},
    ):
        self.operator_id = operator_dict.get("operatorID", "")
        self.operator_type = operator_dict.get("operatorType", "")
        self.operator_version = operator_dict.get("operatorVersion", "")
        self.operator_properties = operator_dict.get("operatorProperties", {})

        # a mapping from port id to port
        self.input_ports: Dict[str, "TexeraPort"] = {
            port_dict.get("portID"): TexeraPort(
                port_dict=port_dict,
                is_input_port=True,
                operator=self,
                schema=(
                    port_indexed_input_schemas[i]
                    if i < len(port_indexed_input_schemas)
                    else DataSchema([])
                ),
            )
            for i, port_dict in enumerate(operator_dict.get("inputPorts", []))
        }

        self.output_ports: Dict[str, "TexeraPort"] = {
            port_dict.get("portID"): TexeraPort(
                port_dict=port_dict,
                is_input_port=False,
                operator=self,
                schema=DataSchema([]),
            )
            for port_dict in operator_dict.get("outputPorts", [])
        }
        self.show_advanced = operator_dict.get("showAdvanced", False)
        self.is_disabled = operator_dict.get("isDisabled", False)
        self.custom_display_name = operator_dict.get("customDisplayName", "")
        self.dynamic_input_ports = operator_dict.get("dynamicInputPorts", False)
        self.dynamic_output_ports = operator_dict.get("dynamicOutputPorts", False)
        self.view_result = operator_dict.get("viewResult", False)
        self.input_schema = port_indexed_input_schemas
        if error != {}:
            self.error = {
                "type": error.get("type").get("name"),
                "message": error.get("message"),
                "details": error.get("details")[:200],
            }
        else:
            self.error = None

    def GetName(self) -> str:
        return self.custom_display_name

    def GetType(self) -> str:
        return self.operator_type

    def GetId(self) -> str:
        return self.operator_id

    def GetProperties(self) -> Dict:
        return self.operator_properties

    def GetInputSchemaByPortID(self, portID: str) -> "DataSchema":
        port = self.input_ports.get(portID)
        if port is not None:
            return port.GetDataSchema()
        return None

    def GetInputPorts(self) -> List["Port"]:
        return list(self.input_ports.values())

    def GetOutputPorts(self) -> List["Port"]:
        return list(self.output_ports.values())

    def GetError(self) -> str:
        return self.error

    def IsDynamicInputPorts(self) -> bool:
        return self.dynamic_input_ports

    def IsDynamicOutputPorts(self) -> bool:
        return self.dynamic_output_ports

    def IsDisabled(self) -> bool:
        return self.is_disabled

    def IsViewResult(self) -> bool:
        return self.view_result

    # Add this method to TexeraOperator

    def ToPydantic(self) -> OperatorInterpretation:

        return OperatorInterpretation(
            operatorID=self.operator_id,
            operatorType=self.operator_type,
            customDisplayName=self.custom_display_name or None,
            operatorProperties=self.operator_properties,
            error=self.error,
            inputSchemas={
                port.GetId(): port.ToPydantic() for port in self.GetInputPorts()
            },
        )

    def __str__(self) -> str:
        input_ports_str = "\n    ".join([str(port) for port in self.GetInputPorts()])
        output_ports_str = "\n    ".join([str(port) for port in self.GetOutputPorts()])
        return (
            f"TexeraOperator(\n"
            f"  ID={self.operator_id},\n"
            f"  Type={self.operator_type},\n"
            f"  Version={self.operator_version},\n"
            f"  Properties={self.operator_properties},\n"
            f"  InputPorts=[\n    {input_ports_str}\n  ],\n"
            f"  OutputPorts=[\n    {output_ports_str}\n  ],\n"
            f"  Error={self.error}\n"
            f")"
        )

    @property
    def properties(self) -> Dict:
        """
        Get the operator properties.

        Returns:
            The operator properties dictionary
        """
        return self.operator_properties
