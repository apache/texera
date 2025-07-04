# the abstraction of operators

# Operator Methods
# - GetName() -> str, return the name of the operator(e.g. Filter out all qualtified year)
# - GetType() -> str, return the type of the operator(e.g. FileScan, CSVFileScan, Filter, Projection)
# - GetId() -> str, return the id of the operator
# - GetProperties() -> dict, return a dict that contains the key(property name), the value(property values, could be single value, tuple, another dict, or list)
# - GetInputPort() -> list of Ports
# - GetOutputPort() -> list of Ports
# - GetInputSchema() -> dict, the key is the input port, the value is the DataSchema
# - GetOutputSchema() -> dict, the key is the output port, the value is the DataSchema
# - IsDynamicInputPorts() -> bool
# - IsDynamicOutputPorts() -> bool
# - IsDisabled() -> bool
# - IsViewResult() -> bool

from abc import ABC, abstractmethod
from typing import Dict, List


class Operator(ABC):
    @abstractmethod
    def GetName(self) -> str:
        """
        Return the name of the operator (e.g. 'Filter out all qualified year').
        """
        pass

    @abstractmethod
    def GetType(self) -> str:
        """
        Return the type of the operator (e.g. 'FileScan', 'CSVFileScan', 'Filter', 'Projection').
        """
        pass

    @abstractmethod
    def GetId(self) -> str:
        """
        Return the ID of the operator.
        """
        pass

    @abstractmethod
    def GetProperties(self) -> Dict:
        """
        Return a dictionary containing the properties of the operator.
        The dictionary can contain property names as keys and property values (which could be a single value, tuple, another dict, or list) as values.
        """
        pass

    @abstractmethod
    def GetInputSchemaByPortID(self, portID: str) -> "DataSchema":
        pass

    @abstractmethod
    def GetInputPorts(self) -> List["Port"]:
        """
        Return a list of input ports for the operator.
        """
        pass

    @abstractmethod
    def GetOutputPorts(self) -> List["Port"]:
        """
        Return a list of output ports for the operator.
        """
        pass

    @abstractmethod
    def GetError(self) -> str:
        """
        Return the static error on this operator if any
        :return:
        """
        pass

    @abstractmethod
    def IsDynamicInputPorts(self) -> bool:
        """
        Return whether the operator has dynamic input ports.
        """
        pass

    @abstractmethod
    def IsDynamicOutputPorts(self) -> bool:
        """
        Return whether the operator has dynamic output ports.
        """
        pass

    @abstractmethod
    def IsDisabled(self) -> bool:
        """
        Return whether the operator is disabled.
        """
        pass

    @abstractmethod
    def IsViewResult(self) -> bool:
        """
        Return whether the operator is for viewing results.
        """
        pass
