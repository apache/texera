# the abstraction of port

# Port Methods
# IsInputPort() -> bool, return whether this port is input port
# IsOutputPort() -> bool, return whether this port is output port
# GetId() -> str, return the id of the port
# GetDisplayName() -> str, return the display name of the port
# AllowMultiInputs() -> bool, return whether the port allow multiple input edges
# IsDynamicPort() -> bool, return whether this port is a dynamic port
# Dependencies() -> str[], return a list of dependencies of the port id str if any

from abc import ABC, abstractmethod
from typing import List


class Port(ABC):
    @abstractmethod
    def IsInputPort(self) -> bool:
        """
        Return whether this port is an input port.
        """
        pass

    @abstractmethod
    def IsOutputPort(self) -> bool:
        """
        Return whether this port is an output port.
        """
        pass

    @abstractmethod
    def GetId(self) -> str:
        """
        Return the ID of the port.
        """
        pass

    @abstractmethod
    def GetDisplayName(self) -> str:
        """
        Return the display name of the port.
        """
        pass

    @abstractmethod
    def AllowMultiInputs(self) -> bool:
        """
        Return whether the port allows multiple input edges.
        """
        pass

    @abstractmethod
    def IsDynamicPort(self) -> bool:
        """
        Return whether this port is a dynamic port.
        """
        pass

    @abstractmethod
    def GetDataSchema(self) -> "DataSchema":
        """
        return the data schema annotated to this port
        """
        pass

    @abstractmethod
    def GetDependencies(self) -> List[str]:
        """
        Return a list of dependencies of the port ID as strings, if any.
        """
        pass

    @abstractmethod
    def GetAffiliateOperator(self) -> "Operator":
        """
        Return the operator that this port is affiliated to
        """
        pass

    def GetTargetPorts(self) -> List["Port"]:
        """
        Return the list of ports that this port(must be an output port) is sourcing
        """
        pass

    def GetSourcePorts(self) -> List["Port"]:
        """
        Return the list of ports that this port(must be an input port) is being targeting
        """
        pass
