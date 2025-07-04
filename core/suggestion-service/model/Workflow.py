# the abstraction of workflows
# Methods
# 1. GetOperators(a list of operator type) -> a list of operators, if given list of types are none empty, filter and only keep those operators that of those types
# 2. GetLinks() -> a list of links(between these operators)
# 3. GetSubWorkflowByIndex(idx: int) -> (Workflow, one operator, a list of link related to that operator): this method will topologically sort the operators in the workflow, and
#    it will return a sub workflow containing operator [0, idx-1], the idx-th operator, the links connected to this idx-th operator.


from abc import ABC, abstractmethod
from typing import List, Tuple, Dict

from model.Operator import Operator
from model.Port import Port


class Workflow(ABC):
    @abstractmethod
    def GetWorkflowContent(self) -> str:
        """
        return the raw content of the workflow
        :return:
        """
        pass

    @abstractmethod
    def GetWorkflowId(self) -> int:
        """
        return the id of the workflow
        :return:
        """
        pass

    @abstractmethod
    def GetOperators(self, types: List[str] = None) -> List["Operator"]:
        """
        Return a list of operators. If the given list of types is non-empty,
        filter and only keep those operators of those types.
        """
        pass

    @abstractmethod
    def TopologicalSort(self) -> List["Operator"]:
        """
        Perform a topological sort on the operators in the workflow.
        """
        pass

    @abstractmethod
    def VisualizeDAG(self):
        pass

    @abstractmethod
    def GetDAG(self):
        pass

    @abstractmethod
    def GetSchemaToNextOperatorDistributionMapping(
        self,
    ) -> Dict["DataSchema", Dict[str, int]]:
        pass

    @abstractmethod
    def GetOperatorTypeToNextOperatorDistributionMapping(
        self,
    ) -> Dict[str, Dict[str, int]]:
        pass

    @abstractmethod
    def GetAdditionPairs(
        self,
    ) -> List[Tuple[Tuple[Operator, Port], Tuple[Operator, Port]]]:
        pass
