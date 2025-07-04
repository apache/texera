from typing import Tuple, List

from abc import ABC, abstractmethod

from model.Operator import Operator
from model.Port import Port
from model.Workflow import Workflow


class EditingOperationType:
    AddOperator = "add_op"
    RemoveOperator = "remove_op"
    UpdateOperator = "edit_op"
    AddLink = "add_link"
    RemoveLink = "remove_link"
    UpdateLink = "edit_link"
    Misc = "misc"
    Unchanged = "unchanged"
    Void = "void"


class EditingOperation(ABC):
    @abstractmethod
    def GetBaseWorkflow(self) -> Workflow:
        pass

    @abstractmethod
    def GetBase(
        self,
    ) -> (
        None
        | List[Operator]
        | List[Tuple[Tuple[Operator, Port], Tuple[Operator, Port]]]
    ):
        """
        Return the base of this operation
        add operator(s) -> None
        remove operator(s) -> List[Operator]
        modify operator(s) -> List[Operator]

        add link -> List[Tuple[Tuple[Operator, Port], Tuple[Operator, Port]]]
        remove link -> List[Tuple[Tuple[Operator, Port], Tuple[Operator, Port]]]
        modify link -> List[Tuple[Tuple[Operator, Port], Tuple[Operator, Port]]]
        """
        pass

    @abstractmethod
    def GetModification(
        self,
    ) -> (
        None
        | List[Operator]
        | List[Tuple[Tuple[Operator, Port], Tuple[Operator, Port]]]
    ):
        """
        Return "what's new" brought by this patch.

        add operator(s) -> List[Operator]
        remove operator(s) -> None
        modify operator(s) -> List[Operator]

        add link -> None
        remove link -> None
        modify link -> List[Tuple[Tuple[Operator, Port], Tuple[Operator, Port]]]
        """
        pass

    @abstractmethod
    def GetType(self) -> EditingOperationType:
        """
        Return the type of the operation
        """
        pass

    @abstractmethod
    def GetRawPatch(self) -> dict:
        pass

    @abstractmethod
    def IsValid(self) -> bool:
        pass
