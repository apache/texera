# model/serialization.py
from abc import ABC, abstractmethod
from enum import Enum

from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any


class AttributeInterpretation(BaseModel):
    attributeName: str
    attributeType: str


class SchemaInterpretation(BaseModel):
    attributes: List[AttributeInterpretation]


class PortInterpretation(BaseModel):
    portID: str
    inputSchema: SchemaInterpretation


class ErrorInterpretation(BaseModel):
    type: str
    message: str
    details: str


class OperatorInterpretation(BaseModel):
    operatorID: str
    operatorType: str
    customDisplayName: Optional[str]
    operatorProperties: Dict[str, Any]
    error: Optional[ErrorInterpretation]
    inputSchemas: Dict[str, PortInterpretation]
    outputSchema: SchemaInterpretation = Field(
        default_factory=lambda: SchemaInterpretation(attributes=[])
    )


class LinkEndInterpretation(BaseModel):
    operatorID: str
    portID: str


class LinkInterpretation(BaseModel):
    linkID: str
    source: LinkEndInterpretation
    target: LinkEndInterpretation


class WorkflowInterpretation(BaseModel):
    operators: Dict[str, OperatorInterpretation]
    links: List[LinkInterpretation]


# Base interpretation class for commonality among interpretations
class BaseInterpretation(BaseModel, ABC):
    """Abstract base class for workflow interpretation variants."""

    class Config:
        extra = "forbid"

    @abstractmethod
    def get_base_workflow_interpretation(self) -> WorkflowInterpretation:
        """Returns one or more workflow interpretations (typically just one for Raw, or multiple for Path)."""
        pass


# Interpretation derived from linear execution paths
class PathInterpretation(BaseInterpretation):
    workflow: WorkflowInterpretation
    paths: List[List[str]]

    def get_base_workflow_interpretation(self) -> WorkflowInterpretation:
        return self.workflow


# Interpretation derived directly from the raw workflow object
class RawInterpretation(BaseInterpretation):
    workflow: WorkflowInterpretation

    def get_base_workflow_interpretation(self) -> WorkflowInterpretation:
        return self.workflow


class InterpretationMethod(Enum):
    RAW = "raw"
    BY_PATH = "by_path"
