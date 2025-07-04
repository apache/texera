# Input models
import json
import re

from pydantic import BaseModel, Field, validator, field_validator
from typing import List

from typing import Dict, List, Any, Optional

from model.llm.interpretation import AttributeInterpretation

# Import all relevant classes from the proto definition
from model.proto.edu.uci.ics.amber.engine.architecture.worker import (
    TableProfile,
    GlobalProfile,
    ColumnProfile,
    NumericMatrix,
    ColumnIndexList,
    GlobalProfileTimes,
    ColumnStatistics,
)


class SchemaAttribute(BaseModel):
    attributeName: str
    attributeType: str

    class Config:
        extra = "allow"


class PhysicalPlan(BaseModel):
    # operators IS A LIST  ➜ declare it as such
    operators: List[Dict[str, Any]] = Field(default_factory=list)
    links: List[Dict[str, Any]] = Field(default_factory=list)


class CompilationStateInfo(BaseModel):
    state: str
    physicalPlan: Optional[PhysicalPlan] = None
    operatorInputSchemaMap: Optional[
        Dict[str, List[Optional[List[SchemaAttribute]]]]
    ] = None
    operatorErrors: Optional[Dict[str, Any]] = None


class ExecutionStateInfo(BaseModel):
    state: str
    currentTuples: Optional[Dict[str, Any]] = None
    errorMessages: Optional[List[Dict[str, Any]]] = None


class SuggestionRequest(BaseModel):
    workflow: str = Field(..., description="JSON string of the workflow")
    compilationState: CompilationStateInfo
    executionState: Optional[ExecutionStateInfo] = None
    intention: Optional[str] = Field(
        "", description="User intention for the suggestion generation"
    )
    focusingOperatorIDs: Optional[List[str]] = Field(
        default_factory=list, description="Operator IDs that the user wants to focus on"
    )
    operatorIDToTableSchemaMap: Dict[str, List[AttributeInterpretation]]


def _camel_to_snake(name: str) -> str:
    """
    fooBar  → foo_bar
    rowStatsMs → row_stats_ms
    """
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def _deep_snake(d: Any) -> Any:
    """
    Recursively convert all dict keys from camelCase to snake_case.
    Lists / scalars are left untouched.
    """
    if isinstance(d, dict):
        return {_camel_to_snake(k): _deep_snake(v) for k, v in d.items()}
    if isinstance(d, list):
        return [_deep_snake(x) for x in d]
    return d


class TableProfileSuggestionRequest(BaseModel):
    tableProfile: TableProfile
    tableSchema: List[AttributeInterpretation]
    targetColumnName: str
    focusingOperatorID: str

    # 🔑 custom validator
    @field_validator("tableProfile", mode="before")
    def _coerce_table_profile(cls, v):
        """
        Accept:
        • dict (parsed JSON)  → snake_case → betterproto
        • JSON string         → same
        • TableProfile        → pass through
        """
        if isinstance(v, TableProfile):
            return v

        if isinstance(v, (dict, str)):
            data_dict: Dict[str, Any] = v if isinstance(v, dict) else json.loads(v)
            snake = _deep_snake(data_dict)
            return TableProfile().from_json(json.dumps(snake))

        raise TypeError("tableProfile must be a TableProfile, dict, or JSON string.")

    class Config:
        arbitrary_types_allowed = True
