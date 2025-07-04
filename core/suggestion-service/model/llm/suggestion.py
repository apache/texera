from pydantic import BaseModel, Field
from typing import List, Optional, Literal


class Operator(BaseModel):
    operatorType: str
    operatorID: str
    operatorProperties: dict
    customDisplayName: Optional[str] = None

    class Config:
        json_schema_extra = {
            "required": ["operatorType", "operatorID", "operatorProperties"]
        }


class Port(BaseModel):
    operatorID: str
    portID: str

    class Config:
        json_schema_extra = {"required": ["operatorID", "portID"]}


class Link(BaseModel):
    linkID: str
    source: Port
    target: Port

    class Config:
        json_schema_extra = {"required": ["linkID", "source", "target"]}


class Changes(BaseModel):
    operatorsToAdd: List[Operator]
    linksToAdd: List[Link]
    operatorsToDelete: List[str]
    linksToDelete: List[str]

    class Config:
        json_schema_extra = {
            "required": [
                "operatorsToAdd",
                "linksToAdd",
                "operatorsToDelete",
                "linksToDelete",
            ]
        }


class Suggestion(BaseModel):
    suggestion: str
    suggestionType: Literal["fix", "improve"]
    changes: Changes

    class Config:
        json_schema_extra = {"required": ["suggestion", "suggestionType", "changes"]}


class SuggestionList(BaseModel):
    suggestions: List[Suggestion]

    class Config:
        json_schema_extra = {"required": ["suggestions"]}


class DataCleaningSuggestion(BaseModel):
    suggestion: str
    details: str

    class Config:
        json_schema_extra = {"required": ["suggestion", "details"]}
        extra = "forbid"


class DataCleaningSuggestionList(BaseModel):
    suggestions: List[DataCleaningSuggestion]

    class Config:
        json_schema_extra = {"required": ["suggestions"]}
        extra = "forbid"
