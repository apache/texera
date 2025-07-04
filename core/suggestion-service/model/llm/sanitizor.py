from pydantic import BaseModel
from typing import Dict, List, Optional, Any

from model.llm.suggestion import Suggestion, SuggestionList


class OperatorSchema(BaseModel):
    operatorType: str
    jsonSchema: dict


class SuggestionSanitization(BaseModel):
    suggestions: SuggestionList
    schemas: List[OperatorSchema]
