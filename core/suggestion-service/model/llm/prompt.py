from pydantic import BaseModel, Field
from typing import List, Union

from model.llm.interpretation import (
    OperatorInterpretation,
    BaseInterpretation,
    PathInterpretation,
    RawInterpretation,
    SchemaInterpretation,
)

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


class SuggestionPrompt(BaseModel):
    """Prompt structure sent to the LLM agent.

    It combines the user intention, the operators that the user wishes to focus on,
    and the interpretation of the whole workflow.
    """

    # A short sentence or question describing what the user wants from the LLM.
    intention: str = Field(
        "Recommend improvements and fixes of current workflows",
        description="The user's intention for generating suggestions.",
    )

    # The subset of operators that the user explicitly wants the LLM to pay extra attention to.
    focusingOperators: List[OperatorInterpretation] = Field(
        default_factory=list,
        description="Operators that the user is focusing on.",
    )

    # Full interpretation of the workflow (either PathInterpretation or RawInterpretation)
    workflowInterpretation: Union[PathInterpretation, RawInterpretation] = Field(
        ..., description="Interpretation of the complete workflow."
    )


class DataCleaningSuggestionPrompt(BaseModel):
    focusingOperatorID: str
    columnProfile: ColumnProfile
    tableSchema: SchemaInterpretation
