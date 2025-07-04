from typing import Dict, List, Any, Optional
import json
import uuid
import os
from dotenv import load_dotenv

from model.llm.suggestion import SuggestionList, DataCleaningSuggestionList
from model.web.input import (
    CompilationStateInfo,
    ExecutionStateInfo,
    TableProfileSuggestionRequest,
    SuggestionRequest,
)
from workflow_interpretation.interpreter import (
    WorkflowInterpreter,
)
from model.llm.prompt import SuggestionPrompt, DataCleaningSuggestionPrompt
from model.llm.interpretation import (
    PathInterpretation,
    RawInterpretation,
    OperatorInterpretation,
    BaseInterpretation,
    InterpretationMethod,
    AttributeInterpretation,
    SchemaInterpretation,
)
from llm_agent.base import LLMAgentFactory
from distutils.util import strtobool  # stdlib helper

# Load environment variables from .env file if present
load_dotenv()

logs_directory = "logs"


# helper that treats 1/true/yes/y (case‑insensitive) as True
def env_bool(key: str, default: bool = False) -> bool:
    try:
        return bool(strtobool(os.getenv(key, str(default))))
    except ValueError:
        return default


class SuggestionGenerator:
    """
    SuggestionGenerator is responsible for generating workflow suggestions
    based on the current workflow state, compilation information, and result data.
    """

    def __init__(self):
        """
        Initialize the suggestion generator.
        """
        self.workflow_interpretation_method = InterpretationMethod(
            os.environ.get("INTERPRETATION_METHOD")
        )
        self.workflow_interpreter = WorkflowInterpreter(
            self.workflow_interpretation_method
        )

        # Determine provider and model
        self.llm_provider = os.environ.get("LLM_PROVIDER")

        if self.llm_provider == "openai":
            self.llm_model = os.environ.get("OPENAI_MODEL")
        elif self.llm_provider == "anthropic":
            self.llm_model = os.environ.get("ANTHROPIC_MODEL")

        # Create the LLM agent
        try:
            extra_params = {}

            if self.llm_provider == "openai":
                tools = []
                vector_store_ids_raw = os.environ.get("OPENAI_VECTOR_STORE_IDS", "")
                vector_store_ids = [
                    v.strip() for v in vector_store_ids_raw.split(",") if v.strip()
                ]
                if vector_store_ids:
                    tools.append(
                        {"type": "file_search", "vector_store_ids": vector_store_ids}
                    )
                extra_params["tools"] = tools
                extra_params["project"] = os.environ.get("OPENAI_PROJECT_ID")
                extra_params["organization"] = os.environ.get("OPENAI_ORG_ID")
                extra_params["use_function_calls"] = env_bool(
                    "OPENAI_USE_FUNCTION_CALLS", default=True
                )
            self.llm_agent = LLMAgentFactory.create(
                self.llm_provider, model=self.llm_model, **extra_params
            )
        except ValueError as e:
            print(f"Error creating LLM agent: {str(e)}")
            self.llm_agent = None

    def generate_suggestions(
        self,
        request: SuggestionRequest,
        enable_logging: bool = True,
    ) -> SuggestionList:
        """
        Generate workflow suggestions based on the current workflow, compilation state, execution state, and result tables.

        Returns:
            A list of workflow suggestions
        """
        # If LLM generation failed or agent is not available, return mock suggestions
        if not self.llm_agent:
            return SuggestionList(suggestions=[])
        workflow_json = json.loads(request.workflow)
        # Generate natural language description of the workflow
        interpretation = self.workflow_interpreter.interpret_workflow(
            workflow_json,
            request.compilationState,
        )

        # Determine intention (fallback)
        intention = request.intention
        if not intention:
            intention = "Recommend improvements and fixes of current workflows"

        workflow_intp = interpretation.get_base_workflow_interpretation()
        focusing_operators: List[OperatorInterpretation] = []
        for oid in request.focusingOperatorIDs:
            op = workflow_intp.operators.get(oid)
            if op:
                output_attributes = request.operatorIDToTableSchemaMap.get(oid, [])
                op.outputSchema = SchemaInterpretation(attributes=output_attributes)
                focusing_operators.append(op)

        prompt_obj = SuggestionPrompt(
            intention=intention,
            focusingOperators=focusing_operators,
            workflowInterpretation=interpretation,
        )

        # Serialize prompt to JSON string to feed into LLM
        workflow_description = prompt_obj.model_dump_json(indent=2)

        log_id: Optional[str] = None
        # Save the workflow description to a log file if logging is enabled
        if enable_logging:
            try:
                os.makedirs(logs_directory, exist_ok=True)
                log_id = str(uuid.uuid4())
                file_name = f"{log_id}.json"
                file_path = os.path.join(logs_directory, file_name)
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(workflow_description)
            except Exception as e:
                print(f"Failed to log workflow description: {e}")

        # Get suggestions from the LLM agent
        suggestions = self.llm_agent.generate_suggestions(
            prompt=prompt_obj,
            temperature=0.7,  # Lower temperature for more focused suggestions
        )
        if enable_logging and log_id:
            try:
                with open(
                    os.path.join(logs_directory, f"response-{log_id}.json"),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(suggestions.model_dump_json(indent=2))
            except Exception as e:
                print(f"Failed to log suggestions: {e}")
        return suggestions

    def generate_data_cleaning_suggestions(
        self,
        request: TableProfileSuggestionRequest,
        enable_logging: bool = True,
    ) -> DataCleaningSuggestionList:
        """
        Build a DataCleaningSuggestionPrompt from the table profile + target column
        and ask the LLM agent for cleaning recommendations.

        Args:
            request:  TableProfileSuggestionRequest containing full table profile
                      and the column we need to clean.
            enable_logging:  if True, raw prompt / response JSON are written to
                             the `logs/` folder for offline inspection.

        Returns:
            DataCleaningSuggestionList with zero or more natural-language
            cleaning suggestions.
        """
        # Bail out early if no LLM agent has been configured.
        if not self.llm_agent:
            return DataCleaningSuggestionList(suggestions=[])

        tp = request.tableProfile  # proto TableProfile
        target_col = request.targetColumnName

        # -------------------- 1️⃣  locate the target ColumnProfile ------------
        target_profile = None
        for col in tp.column_profiles:
            # ColumnProfile in proto keeps the original field name `columnName`
            if getattr(col, "column_name", None) == target_col:
                target_profile = col
                break

        # If not found we cannot proceed – return empty result.
        if target_profile is None:
            return DataCleaningSuggestionList(suggestions=[])

        # -------------------- 3️⃣  compose the cleaning prompt ----------------
        prompt_obj = DataCleaningSuggestionPrompt(
            focusingOperatorID=request.focusingOperatorID,  # not available in this request
            columnProfile=target_profile,
            tableSchema=SchemaInterpretation(attributes=request.tableSchema),
        )

        prompt_json = prompt_obj.model_dump_json(indent=2)

        # Optional logging for debugging
        log_id: Optional[str] = None
        if enable_logging:
            try:
                os.makedirs(logs_directory, exist_ok=True)
                log_id = str(uuid.uuid4())
                with open(
                    os.path.join(logs_directory, f"cleaning-{log_id}.json"),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(prompt_json)
            except Exception as e:
                print(f"Failed to log cleaning prompt: {e}")

        # -------------------- 4️⃣  call the LLM agent -------------------------
        suggestions = self.llm_agent.generate_data_cleaning_suggestions(
            prompt=prompt_obj,
            temperature=0.4,  # a bit more deterministic
        )

        if enable_logging and log_id:
            try:
                with open(
                    os.path.join(logs_directory, f"cleaning-response-{log_id}.json"),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(suggestions.model_dump_json(indent=2))
            except Exception as e:
                print(f"Failed to log cleaning suggestions: {e}")

        return suggestions

    def _enhance_prompt_with_state_info(
        self,
        workflow_description: str,
        compilation_state: Dict[str, Any],
        execution_state: Optional[Dict[str, Any]],
    ) -> str:
        """
        Enhance the workflow description with compilation and execution state information.

        Args:
            workflow_description: Natural language description of the workflow
            compilation_state: Compilation information and errors
            execution_state: Current execution state of the workflow

        Returns:
            Enhanced workflow description
        """
        prompt = workflow_description + "\n\n"

        # Add compilation state info
        prompt += f"Compilation State: {compilation_state['state']}\n"

        # Add compilation errors if any
        if compilation_state["state"] == "Failed" and compilation_state.get(
            "operatorErrors"
        ):
            prompt += "Compilation Errors:\n"
            for op_id, error in compilation_state["operatorErrors"].items():
                if error:
                    prompt += f"- Operator {op_id}: {error}\n"

        # Add execution state info if available
        if execution_state:
            prompt += f"\nExecution State: {execution_state['state']}\n"

            # Add execution errors if any
            if execution_state["state"] == "Failed" and execution_state.get(
                "errorMessages"
            ):
                prompt += "Execution Errors:\n"
                for error in execution_state["errorMessages"]:
                    prompt += f"- {error}\n"

        # Add final instruction
        prompt += "\nBased on this workflow description and state information, suggest improvements or fixes."

        return prompt
