"""
Workflow Interpretation and Suggestion CLI Utility
"""

import json
import os
from typing import List, Optional
from pathlib import Path

from dotenv import load_dotenv

from workflow_interpretation.interpreter import (
    WorkflowInterpreter,
)
from model.llm.interpretation import InterpretationMethod
from suggestion_engine.generator import SuggestionGenerator
from model.web.input import SuggestionRequest, CompilationStateInfo, ExecutionStateInfo

# Base directory for test data and results
DATA_DIR = Path("test/data")
RESULTS_DIR = Path("test/results")
load_dotenv()


def load_workflow_data(dir_name: str) -> SuggestionRequest:
    """
    Load workflow-related JSON files from a given directory.

    Args:
        dir_name: Subdirectory name under test/data

    Returns:
        SuggestionRequest object containing all loaded workflow components.
    """
    path = DATA_DIR / dir_name

    workflow = open(path / "workflow.json").read()
    compilation_state = json.load(open(path / "workflow_compilation_state.json"))
    execution_state = json.load(open(path / "execution_state.json"))

    return SuggestionRequest(
        workflow=workflow,
        compilationState=CompilationStateInfo.model_validate(compilation_state),
        executionState=ExecutionStateInfo.model_validate(execution_state),
    )


def interpret_workflow(dir_name: str):
    """
    Run interpretation on a workflow and save the output.

    Args:
        dir_name: Workflow folder name
    """
    print(f"\n🧠 Interpreting workflow: {dir_name}")

    request = load_workflow_data(dir_name)

    result_dir = RESULTS_DIR / dir_name
    os.makedirs(result_dir, exist_ok=True)

    # RAW
    raw_text = WorkflowInterpreter(InterpretationMethod.RAW).interpret_workflow(
        json.loads(request.workflow), request.compilationState
    )
    with open(result_dir / "raw_interpretation.txt", "w") as f:
        f.write(raw_text.model_dump_json())

    print("\n📄 Raw interpretation saved.")

    # BY_PATH
    by_path_text = WorkflowInterpreter(InterpretationMethod.BY_PATH).interpret_workflow(
        json.loads(request.workflow), request.compilationState
    )
    with open(result_dir / "by_path_interpretation.txt", "w") as f:
        f.write(by_path_text.model_dump_json())

    print("📄 By-path interpretation saved.")


def generate_suggestions(
    dir_name: str,
    intention: str = "",
    focusing_operator_ids: Optional[List[str]] = None,
):
    """
    Generate suggestions from a workflow.

    Args:
        dir_name: Workflow folder name
        intention: User's intention for the suggestion
        focusing_operator_ids: List of operator IDs to focus on
    """
    print(f"\n💡 Generating suggestions for: {dir_name}")
    print(f"  Intention: {intention or '(empty)'}")
    print(f"  Focusing operators: {focusing_operator_ids or '[]'}")

    request = load_workflow_data(dir_name)
    generator = SuggestionGenerator()

    result_dir = RESULTS_DIR / dir_name
    os.makedirs(result_dir, exist_ok=True)

    # Update request with any custom parameters
    if intention:
        request.intention = intention
    if focusing_operator_ids:
        request.focusingOperatorIDs = focusing_operator_ids

    suggestions = generator.generate_suggestions(
        request.workflow,
        request.compilationState,
        request.executionState,
        request.intention,
        request.focusingOperatorIDs,
    )

    output_file = result_dir / "suggestions.json"
    with open(output_file, "w") as f:
        f.write(suggestions.model_dump_json(indent=2))

    print(f"✅ {len(suggestions.suggestions)} suggestions written to {output_file}")


def run_all():
    """Run interpretation and suggestion on all workflows with specific test cases."""
    # Dictionary of test cases with their specific parameters
    test_cases = {
        "workflow1": {
            "intention": "",
            "focusing_operator_ids": [],
        },
        "workflow2": {
            "intention": "Suggest good recommendation techniques",
            "focusing_operator_ids": [
                "PythonUDFV2-operator-29189f24-4d27-413f-9b67-1c8b37529289"
            ],
        },
        "workflow3": {
            "intention": "Set the parameter of the operator to do visualization correctly",
            "focusing_operator_ids": [
                "LineChart-operator-e3009841-32e4-4080-a6e4-f659762b3865"
            ],
        },
    }

    os.makedirs(RESULTS_DIR, exist_ok=True)

    for dir_name, params in test_cases.items():
        print(f"\n{'='*60}")
        print(f"▶ Running for workflow: {dir_name}")
        print(f"{'='*60}")

        interpret_workflow(dir_name)
        generate_suggestions(
            dir_name, params["intention"], params["focusing_operator_ids"]
        )


if __name__ == "__main__":
    run_all()
