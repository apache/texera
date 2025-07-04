from model.llm.interpretation import (
    RawInterpretation,
    PathInterpretation,
    BaseInterpretation,
    InterpretationMethod,
)
from model.texera.TexeraWorkflow import TexeraWorkflow
from typing import Dict, Any, Optional

from model.web.input import CompilationStateInfo


class WorkflowInterpreter:
    def __init__(self, method: InterpretationMethod):
        self.method = method
        pass

    def interpret_workflow(
        self,
        workflow: Dict[str, Any],
        compilation_state_info: Optional[CompilationStateInfo] = None,
    ) -> BaseInterpretation:
        if compilation_state_info is None:
            input_schema = {}
            operator_errors = {}
        else:
            input_schema = {
                op_id: [
                    [attr.model_dump() for attr in schema] if schema else None
                    for schema in schemas
                ]
                for op_id, schemas in (
                    compilation_state_info.operatorInputSchemaMap or {}
                ).items()
            }
            operator_errors = compilation_state_info.operatorErrors or {}
        if self.method == InterpretationMethod.RAW:
            return self._interpret_raw(workflow, input_schema, operator_errors)
        elif self.method == InterpretationMethod.BY_PATH:
            return self._interpret_by_path(workflow, input_schema, operator_errors)
        else:
            raise ValueError(f"Unsupported interpretation method: {self.method}")

    def _interpret_raw(
        self,
        workflow: Dict[str, Any],
        input_schema: Dict[str, Any],
        operator_errors: Dict[str, Any],
    ) -> RawInterpretation:
        texera_workflow = TexeraWorkflow(
            workflow_dict=workflow,
            input_schema=input_schema,
            operator_errors=operator_errors,
        )
        return RawInterpretation(
            workflow=texera_workflow.ToPydantic(),
        )

    def _interpret_by_path(
        self,
        workflow: Dict[str, Any],
        input_schema: Dict[str, Any],
        operator_errors: Dict[str, Any],
    ) -> PathInterpretation:
        texera_workflow = TexeraWorkflow(
            workflow_dict=workflow,
            input_schema=input_schema,
            operator_errors=operator_errors,
        )
        paths = texera_workflow.get_all_paths()
        return PathInterpretation(workflow=texera_workflow.ToPydantic(), paths=paths)
