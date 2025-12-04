/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { z } from "zod";
import { tool } from "ai";
import { WorkflowActionService } from "../../workflow-graph/model/workflow-action.service";
import { WorkflowUtilService } from "../../workflow-graph/util/workflow-util.service";
import { OperatorMetadataService } from "../../operator-metadata/operator-metadata.service";
import { AgentActionService } from "../../agent-action/agent-action.service";
import { ExecuteWorkflowService } from "../../execute-workflow/execute-workflow.service";
import { ValidationWorkflowService } from "../../validation/validation-workflow.service";
import { WorkflowCompilingService } from "../../compile-workflow/workflow-compiling.service";
import { WorkflowConsoleService } from "../../workflow-console/workflow-console.service";
import { WorkflowStatusService } from "../../workflow-status/workflow-status.service";
import { WorkflowResultService } from "../../workflow-result/workflow-result.service";
import { createSuccessResult, createErrorResult } from "./tools-utility";
import { executeWorkflowAndGetResults$, WorkflowExecutionServices } from "./current-workflow-execution-tools";
import { firstValueFrom } from "rxjs";

// Tool name constants for baseline mode
export const TOOL_NAME_CREATE_PYTHON_UDF = "createPythonUDF";
export const TOOL_NAME_EXECUTE_TO_OPERATOR = "executeToOperator";

/**
 * Create a tool for adding a PythonUDFSource operator with code
 * This tool only creates the operator - use executeToOperator to run it.
 * The operator uses GenerateOperator class with produce() method that uses print() for output
 * and yields nothing at the end.
 */
export function createPythonUDFTool(
  workflowActionService: WorkflowActionService,
  workflowUtilService: WorkflowUtilService,
  operatorMetadataService: OperatorMetadataService,
  agentActionService: AgentActionService,
  agentId: string = "",
  agentName: string = ""
) {
  return tool({
    name: TOOL_NAME_CREATE_PYTHON_UDF,
    description:
      "Create a new PythonUDFSource operator with the specified Python code. " +
      "This tool only creates the operator - use executeToOperator to run it. " +
      "The code should use the DatasetFileDocument API to read data and perform analysis. " +
      "Use the GenerateOperator class template with the produce() method. " +
      "Use print() to output results and always end with an empty yield.",
    inputSchema: z.object({
      code: z
        .string()
        .describe(
          "The Python code for the UDF Source. Must define a GenerateOperator class extending UDFSourceOperator with produce() method. " +
            "Use DatasetFileDocument from pytexera.storage to read dataset files. " +
            "Use print() to output results and always end with 'yield' (empty yield, no data)."
        ),
      customDisplayName: z
        .string()
        .optional()
        .describe("Brief custom name summarizing what this Python UDF Source does (e.g., 'Find Negative Prices')"),
    }),
    execute: async (args: { code: string; customDisplayName?: string }) => {
      try {
        const operatorType = "PythonUDFSourceV2";

        // Validate operator type exists
        if (!operatorMetadataService.operatorTypeExists(operatorType)) {
          return createErrorResult("PythonUDFSourceV2 operator type not found in this Texera installation.");
        }

        // Capture workflow state before adding operator for agent action
        const beforeContent = workflowActionService.getWorkflowContent();

        // Get a new operator predicate with default settings and optional custom display name
        const operator = workflowUtilService.getNewOperatorPredicate(operatorType, args.customDisplayName);

        // Calculate a default position
        const existingOperators = workflowActionService.getTexeraGraph().getAllOperators();
        const defaultX = 100 + (existingOperators.length % 5) * 200;
        const defaultY = 100 + Math.floor(existingOperators.length / 5) * 150;
        const position = { x: defaultX, y: defaultY };

        // Add the operator to the workflow first
        workflowActionService.addOperator(operator, position);

        // Set the code property for the Python UDF Source
        const properties = {
          code: args.code,
        };
        workflowActionService.setOperatorProperty(operator.operatorID, properties);

        // Auto-layout the workflow after adding operator
        workflowActionService.autoLayoutWorkflow();

        // Capture workflow state after adding operator for agent action
        const afterContent = workflowActionService.getWorkflowContent();

        // Create agent action to record this addition
        const agentAction = agentActionService.createAgentAction(
          agentId,
          agentName || "Baseline Agent",
          args.customDisplayName || "Add PythonUDFSource for data analysis",
          {
            add: { operatorIds: [operator.operatorID], linkIds: [] },
            modify: { operatorIds: [] },
            delete: { operatorIds: [], linkIds: [] },
          },
          [operator.operatorID],
          [],
          workflowActionService.getWorkflowMetadata(),
          beforeContent,
          afterContent
        );

        return createSuccessResult(
          {
            operatorId: operator.operatorID,
            agentActionId: agentAction.id,
            message: `Created PythonUDFSource operator${args.customDisplayName ? ` "${args.customDisplayName}"` : ""}. Use executeToOperator to run it.`,
            code: args.code,
          },
          [],
          [operator.operatorID]
        );
      } catch (error: unknown) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        return createErrorResult(errorMessage);
      }
    },
  });
}

/**
 * Create a tool for executing the workflow up to a specific operator and retrieving results.
 * This tool executes the workflow in "executeTo" mode, running only the operators
 * necessary to produce results for the target operator.
 */
export function createExecuteToOperatorTool(
  executeWorkflowService: ExecuteWorkflowService,
  validationWorkflowService: ValidationWorkflowService,
  workflowCompilingService: WorkflowCompilingService,
  workflowActionService: WorkflowActionService,
  workflowConsoleService: WorkflowConsoleService,
  workflowStatusService: WorkflowStatusService,
  workflowResultService: WorkflowResultService
) {
  const executionServices: WorkflowExecutionServices = {
    executeWorkflowService,
    validationWorkflowService,
    workflowCompilingService,
    workflowActionService,
    workflowConsoleService,
    workflowStatusService,
    workflowResultService,
  };

  return tool({
    name: TOOL_NAME_EXECUTE_TO_OPERATOR,
    description:
      "Execute the workflow up to a specific operator and retrieve results. " +
      "This runs only the operators necessary to produce results for the target operator. " +
      "Returns console logs and execution state for the target operator.",
    inputSchema: z.object({
      operatorId: z
        .string()
        .describe(
          "The ID of the operator to execute up to. The workflow will run all upstream operators needed to produce results for this operator."
        ),
      executionName: z.string().optional().describe("Optional name for this execution (for logging/display purposes)"),
    }),
    execute: async (args: { operatorId: string; executionName?: string }) => {
      try {
        // Validate that the operator exists in the workflow
        const allOperators = workflowActionService.getTexeraGraph().getAllOperators();
        const targetOperator = allOperators.find(op => op.operatorID === args.operatorId);

        if (!targetOperator) {
          return createErrorResult(`Operator with ID "${args.operatorId}" not found in the workflow.`);
        }

        // Execute the workflow up to the target operator
        const executionResult = await firstValueFrom(
          executeWorkflowAndGetResults$(executionServices, {
            executionName: args.executionName || `Execute to ${targetOperator.customDisplayName || args.operatorId}`,
            targetOperatorIds: [args.operatorId],
            includeOperatorResults: false, // Baseline mode: console logs only
          })
        );

        // Get console logs for the target operator
        const operatorConsoleLogs = executionResult.consoleLogs?.[args.operatorId] || [];

        if (executionResult.success) {
          return createSuccessResult(
            {
              operatorId: args.operatorId,
              message: `Executed workflow up to operator "${targetOperator.customDisplayName || args.operatorId}"`,
              executionState: executionResult.executionState,
              consoleLogs: operatorConsoleLogs,
              allConsoleLogs: executionResult.consoleLogs,
              operatorStates: executionResult.operatorStates,
            },
            [],
            [args.operatorId]
          );
        } else {
          return createSuccessResult(
            {
              operatorId: args.operatorId,
              message: `Execution failed for operator "${targetOperator.customDisplayName || args.operatorId}"`,
              executionState: executionResult.executionState,
              executionError: executionResult.error,
              consoleLogs: operatorConsoleLogs,
              allConsoleLogs: executionResult.consoleLogs,
              operatorStates: executionResult.operatorStates,
            },
            [],
            [args.operatorId]
          );
        }
      } catch (error: unknown) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        return createErrorResult(errorMessage);
      }
    },
  });
}
