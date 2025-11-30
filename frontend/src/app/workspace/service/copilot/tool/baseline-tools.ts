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
import { ExecuteWorkflowService } from "../../execute-workflow/execute-workflow.service";
import { ValidationWorkflowService } from "../../validation/validation-workflow.service";
import { WorkflowConsoleService } from "../../workflow-console/workflow-console.service";
import { WorkflowStatusService } from "../../workflow-status/workflow-status.service";
import { WorkflowResultService } from "../../workflow-result/workflow-result.service";
import { ActionPlanService } from "../../action-plan/action-plan.service";
import { createSuccessResult, createErrorResult } from "./tools-utility";
import { executeWorkflowAndGetResults, WorkflowExecutionServices } from "./current-workflow-execution-tools";

// Tool name constants for baseline mode
export const TOOL_NAME_CREATE_PYTHON_UDF = "createPythonUDF";

/**
 * Create a tool for adding a PythonUDFSource operator with code
 * This is the primary tool for baseline mode - it creates a Python UDF Source and sets its code.
 * The operator uses GenerateOperator class with produce() method that uses print() for output
 * and yields nothing at the end.
 * After creating the operator, it automatically executes the workflow and retrieves console logs.
 */
export function createPythonUDFTool(
  workflowActionService: WorkflowActionService,
  workflowUtilService: WorkflowUtilService,
  operatorMetadataService: OperatorMetadataService,
  executeWorkflowService: ExecuteWorkflowService,
  validationWorkflowService: ValidationWorkflowService,
  workflowConsoleService: WorkflowConsoleService,
  workflowStatusService: WorkflowStatusService,
  workflowResultService: WorkflowResultService,
  actionPlanService: ActionPlanService,
  agentId: string = "",
  agentName: string = ""
) {
  // Build services object for execution
  const executionServices: WorkflowExecutionServices = {
    executeWorkflowService,
    validationWorkflowService,
    workflowActionService,
    workflowConsoleService,
    workflowStatusService,
    workflowResultService,
  };
  return tool({
    name: TOOL_NAME_CREATE_PYTHON_UDF,
    description:
      "Create a new PythonUDFSource operator with the specified Python code. " +
      "This is the primary tool for data analysis in baseline mode. " +
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

        // Capture workflow state before adding operator for action plan
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

        // Capture workflow state after adding operator for action plan
        const afterContent = workflowActionService.getWorkflowContent();

        // Create action plan to record this addition
        const actionPlan = actionPlanService.createActionPlan(
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

        // Execute the workflow and get console logs (baseline mode: no operator results, only console logs)
        const executionResult = await executeWorkflowAndGetResults(executionServices, {
          executionName: args.customDisplayName || "Baseline Python Analysis",
          targetOperatorId: operator.operatorID,
          includeOperatorResults: false, // Baseline mode doesn't include operator results
        });

        // Get console logs for the newly created operator
        const operatorConsoleLogs = executionResult.consoleLogs?.[operator.operatorID] || [];

        if (executionResult.success) {
          return createSuccessResult(
            {
              operatorId: operator.operatorID,
              actionPlanId: actionPlan.id,
              message: `Created and executed PythonUDFSource operator${args.customDisplayName ? ` "${args.customDisplayName}"` : ""}`,
              code: args.code,
              executionState: executionResult.executionState,
              consoleLogs: operatorConsoleLogs,
              allConsoleLogs: executionResult.consoleLogs,
              operatorStates: executionResult.operatorStates,
            },
            [],
            [operator.operatorID]
          );
        } else {
          // Even on execution failure, return the operator info with error details
          return createSuccessResult(
            {
              operatorId: operator.operatorID,
              actionPlanId: actionPlan.id,
              message: `Created PythonUDFSource operator${args.customDisplayName ? ` "${args.customDisplayName}"` : ""}, but execution failed`,
              code: args.code,
              executionState: executionResult.executionState,
              executionError: executionResult.error,
              consoleLogs: operatorConsoleLogs,
              allConsoleLogs: executionResult.consoleLogs,
              operatorStates: executionResult.operatorStates,
            },
            [],
            [operator.operatorID]
          );
        }
      } catch (error: unknown) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        return createErrorResult(errorMessage);
      }
    },
  });
}
