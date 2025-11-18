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
import { ValidationWorkflowService } from "../../validation/validation-workflow.service";
import { WorkflowActionService } from "../../workflow-graph/model/workflow-action.service";
import { createSuccessResult, createErrorResult } from "./tools-utility";

// Tool name constants
export const TOOL_NAME_GET_CURRENT_WORKFLOW_VALIDATION_INFO = "getCurrentWorkflowValidationInfo";
export const TOOL_NAME_VALIDATE_CURRENT_OPERATOR = "validateCurrentOperator";

/**
 * Create getValidationInfoOfCurrentWorkflow tool for getting workflow validation information
 */
export function createGetCurrentWorkflowValidationInfoTool(
  validationWorkflowService: ValidationWorkflowService,
  workflowActionService: WorkflowActionService
) {
  return tool({
    name: TOOL_NAME_GET_CURRENT_WORKFLOW_VALIDATION_INFO,
    description:
      "Get all current validation errors in the workflow. This shows which operators have validation issues and what the errors are. Also returns lists of valid and invalid operator IDs. Use this to check if operators are properly configured before execution.",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const validationOutput = validationWorkflowService.getCurrentWorkflowValidationError();
        const errorCount = Object.keys(validationOutput.errors).length;

        const validGraph = validationWorkflowService.getValidTexeraGraph();
        const validOperators = validGraph.getAllOperators();
        const allOperators = workflowActionService.getTexeraGraph().getAllOperators();

        const validOperatorIds = validOperators.map(op => op.operatorID);
        const invalidOperatorIds = Object.keys(validationOutput.errors);
        const invalidCount = allOperators.length - validOperators.length;

        return createSuccessResult(
          {
            errors: validationOutput.errors,
            errorCount: errorCount,
            validOperatorIds: validOperatorIds,
            validCount: validOperators.length,
            totalCount: allOperators.length,
            invalidCount: invalidCount,
            message:
              errorCount === 0
                ? "No validation errors in the workflow"
                : `Found ${errorCount} operator(s) with validation errors. ${validOperators.length} valid operator(s) out of ${allOperators.length} total`,
          },
          [],
          []
        );
      } catch (error: any) {
        return createErrorResult(error.message);
      }
    },
  });
}

/**
 * Create validateOperator tool for validating a list of operators
 */
export function createValidateCurrentOperatorTool(validationWorkflowService: ValidationWorkflowService) {
  return tool({
    name: TOOL_NAME_VALIDATE_CURRENT_OPERATOR,
    description:
      "Validate a list of operators in the current workflow to check if they're properly configured. Returns an array of validation results with status and error messages for each operator.",
    inputSchema: z.object({
      operatorIds: z.array(z.string()).describe("Array of operator IDs to validate"),
    }),
    execute: async (args: { operatorIds: string[] }) => {
      try {
        const validationResults = args.operatorIds.map(operatorId => {
          const validation = validationWorkflowService.validateOperator(operatorId);

          if (validation.isValid) {
            return {
              operatorId: operatorId,
              isValid: true,
              message: `Operator ${operatorId} is valid`,
            };
          } else {
            return {
              operatorId: operatorId,
              isValid: false,
              message: Object.values(validation.messages).join("; "),
            };
          }
        });

        const validCount = validationResults.filter(r => r.isValid).length;
        const invalidCount = validationResults.length - validCount;

        return createSuccessResult(
          {
            results: validationResults,
            validCount: validCount,
            invalidCount: invalidCount,
            message:
              invalidCount === 0
                ? `All ${validCount} operator(s) are valid`
                : `${validCount} valid, ${invalidCount} invalid out of ${validationResults.length} operator(s)`,
          },
          args.operatorIds,
          []
        );
      } catch (error: any) {
        return createErrorResult(error.message);
      }
    },
  });
}
