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
import { OperatorMetadataService } from "../../operator-metadata/operator-metadata.service";
import { OperatorLink } from "../../../types/workflow-common.interface";
import { WorkflowUtilService } from "../../workflow-graph/util/workflow-util.service";
import { WorkflowCompilingService } from "../../compile-workflow/workflow-compiling.service";
import { ValidationWorkflowService } from "../../validation/validation-workflow.service";
import { createSuccessResult, createErrorResult } from "./tools-utility";

// Tool name constants
export const TOOL_NAME_LIST_OPERATORS_IN_CURRENT_WORKFLOW = "listOperatorsInCurrentWorkflow";
export const TOOL_NAME_LIST_CURRENT_LINKS = "listCurrentLinks";
export const TOOL_NAME_GET_CURRENT_OPERATOR = "getCurrentOperator";

/**
 * Create listLinksInCurrentWorkflow tool for getting all links in the workflow
 */
export function createListCurrentLinksTool(workflowActionService: WorkflowActionService) {
  return tool({
    name: TOOL_NAME_LIST_CURRENT_LINKS,
    description: "Get all links in the current workflow",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const links = workflowActionService.getTexeraGraph().getAllLinks();
        return createSuccessResult(
          {
            links: links,
            count: links.length,
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

export function createListOperatorsInCurrentWorkflowTool(workflowActionService: WorkflowActionService) {
  return tool({
    name: TOOL_NAME_LIST_OPERATORS_IN_CURRENT_WORKFLOW,
    description: "Get all operator IDs, types and custom names in the current workflow",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const operators = workflowActionService.getTexeraGraph().getAllOperators();
        const operatorIds = operators.map(op => op.operatorID);
        return createSuccessResult(
          {
            operators: operators.map(op => ({
              operatorId: op.operatorID,
              operatorType: op.operatorType,
              customDisplayName: op.customDisplayName,
            })),
            count: operators.length,
          },
          operatorIds,
          []
        );
      } catch (error: any) {
        return createErrorResult(error.message);
      }
    },
  });
}

export function createGetCurrentOperatorTool(
  workflowActionService: WorkflowActionService,
  workflowCompilingService: WorkflowCompilingService
) {
  return tool({
    name: TOOL_NAME_GET_CURRENT_OPERATOR,
    description:
      "Get detailed information about a specific operator in the current workflow, including its input and output schemas",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to retrieve"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        const operator = workflowActionService.getTexeraGraph().getOperator(args.operatorId);

        // Get input schema (empty map if not available)
        const inputSchemaMap = workflowCompilingService.getOperatorInputSchemaMap(args.operatorId);
        const inputSchema = inputSchemaMap || {};

        // Get output schema (empty map if not available)
        const outputSchemaMap = workflowCompilingService.getOperatorOutputSchemaMap(args.operatorId);
        const outputSchema = outputSchemaMap || {};

        return createSuccessResult(
          {
            operator: operator,
            inputSchema: inputSchema,
            outputSchema: outputSchema,
            message: `Retrieved operator ${args.operatorId}`,
          },
          [args.operatorId],
          []
        );
      } catch (error: any) {
        return createErrorResult(error.message || `Operator ${args.operatorId} not found`);
      }
    },
  });
}
