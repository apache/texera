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
export const TOOL_NAME_ADD_OPERATOR_TO_CURRENT_WORKFLOW = "addOperatorToCurrentWorkflow";
export const TOOL_NAME_ADD_LINK_TO_CURRENT_WORKFLOW = "addLinkToCurrentWorkflow";
export const TOOL_NAME_DELETE_OPERATOR_IN_CURRENT_WORKFLOW = "deleteOperatorInCurrentWorkflow";
export const TOOL_NAME_DELETE_LINK_IN_CURRENT_WORKFLOW = "deleteLinkInCurrentWorkflow";
export const TOOL_NAME_SET_OPERATOR_PROPERTY_IN_CURRENT_WORKFLOW = "setOperatorPropertyInCurrentWorkflow";
export const TOOL_NAME_SET_PORT_PROPERTY_IN_CURRENT_WORKFLOW = "setPortPropertyInCurrentWorkflow";
export const TOOL_NAME_LIST_OPERATORS_IN_CURRENT_WORKFLOW = "listOperatorsInCurrentWorkflow";
export const TOOL_NAME_LIST_CURRENT_LINKS = "listCurrentLinks";
export const TOOL_NAME_GET_CURRENT_OPERATOR = "getCurrentOperator";
export const TOOL_NAME_LIST_CURRENT_RELEVANT_OPERATOR_IDS = "listCurrentRelevantOperatorIds";
export const TOOL_NAME_GET_CURRENT_WORKFLOW = "getCurrentWorkflow";
export const TOOL_NAME_GET_CURRENT_WORKFLOW_COMPILATION_STATE = "getCurrentWorkflowCompilationState";

/**
 * Operator detail information including operatorProperties (not port properties)
 */
interface OperatorDetail {
  operatorId: string;
  operatorType: string;
  customDisplayName?: string;
  operatorProperties: Record<string, any>;
  inputSchema: Record<string, any>;
  outputSchema: Record<string, any>;
}

/**
 * Create unified getCurrentWorkflow tool that returns both operators and links
 * This merges the functionality of listCurrentLinks and listCurrentRelevantOperatorIds
 */
export function createGetCurrentWorkflowTool(
  workflowActionService: WorkflowActionService,
  workflowCompilingService: WorkflowCompilingService
) {
  return tool({
    name: TOOL_NAME_GET_CURRENT_WORKFLOW,
    description:
      "Get the current workflow structure including operators and links. " +
      "Returns a list of operators (with id, type, name, properties, input/output schemas) and a list of links. " +
      "Optionally filter to specific operator IDs. If no operatorIds provided, returns all enabled operators.",
    inputSchema: z.object({
      operatorIds: z
        .array(z.string())
        .optional()
        .describe(
          "Optional list of operator IDs to retrieve. If empty or not provided, returns all enabled operators in the workflow."
        ),
    }),
    execute: async (args: { operatorIds?: string[] }) => {
      try {
        const texeraGraph = workflowActionService.getTexeraGraph();

        // Get all links
        const links = texeraGraph.getAllLinks();

        // Determine which operators to return
        let operatorsToReturn: OperatorDetail[];

        if (args.operatorIds && args.operatorIds.length > 0) {
          // Filter to specific operator IDs
          const mappedOperators = args.operatorIds.map(operatorId => {
            try {
              const operator = texeraGraph.getOperator(operatorId);
              if (!operator) return null;

              const inputSchemaMap = workflowCompilingService.getOperatorInputSchemaMap(operatorId);
              const outputSchemaMap = workflowCompilingService.getOperatorOutputSchemaMap(operatorId);

              return {
                operatorId: operator.operatorID,
                operatorType: operator.operatorType,
                customDisplayName: operator.customDisplayName,
                operatorProperties: operator.operatorProperties,
                inputSchema: inputSchemaMap || {},
                outputSchema: outputSchemaMap || {},
              } as OperatorDetail;
            } catch {
              return null;
            }
          });
          operatorsToReturn = mappedOperators.filter(
            (op): op is NonNullable<typeof op> => op !== null
          ) as OperatorDetail[];
        } else {
          // Return all enabled operators
          const enabledOperators = texeraGraph.getAllEnabledOperators();
          operatorsToReturn = enabledOperators.map(operator => {
            const operatorId = operator.operatorID;
            const inputSchemaMap = workflowCompilingService.getOperatorInputSchemaMap(operatorId);
            const outputSchemaMap = workflowCompilingService.getOperatorOutputSchemaMap(operatorId);

            return {
              operatorId: operator.operatorID,
              operatorType: operator.operatorType,
              customDisplayName: operator.customDisplayName,
              operatorProperties: operator.operatorProperties,
              inputSchema: inputSchemaMap || {},
              outputSchema: outputSchemaMap || {},
            };
          });
        }

        const operatorIds = operatorsToReturn.map(op => op.operatorId);

        return createSuccessResult(
          {
            operators: operatorsToReturn,
            links: links,
            summary: {
              operatorCount: operatorsToReturn.length,
              linkCount: links.length,
            },
            message: `Retrieved ${operatorsToReturn.length} operator(s) and ${links.length} link(s) from the workflow.`,
          },
          operatorIds,
          []
        );
      } catch (error: any) {
        return createErrorResult(error.message || String(error));
      }
    },
  });
}
