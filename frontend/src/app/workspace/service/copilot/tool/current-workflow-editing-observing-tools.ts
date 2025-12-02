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

/**
 * Tool to find operators by output schema
 * This helps the agent identify relevant operators that produce data matching a specific schema
 * Returns detailed operator information including input/output schemas and custom display names
 */
export function createListCurrentRelevantOperatorIdsTool(
  workflowActionService: WorkflowActionService,
  workflowCompilingService: WorkflowCompilingService
) {
  return tool({
    name: TOOL_NAME_LIST_CURRENT_RELEVANT_OPERATOR_IDS,
    description:
      "Find all operators in the workflow that are relevant context when working with specific data schemas. " +
      "Returns detailed operator information including operatorId, operatorType, customDisplayName, inputSchema, and outputSchema. " +
      "Please use this method when you want to work on certain columns in the data. " +
      "If you don't have a specific scope to work on, provide an empty schema array to get all operators with their detailed information.",
    inputSchema: z.object({
      targetSchema: z
        .array(
          z.object({
            attributeName: z.string().describe("Name of the attribute to match"),
            attributeType: z
              .enum(["string", "integer", "double", "boolean", "long", "timestamp", "binary"])
              .describe("Type of the attribute"),
          })
        )
        .describe(
          "Array of schema attributes to match. Operators whose output contains all these attributes (in any order) will be returned. " +
            "Pass an empty array to get all operator IDs in the workflow."
        ),
    }),
    execute: async (args: { targetSchema: Array<{ attributeName: string; attributeType: string }> }) => {
      try {
        const texeraGraph = workflowActionService.getTexeraGraph();

        // If no schema provided (empty array), return all enabled operators with details
        if (!args.targetSchema || args.targetSchema.length === 0) {
          // Use getAllEnabledOperators to filter out disabled operators
          const enabledOperators = texeraGraph.getAllEnabledOperators();
          const operatorDetails = enabledOperators.map(op => {
            const operatorID = op.operatorID;
            const inputSchemaMap = workflowCompilingService.getOperatorInputSchemaMap(operatorID);
            const outputSchemaMap = workflowCompilingService.getOperatorOutputSchemaMap(operatorID);

            return {
              operatorId: operatorID,
              operatorType: op.operatorType,
              customDisplayName: op.customDisplayName,
              inputSchema: inputSchemaMap || {},
              outputSchema: outputSchemaMap || {},
            };
          });

          const operatorIds = operatorDetails.map(op => op.operatorId);
          return createSuccessResult(
            {
              operators: operatorDetails,
              count: operatorDetails.length,
              message: `No specific schema provided. Returning all ${operatorDetails.length} enabled operator(s) in the workflow with their schemas.`,
            },
            operatorIds,
            []
          );
        }

        const matchingOperatorIds = workflowActionService.findOperatorsByOutputSchema(
          args.targetSchema,
          workflowCompilingService
        );

        // Filter out disabled operators from matching results
        const enabledMatchingOperatorIds = matchingOperatorIds.filter(
          operatorID => !texeraGraph.isOperatorDisabled(operatorID)
        );

        // Get detailed information for matching enabled operators
        const operatorDetails = enabledMatchingOperatorIds.map(operatorID => {
          const operator = texeraGraph.getOperator(operatorID);
          const inputSchemaMap = workflowCompilingService.getOperatorInputSchemaMap(operatorID);
          const outputSchemaMap = workflowCompilingService.getOperatorOutputSchemaMap(operatorID);

          return {
            operatorId: operatorID,
            operatorType: operator?.operatorType,
            customDisplayName: operator?.customDisplayName,
            inputSchema: inputSchemaMap || {},
            outputSchema: outputSchemaMap || {},
          };
        });

        return createSuccessResult(
          {
            operators: operatorDetails,
            count: operatorDetails.length,
            message: `Found ${operatorDetails.length} enabled operator(s) with output schema matching the target attributes: ${args.targetSchema.map(attr => attr.attributeName).join(", ")}`,
          },
          enabledMatchingOperatorIds,
          []
        );
      } catch (error: any) {
        return createErrorResult(error.message || String(error));
      }
    },
  });
}

/**
 * Operator detail information including properties (not port properties)
 */
interface OperatorDetail {
  operatorId: string;
  operatorType: string;
  customDisplayName?: string;
  properties: Record<string, any>;
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

              // Extract operator properties (excluding internal fields)
              const { operatorID, operatorType, operatorVersion, customDisplayName, ...properties } = operator;

              return {
                operatorId: operatorID,
                operatorType: operatorType,
                customDisplayName: customDisplayName,
                properties: properties,
                inputSchema: inputSchemaMap || {},
                outputSchema: outputSchemaMap || {},
              } as OperatorDetail;
            } catch {
              return null;
            }
          });
          operatorsToReturn = mappedOperators.filter((op): op is NonNullable<typeof op> => op !== null) as OperatorDetail[];
        } else {
          // Return all enabled operators
          const enabledOperators = texeraGraph.getAllEnabledOperators();
          operatorsToReturn = enabledOperators.map(operator => {
            const operatorId = operator.operatorID;
            const inputSchemaMap = workflowCompilingService.getOperatorInputSchemaMap(operatorId);
            const outputSchemaMap = workflowCompilingService.getOperatorOutputSchemaMap(operatorId);

            // Extract operator properties (excluding internal fields)
            const { operatorID, operatorType, operatorVersion, customDisplayName, ...properties } = operator;

            return {
              operatorId: operatorID,
              operatorType: operatorType,
              customDisplayName: customDisplayName,
              properties: properties,
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
