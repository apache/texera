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
export const TOOL_NAME_GET_CURRENT_WORKFLOW_COMPILATION_STATE = "getCurrentWorkflowCompilationState";

/**
 * Create addOperator tool for adding a new operator to the workflow
 */
export function createAddOperatorToCurrentWorkflowTool(
  workflowActionService: WorkflowActionService,
  workflowUtilService: WorkflowUtilService,
  operatorMetadataService: OperatorMetadataService
) {
  return tool({
    name: TOOL_NAME_ADD_OPERATOR_TO_CURRENT_WORKFLOW,
    description: "Add a new operator to the current workflow",
    inputSchema: z.object({
      operatorType: z.string().describe("Type of operator (e.g., 'CSVSource', 'Filter', 'Aggregate')"),
      customDisplayName: z
        .string()
        .optional()
        .describe("Brief custom name summarizing what this operator does in one sentence"),
    }),
    execute: async (args: { operatorType: string; customDisplayName?: string }) => {
      try {
        // Validate operator type exists
        if (!operatorMetadataService.operatorTypeExists(args.operatorType)) {
          return createErrorResult(`Unknown operator type: ${args.operatorType}.Use tools to see available types.`);
        }

        // Get a new operator predicate with default settings and optional custom display name
        const operator = workflowUtilService.getNewOperatorPredicate(args.operatorType, args.customDisplayName);

        // Calculate a default position (can be adjusted by auto-layout later)
        const existingOperators = workflowActionService.getTexeraGraph().getAllOperators();
        const defaultX = 100 + (existingOperators.length % 5) * 200;
        const defaultY = 100 + Math.floor(existingOperators.length / 5) * 150;
        const position = { x: defaultX, y: defaultY };

        // Add the operator to the workflow first
        workflowActionService.addOperator(operator, position);

        // Auto-layout the workflow after adding operator
        workflowActionService.autoLayoutWorkflow();

        // Show copilot is adding this operator (after it's added to graph)
        setTimeout(() => {}, 100);

        return createSuccessResult(
          {
            operatorId: operator.operatorID,
            message: `Added ${args.operatorType} operator to workflow`,
          },
          [],
          [operator.operatorID]
        );
      } catch (error: any) {
        return createErrorResult(error.message);
      }
    },
  });
}

/**
 * Create addLink tool for connecting two operators
 */
export function createAddLinkToCurrentWorkflowTool(workflowActionService: WorkflowActionService) {
  return tool({
    name: TOOL_NAME_ADD_LINK_TO_CURRENT_WORKFLOW,
    description: "Connect two operators with a link",
    inputSchema: z.object({
      sourceOperatorId: z.string().describe("ID of the source operator"),
      sourcePortId: z.string().optional().describe("Port ID on source operator (e.g., 'output-0')"),
      targetOperatorId: z.string().describe("ID of the target operator"),
      targetPortId: z.string().optional().describe("Port ID on target operator (e.g., 'input-0')"),
    }),
    execute: async (args: {
      sourceOperatorId: string;
      sourcePortId?: string;
      targetOperatorId: string;
      targetPortId?: string;
    }) => {
      try {
        // Default port IDs if not specified
        const sourcePId = args.sourcePortId || "output-0";
        const targetPId = args.targetPortId || "input-0";

        const link: OperatorLink = {
          linkID: `link_${Date.now()}`,
          source: {
            operatorID: args.sourceOperatorId,
            portID: sourcePId,
          },
          target: {
            operatorID: args.targetOperatorId,
            portID: targetPId,
          },
        };

        workflowActionService.addLink(link);

        // Auto-layout the workflow after adding link
        workflowActionService.autoLayoutWorkflow();

        return createSuccessResult(
          {
            linkId: link.linkID,
            message: `Connected ${args.sourceOperatorId}:${sourcePId} to ${args.targetOperatorId}:${targetPId}`,
          },
          [args.sourceOperatorId, args.targetOperatorId]
        );
      } catch (error: any) {
        return createErrorResult(error.message);
      }
    },
  });
}

/**
 * Create deleteOperator tool for removing an operator from the workflow
 */
export function createDeleteOperatorInCurrentWorkflowTool(workflowActionService: WorkflowActionService) {
  return tool({
    name: TOOL_NAME_DELETE_OPERATOR_IN_CURRENT_WORKFLOW,
    description: "Delete an operator from the current workflow",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to delete"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        workflowActionService.deleteOperator(args.operatorId);

        return createSuccessResult(
          {
            message: `Deleted operator ${args.operatorId}`,
          },
          [],
          [args.operatorId]
        );
      } catch (error: any) {
        return createErrorResult(error.message);
      }
    },
  });
}

/**
 * Create deleteLink tool for removing a link from the workflow
 */
export function createDeleteLinkInCurrentWorkflowTool(workflowActionService: WorkflowActionService) {
  return tool({
    name: TOOL_NAME_DELETE_LINK_IN_CURRENT_WORKFLOW,
    description: "Delete a link between two operators in the current workflow by link ID",
    inputSchema: z.object({
      linkId: z.string().describe("ID of the link to delete"),
    }),
    execute: async (args: { linkId: string }) => {
      try {
        workflowActionService.deleteLinkWithID(args.linkId);
        return createSuccessResult(
          {
            message: `Deleted link ${args.linkId}`,
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
 * Create setOperatorProperty tool for modifying operator properties
 */
export function createSetOperatorPropertyInCurrentWorkflowTool(
  workflowActionService: WorkflowActionService,
  validationWorkflowService: ValidationWorkflowService
) {
  return tool({
    name: TOOL_NAME_SET_OPERATOR_PROPERTY_IN_CURRENT_WORKFLOW,
    description:
      "Set or update properties of an operator in the current workflow. Properties must match the operator's schema. Use getOperatorPropertiesSchema first to understand required properties and their types.",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to modify"),
      properties: z.record(z.any()).describe("Properties object to set on the operator"),
    }),
    execute: async (args: { operatorId: string; properties: Record<string, any> }) => {
      try {
        // Set the properties first
        workflowActionService.setOperatorProperty(args.operatorId, args.properties);

        // Validate the operator after setting properties
        const validation = validationWorkflowService.validateOperator(args.operatorId);

        if (!validation.isValid) {
          // Properties are set but invalid - return error with details
          return createErrorResult(
            `Property validation failed: ${JSON.stringify(validation.messages)}. Use getOperatorPropertiesSchema tool to see the expected schema structure for this operator`
          );
        }

        // Show property was changed

        return createSuccessResult(
          {
            message: `Updated properties for operator ${args.operatorId}`,
            properties: args.properties,
          },
          [],
          [args.operatorId]
        );
      } catch (error: any) {
        return createErrorResult(error.message);
      }
    },
  });
}

/**
 * Create setPortProperty tool for modifying port properties
 */
export function createSetPortPropertyInCurrentWorkflowTool(
  workflowActionService: WorkflowActionService,
  validationWorkflowService: ValidationWorkflowService
) {
  return tool({
    name: TOOL_NAME_SET_PORT_PROPERTY_IN_CURRENT_WORKFLOW,
    description:
      "Set or update properties of a port on an operator in the current workflow (e.g., partition information, dependencies). Use getOperatorPortsInfo first to see available ports.",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator that owns the port"),
      portId: z.string().describe("ID of the port to modify (e.g., 'input-0', 'output-0')"),
      properties: z.record(z.any()).describe("Port properties to set (partitionInfo, dependencies)"),
    }),
    execute: async (args: { operatorId: string; portId: string; properties: Record<string, any> }) => {
      try {
        const logicalPort = {
          operatorID: args.operatorId,
          portID: args.portId,
        };

        // Set the port properties using the high-level service method
        workflowActionService.setPortProperty(logicalPort, args.properties);

        // Validate the operator after setting port properties
        const validation = validationWorkflowService.validateOperator(args.operatorId);

        if (!validation.isValid) {
          // Properties are set but invalid - return error with details
          return createErrorResult(
            `Port property validation failed: ${JSON.stringify(validation.messages)}. Use getOperatorPortsInfo tool to see the available ports and their current configuration`
          );
        }

        // Show property was changed

        return createSuccessResult(
          {
            message: `Updated port ${args.portId} properties for operator ${args.operatorId}`,
            properties: args.properties,
          },
          [],
          [args.operatorId]
        );
      } catch (error: any) {
        return createErrorResult(error.message);
      }
    },
  });
}

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
    description: "Get all operators in the current workflow",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const operators = workflowActionService.getTexeraGraph().getAllOperators();
        const operatorIds = operators.map(op => op.operatorID);
        return createSuccessResult(
          {
            operators: operators,
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
        // If no schema provided (empty array), return all operators with details
        if (!args.targetSchema || args.targetSchema.length === 0) {
          const allOperators = workflowActionService.getTexeraGraph().getAllOperators();
          const operatorDetails = allOperators.map(op => {
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
              message: `No specific schema provided. Returning all ${operatorDetails.length} operator(s) in the workflow with their schemas.`,
            },
            operatorIds,
            []
          );
        }

        const matchingOperatorIds = workflowActionService.findOperatorsByOutputSchema(
          args.targetSchema,
          workflowCompilingService
        );

        // Get detailed information for matching operators
        const operatorDetails = matchingOperatorIds.map(operatorID => {
          const operator = workflowActionService.getTexeraGraph().getOperator(operatorID);
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
            message: `Found ${operatorDetails.length} operator(s) with output schema matching the target attributes: ${args.targetSchema.map(attr => attr.attributeName).join(", ")}`,
          },
          matchingOperatorIds,
          []
        );
      } catch (error: any) {
        return createErrorResult(error.message || String(error));
      }
    },
  });
}

/**
 * Create getWorkflowCompilationState tool for checking compilation status and errors
 */
export function createGetCurrentWorkflowCompilationStateTool(workflowCompilingService: WorkflowCompilingService) {
  return tool({
    name: TOOL_NAME_GET_CURRENT_WORKFLOW_COMPILATION_STATE,
    description:
      "Get the current workflow compilation state and any compilation errors. Use this to check if the workflow is valid and identify any operator configuration issues.",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const compilationState = workflowCompilingService.getWorkflowCompilationState();
        const compilationErrors = workflowCompilingService.getWorkflowCompilationErrors();

        const hasErrors = Object.keys(compilationErrors).length > 0;

        return createSuccessResult(
          {
            state: compilationState,
            hasErrors: hasErrors,
            errors: hasErrors ? compilationErrors : undefined,
            message: hasErrors
              ? `Workflow compilation failed with ${Object.keys(compilationErrors).length} error(s)`
              : `Workflow compilation state: ${compilationState}`,
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
