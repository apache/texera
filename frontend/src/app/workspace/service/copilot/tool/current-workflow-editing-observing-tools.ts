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

/**
 * Create addOperator tool for adding a new operator to the workflow
 */
export function createAddOperatorToCurrentWorkflowTool(
  workflowActionService: WorkflowActionService,
  workflowUtilService: WorkflowUtilService,
  operatorMetadataService: OperatorMetadataService
) {
  return tool({
    name: "addOperatorToCurrentWorkflow",
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
          return {
            success: false,
            error: `Unknown operator type: ${args.operatorType}.Use tools to see available types.`,
          };
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

        // Show copilot is adding this operator (after it's added to graph)
        setTimeout(() => {}, 100);

        return {
          success: true,
          operatorId: operator.operatorID,
          message: `Added ${args.operatorType} operator to workflow`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create addLink tool for connecting two operators
 */
export function createAddLinkToCurrentWorkflowTool(workflowActionService: WorkflowActionService) {
  return tool({
    name: "addLinkToCurrentWorkflow",
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

        return {
          success: true,
          linkId: link.linkID,
          message: `Connected ${args.sourceOperatorId}:${sourcePId} to ${args.targetOperatorId}:${targetPId}`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create deleteOperator tool for removing an operator from the workflow
 */
export function createDeleteOperatorInCurrentWorkflowTool(workflowActionService: WorkflowActionService) {
  return tool({
    name: "deleteOperatorInCurrentWorkflow",
    description: "Delete an operator from the current workflow",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to delete"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        workflowActionService.deleteOperator(args.operatorId);

        return {
          success: true,
          message: `Deleted operator ${args.operatorId}`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create deleteLink tool for removing a link from the workflow
 */
export function createDeleteLinkInCurrentWorkflowTool(workflowActionService: WorkflowActionService) {
  return tool({
    name: "deleteLinkInCurrentWorkflow",
    description: "Delete a link between two operators in the current workflow by link ID",
    inputSchema: z.object({
      linkId: z.string().describe("ID of the link to delete"),
    }),
    execute: async (args: { linkId: string }) => {
      try {
        workflowActionService.deleteLinkWithID(args.linkId);
        return {
          success: true,
          message: `Deleted link ${args.linkId}`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
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
    name: "setOperatorPropertyInCurrentWorkflow",
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
          return {
            success: false,
            error: "Property validation failed",
            validationErrors: validation.messages,
            hint: "Use getOperatorPropertiesSchema tool to see the expected schema structure for this operator",
          };
        }

        // Show property was changed

        return {
          success: true,
          message: `Updated properties for operator ${args.operatorId}`,
          properties: args.properties,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
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
    name: "setPortPropertyInCurrentWorkflow",
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
          return {
            success: false,
            error: "Port property validation failed",
            validationErrors: validation.messages,
            hint: "Use getOperatorPortsInfo tool to see the available ports and their current configuration",
          };
        }

        // Show property was changed

        return {
          success: true,
          message: `Updated port ${args.portId} properties for operator ${args.operatorId}`,
          properties: args.properties,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create listLinksInCurrentWorkflow tool for getting all links in the workflow
 */
export function createListCurrentLinksTool(workflowActionService: WorkflowActionService) {
  return tool({
    name: "listCurrentLinks",
    description: "Get all links in the current workflow",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const links = workflowActionService.getTexeraGraph().getAllLinks();
        return {
          success: true,
          links: links,
          count: links.length,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

export function createListOperatorsInCurrentWorkflowTool(workflowActionService: WorkflowActionService) {
  return tool({
    name: "listOperatorsInCurrentWorkflow",
    description: "Get all operators in the current workflow",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const operators = workflowActionService.getTexeraGraph().getAllOperators();
        return {
          success: true,
          operators: operators,
          count: operators.length,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

export function createGetCurrentOperatorTool(
  workflowActionService: WorkflowActionService,
  workflowCompilingService: WorkflowCompilingService
) {
  return tool({
    name: "getCurrentOperator",
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

        return {
          success: true,
          operator: operator,
          inputSchema: inputSchema,
          outputSchema: outputSchema,
          message: `Retrieved operator ${args.operatorId}`,
        };
      } catch (error: any) {
        return {
          success: false,
          error: error.message || `Operator ${args.operatorId} not found`,
        };
      }
    },
  });
}

/**
 * Tool to find operators by output schema
 * This helps the agent identify relevant operators that produce data matching a specific schema
 */
export function createListCurrentRelevantOperatorIdsTool(
  workflowActionService: WorkflowActionService,
  workflowCompilingService: WorkflowCompilingService
) {
  return tool({
    name: "listCurrentRelevantOperatorIds",
    description:
      "Find all operators in the workflow that are relevant context when working with specific data schemas. " +
      "Please use this method when you want to work on certain columns in the data. " +
      "If you don't have a specific scope to work on, provide an empty schema array to get all operator IDs.",
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
        // If no schema provided (empty array), return all operator IDs
        if (!args.targetSchema || args.targetSchema.length === 0) {
          const allOperatorIds = workflowActionService
            .getTexeraGraph()
            .getAllOperators()
            .map(op => op.operatorID);
          return {
            success: true,
            operatorIds: allOperatorIds,
            count: allOperatorIds.length,
            message: `No specific schema provided. Returning all ${allOperatorIds.length} operator(s) in the workflow.`,
          };
        }

        const matchingOperatorIds = workflowActionService.findOperatorsByOutputSchema(
          args.targetSchema,
          workflowCompilingService
        );

        return {
          success: true,
          operatorIds: matchingOperatorIds,
          count: matchingOperatorIds.length,
          message: `Found ${matchingOperatorIds.length} operator(s) with output schema matching the target attributes: ${args.targetSchema.map(attr => attr.attributeName).join(", ")}`,
        };
      } catch (error: any) {
        return {
          success: false,
          error: error.message || String(error),
        };
      }
    },
  });
}

/**
 * Create getWorkflowCompilationState tool for checking compilation status and errors
 */
export function createGetCurrentWorkflowCompilationStateTool(workflowCompilingService: WorkflowCompilingService) {
  return tool({
    name: "getCurrentWorkflowCompilationState",
    description:
      "Get the current workflow compilation state and any compilation errors. Use this to check if the workflow is valid and identify any operator configuration issues.",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const compilationState = workflowCompilingService.getWorkflowCompilationState();
        const compilationErrors = workflowCompilingService.getWorkflowCompilationErrors();

        const hasErrors = Object.keys(compilationErrors).length > 0;

        return {
          success: true,
          state: compilationState,
          hasErrors: hasErrors,
          errors: hasErrors ? compilationErrors : undefined,
          message: hasErrors
            ? `Workflow compilation failed with ${Object.keys(compilationErrors).length} error(s)`
            : `Workflow compilation state: ${compilationState}`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}
