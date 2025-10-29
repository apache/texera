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
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { OperatorLink } from "../../types/workflow-common.interface";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { DynamicSchemaService } from "../dynamic-schema/dynamic-schema.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { WorkflowResultService } from "../workflow-result/workflow-result.service";
import { CopilotCoeditorService } from "./copilot-coeditor.service";
import { WorkflowCompilingService } from "../compile-workflow/workflow-compiling.service";
import { ValidationWorkflowService } from "../validation/validation-workflow.service";
import { DataInconsistencyService } from "../data-inconsistency/data-inconsistency.service";
import { ActionPlanService } from "../action-plan/action-plan.service";

// Tool execution timeout in milliseconds (5 seconds)
const TOOL_TIMEOUT_MS = 120000;

// Maximum token limit for operator result data to prevent overwhelming LLM context
// Estimated as characters / 4 (common approximation for token counting)
const MAX_OPERATOR_RESULT_TOKEN_LIMIT = 1000;

export interface ActionPlan {
  summary: string;
  operators: Array<{ operatorType: string; customDisplayName?: string; description?: string }>;
  links: Array<{
    sourceOperatorId: string;
    targetOperatorId: string;
    sourcePortId?: string;
    targetPortId?: string;
  }>;
}

/**
 * Estimates the number of tokens in a JSON-serializable object
 * Uses a common approximation: tokens ≈ characters / 4
 */
function estimateTokenCount(data: any): number {
  try {
    const jsonString = JSON.stringify(data);
    return Math.ceil(jsonString.length / 4);
  } catch (error) {
    // Fallback if JSON.stringify fails
    return 0;
  }
}

/**
 * Wraps a tool definition to add timeout protection to its execute function
 * Uses AbortController to properly cancel operations on timeout
 */
export function toolWithTimeout(toolConfig: any): any {
  const originalExecute = toolConfig.execute;

  return {
    ...toolConfig,
    execute: async (args: any) => {
      // Create an AbortController for this execution
      const abortController = new AbortController();

      // Create a timeout promise that will abort the controller
      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => {
          abortController.abort(); // Signal cancellation to the operation
          reject(new Error("timeout"));
        }, TOOL_TIMEOUT_MS);
      });

      try {
        // Pass the abort signal in args so tools can check it
        const argsWithSignal = { ...args, signal: abortController.signal };
        return await Promise.race([originalExecute(argsWithSignal), timeoutPromise]);
      } catch (error: any) {
        // If it's a timeout error, return a properly formatted error response
        if (error.message === "timeout") {
          return {
            success: false,
            error: "Tool execution timeout - operation took longer than 5 seconds. Please try again later.",
          };
        }
        // Re-throw other errors to be handled by the original error handler
        throw error;
      }
    },
  };
}

/**
 * Create addOperator tool for adding a new operator to the workflow
 */
export function createAddOperatorTool(
  workflowActionService: WorkflowActionService,
  workflowUtilService: WorkflowUtilService,
  operatorMetadataService: OperatorMetadataService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "addOperator",
    description: "Add a new operator to the workflow",
    inputSchema: z.object({
      operatorType: z.string().describe("Type of operator (e.g., 'CSVSource', 'Filter', 'Aggregate')"),
      customDisplayName: z
        .string()
        .optional()
        .describe("Brief custom name summarizing what this operator does in one sentence"),
    }),
    execute: async (args: { operatorType: string; customDisplayName?: string }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Validate operator type exists
        if (!operatorMetadataService.operatorTypeExists(args.operatorType)) {
          return {
            success: false,
            error: `Unknown operator type: ${args.operatorType}. Use listOperatorTypes tool to see available types.`,
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
        setTimeout(() => {
          copilotCoeditor.showEditingOperator(operator.operatorID);
          copilotCoeditor.highlightOperators([operator.operatorID]);
        }, 100);

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
export function createAddLinkTool(workflowActionService: WorkflowActionService) {
  return tool({
    name: "addLink",
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
 * Create actionPlan tool for adding batch operators and links
 */
export function createActionPlanTool(
  workflowActionService: WorkflowActionService,
  workflowUtilService: WorkflowUtilService,
  operatorMetadataService: OperatorMetadataService,
  copilotCoeditor: CopilotCoeditorService,
  actionPlanService: ActionPlanService
) {
  return tool({
    name: "actionPlan",
    description:
      "Add a batch of operators and links to the workflow as part of an action plan. This tool is used to show the structure of what you plan to add without filling in detailed operator properties. It creates a workflow skeleton that demonstrates the planned data flow.",
    inputSchema: z.object({
      summary: z.string().describe("A brief summary of what this action plan does"),
      operators: z
        .array(
          z.object({
            operatorType: z.string().describe("Type of operator (e.g., 'CSVSource', 'Filter', 'Aggregate')"),
            customDisplayName: z
              .string()
              .optional()
              .describe("Brief custom name summarizing what this operator does in one sentence"),
            description: z.string().optional().describe("Detailed description of what this operator will do"),
          })
        )
        .describe("List of operators to add to the workflow"),
      links: z
        .array(
          z.object({
            sourceOperatorId: z
              .string()
              .describe(
                "ID of the source operator - can be either an existing operator ID from the workflow, or an index (e.g., '0', '1', '2') referring to operators in the plan array (0-based)"
              ),
            targetOperatorId: z
              .string()
              .describe(
                "ID of the target operator - can be either an existing operator ID from the workflow, or an index (e.g., '0', '1', '2') referring to operators in the plan array (0-based)"
              ),
            sourcePortId: z.string().optional().describe("Port ID on source operator (e.g., 'output-0')"),
            targetPortId: z.string().optional().describe("Port ID on target operator (e.g., 'input-0')"),
          })
        )
        .describe("List of links to connect the operators"),
    }),
    execute: async (args: { summary: string; operators: ActionPlan["operators"]; links: ActionPlan["links"] }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Validate all operator types exist
        for (let i = 0; i < args.operators.length; i++) {
          const operatorSpec = args.operators[i];
          if (!operatorMetadataService.operatorTypeExists(operatorSpec.operatorType)) {
            return {
              success: false,
              error: `Unknown operator type at index ${i}: ${operatorSpec.operatorType}. Use listOperatorTypes tool to see available types.`,
            };
          }
        }

        // Helper function to resolve operator ID (can be existing ID or index string)
        const resolveOperatorId = (idOrIndex: string, createdIds: string[]): string | null => {
          // Check if it's a numeric index (referring to operators array)
          const indexMatch = idOrIndex.match(/^(\d+)$/);
          if (indexMatch) {
            const index = parseInt(indexMatch[1], 10);
            if (index >= 0 && index < createdIds.length) {
              return createdIds[index];
            }
            return null; // Invalid index
          }

          // Otherwise, treat as existing operator ID
          const existingOp = workflowActionService.getTexeraGraph().getOperator(idOrIndex);
          return existingOp ? idOrIndex : null;
        };

        // Create all operators and store their IDs
        const createdOperatorIds: string[] = [];
        const existingOperators = workflowActionService.getTexeraGraph().getAllOperators();
        const startIndex = existingOperators.length;

        for (let i = 0; i < args.operators.length; i++) {
          const operatorSpec = args.operators[i];

          // Get a new operator predicate with default settings and optional custom display name
          const operator = workflowUtilService.getNewOperatorPredicate(
            operatorSpec.operatorType,
            operatorSpec.customDisplayName
          );

          // Calculate a default position with better spacing for batch operations
          const defaultX = 100 + ((startIndex + i) % 5) * 200;
          const defaultY = 100 + Math.floor((startIndex + i) / 5) * 150;
          const position = { x: defaultX, y: defaultY };

          // Add the operator to the workflow
          workflowActionService.addOperator(operator, position);
          createdOperatorIds.push(operator.operatorID);
        }

        // Create all links using the operator IDs
        const createdLinkIds: string[] = [];
        for (let i = 0; i < args.links.length; i++) {
          const linkSpec = args.links[i];

          // Resolve source and target operator IDs
          const sourceOperatorId = resolveOperatorId(linkSpec.sourceOperatorId, createdOperatorIds);
          const targetOperatorId = resolveOperatorId(linkSpec.targetOperatorId, createdOperatorIds);

          if (!sourceOperatorId) {
            return {
              success: false,
              error: `Invalid source operator ID at link ${i}: '${linkSpec.sourceOperatorId}'. Must be either an existing operator ID or a valid index (0-${createdOperatorIds.length - 1}).`,
            };
          }

          if (!targetOperatorId) {
            return {
              success: false,
              error: `Invalid target operator ID at link ${i}: '${linkSpec.targetOperatorId}'. Must be either an existing operator ID or a valid index (0-${createdOperatorIds.length - 1}).`,
            };
          }

          const sourcePId = linkSpec.sourcePortId || "output-0";
          const targetPId = linkSpec.targetPortId || "input-0";

          const link: OperatorLink = {
            linkID: `link_${Date.now()}_${Math.random()}`,
            source: {
              operatorID: sourceOperatorId,
              portID: sourcePId,
            },
            target: {
              operatorID: targetOperatorId,
              portID: targetPId,
            },
          };

          workflowActionService.addLink(link);
          createdLinkIds.push(link.linkID);
        }

        // Show copilot is adding these operators (after they're added to graph)
        setTimeout(() => {
          copilotCoeditor.highlightOperators(createdOperatorIds);
        }, 100);

        // Show action plan and wait for user feedback
        const feedback = await new Promise<any>((resolve, reject) => {
          setTimeout(() => {
            actionPlanService
              .showActionPlanAndWaitForFeedback(createdOperatorIds, createdLinkIds, args.summary)
              .subscribe({
                next: feedback => resolve(feedback),
                error: (err: unknown) => reject(err),
              });
          }, 150);
        });

        // Handle user feedback
        if (!feedback.accepted) {
          // User rejected - remove the created operators and links
          workflowActionService.deleteOperatorsAndLinks(createdOperatorIds);

          return {
            success: false,
            rejected: true,
            userFeedback: feedback.message || "User rejected this action plan",
            message: `Action plan rejected by user: ${feedback.message || "No reason provided"}`,
          };
        }

        // User accepted - return success
        return {
          success: true,
          summary: args.summary,
          operatorIds: createdOperatorIds,
          linkIds: createdLinkIds,
          message: `Action Plan: ${args.summary}. Added ${args.operators.length} operator(s) and ${args.links.length} link(s) to workflow. User accepted the plan.`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create listOperators tool for getting all operators in the workflow
 */
export function createListOperatorsTool(
  workflowActionService: WorkflowActionService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "listOperators",
    description: "Get all operators in the current workflow",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        const operators = workflowActionService.getTexeraGraph().getAllOperators();

        // Highlight all operators to show copilot is inspecting them
        const operatorIds = operators.map(op => op.operatorID);
        copilotCoeditor.highlightOperators(operatorIds);

        return {
          success: true,
          operators: operators,
          count: operators.length,
        };
      } catch (error: any) {
        // Can't clear highlights without operator IDs
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create listLinks tool for getting all links in the workflow
 */
export function createListLinksTool(workflowActionService: WorkflowActionService) {
  return tool({
    name: "listLinks",
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

/**
 * Create listOperatorTypes tool for getting all available operator types
 */
export function createListOperatorTypesTool(workflowUtilService: WorkflowUtilService) {
  return tool({
    name: "listOperatorTypes",
    description: "Get all available operator types in the system",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const operatorTypes = workflowUtilService.getOperatorTypeList();
        return {
          success: true,
          operatorTypes: operatorTypes,
          count: operatorTypes.length,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create getOperator tool for getting detailed information about a specific operator
 */
export function createGetOperatorTool(
  workflowActionService: WorkflowActionService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "getOperator",
    description: "Get detailed information about a specific operator in the workflow",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to retrieve"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Show copilot is viewing this operator
        copilotCoeditor.showEditingOperator(args.operatorId);
        copilotCoeditor.highlightOperators([args.operatorId]);

        const operator = workflowActionService.getTexeraGraph().getOperator(args.operatorId);

        return {
          success: true,
          operator: operator,
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
 * Create deleteOperator tool for removing an operator from the workflow
 */
export function createDeleteOperatorTool(
  workflowActionService: WorkflowActionService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "deleteOperator",
    description: "Delete an operator from the workflow",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to delete"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Show copilot is editing this operator before deletion
        copilotCoeditor.showEditingOperator(args.operatorId);

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
export function createDeleteLinkTool(workflowActionService: WorkflowActionService) {
  return tool({
    name: "deleteLink",
    description: "Delete a link between two operators in the workflow by link ID",
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
export function createSetOperatorPropertyTool(
  workflowActionService: WorkflowActionService,
  copilotCoeditor: CopilotCoeditorService,
  validationWorkflowService: ValidationWorkflowService
) {
  return tool({
    name: "setOperatorProperty",
    description:
      "Set or update properties of an operator in the workflow. Properties must match the operator's schema. Use getOperatorPropertiesSchema first to understand required properties and their types.",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to modify"),
      properties: z.record(z.any()).describe("Properties object to set on the operator"),
    }),
    execute: async (args: { operatorId: string; properties: Record<string, any> }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Show copilot is editing this operator
        copilotCoeditor.showEditingOperator(args.operatorId);

        // Set the properties first
        workflowActionService.setOperatorProperty(args.operatorId, args.properties);

        // Validate the operator after setting properties
        const validation = validationWorkflowService.validateOperator(args.operatorId);

        if (!validation.isValid) {
          // Properties are set but invalid - return error with details
          copilotCoeditor.clearEditingOperator();
          return {
            success: false,
            error: "Property validation failed",
            validationErrors: validation.messages,
            hint: "Use getOperatorPropertiesSchema tool to see the expected schema structure for this operator",
          };
        }

        // Show property was changed
        copilotCoeditor.showPropertyChanged(args.operatorId);

        return {
          success: true,
          message: `Updated properties for operator ${args.operatorId}`,
          properties: args.properties,
        };
      } catch (error: any) {
        copilotCoeditor.clearEditingOperator();
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create setPortProperty tool for modifying port properties
 */
export function createSetPortPropertyTool(
  workflowActionService: WorkflowActionService,
  copilotCoeditor: CopilotCoeditorService,
  validationWorkflowService: ValidationWorkflowService
) {
  return tool({
    name: "setPortProperty",
    description:
      "Set or update properties of a port on an operator (e.g., partition information, dependencies). Use getOperatorPortsInfo first to see available ports.",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator that owns the port"),
      portId: z.string().describe("ID of the port to modify (e.g., 'input-0', 'output-0')"),
      properties: z.record(z.any()).describe("Port properties to set (partitionInfo, dependencies)"),
    }),
    execute: async (args: { operatorId: string; portId: string; properties: Record<string, any> }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Show copilot is editing this operator
        copilotCoeditor.showEditingOperator(args.operatorId);

        // Create LogicalPort object
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
          copilotCoeditor.clearEditingOperator();
          return {
            success: false,
            error: "Port property validation failed",
            validationErrors: validation.messages,
            hint: "Use getOperatorPortsInfo tool to see the available ports and their current configuration",
          };
        }

        // Show property was changed
        copilotCoeditor.showPropertyChanged(args.operatorId);

        return {
          success: true,
          message: `Updated port ${args.portId} properties for operator ${args.operatorId}`,
          properties: args.properties,
        };
      } catch (error: any) {
        copilotCoeditor.clearEditingOperator();
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create getOperatorSchema tool for getting operator schema information
 * Returns the original operator schema (not the dynamic one) to save tokens
 */
export function createGetOperatorSchemaTool(
  workflowActionService: WorkflowActionService,
  operatorMetadataService: OperatorMetadataService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "getOperatorSchema",
    description:
      "Get the original schema of an operator, which includes properties of this operator. Use this to understand what properties can be edited on an operator before modifying it.",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to get schema for"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Highlight the operator being inspected
        copilotCoeditor.highlightOperators([args.operatorId]);

        // Get the operator to find its type
        const operator = workflowActionService.getTexeraGraph().getOperator(args.operatorId);
        if (!operator) {
          return { success: false, error: `Operator ${args.operatorId} not found` };
        }

        // Get the original operator schema from metadata
        const schema = operatorMetadataService.getOperatorSchema(operator.operatorType);

        return {
          success: true,
          schema: schema,
          message: `Retrieved original schema for operator ${args.operatorId} (type: ${operator.operatorType})`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create getOperatorPropertiesSchema tool for getting just the properties schema
 * More token-efficient than getOperatorSchema for property-focused queries
 */
export function createGetOperatorPropertiesSchemaTool(
  workflowActionService: WorkflowActionService,
  operatorMetadataService: OperatorMetadataService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "getOperatorPropertiesSchema",
    description:
      "Get just the properties schema for an operator. This is more token-efficient than getOperatorSchema and returns only the properties structure and required fields. Use this before setting operator properties.",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to get properties schema for"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Highlight the operator being inspected
        copilotCoeditor.highlightOperators([args.operatorId]);

        // Get the operator to find its type
        const operator = workflowActionService.getTexeraGraph().getOperator(args.operatorId);
        if (!operator) {
          return { success: false, error: `Operator ${args.operatorId} not found` };
        }

        // Get the original operator schema from metadata
        const schema = operatorMetadataService.getOperatorSchema(operator.operatorType);

        // Extract just the properties and required fields from the JSON schema
        const propertiesSchema = {
          properties: schema.jsonSchema.properties,
          required: schema.jsonSchema.required,
          definitions: schema.jsonSchema.definitions, // Include definitions for $ref resolution
        };

        return {
          success: true,
          propertiesSchema: propertiesSchema,
          operatorType: operator.operatorType,
          message: `Retrieved properties schema for operator ${args.operatorId} (type: ${operator.operatorType})`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create getOperatorPortsInfo tool for getting just the port information
 * More token-efficient than getOperatorSchema for port-focused queries
 */
export function createGetOperatorPortsInfoTool(
  workflowActionService: WorkflowActionService,
  operatorMetadataService: OperatorMetadataService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "getOperatorPortsInfo",
    description:
      "Get input and output port information for an operator. This is more token-efficient than getOperatorSchema and returns only port details (display names, multi-input support, etc.).",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to get port information for"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Highlight the operator being inspected
        copilotCoeditor.highlightOperators([args.operatorId]);

        // Get the operator to find its type
        const operator = workflowActionService.getTexeraGraph().getOperator(args.operatorId);
        if (!operator) {
          return { success: false, error: `Operator ${args.operatorId} not found` };
        }

        // Get the original operator schema from metadata
        const schema = operatorMetadataService.getOperatorSchema(operator.operatorType);

        // Extract just the port information from the additional metadata
        const portsInfo = {
          inputPorts: schema.additionalMetadata.inputPorts,
          outputPorts: schema.additionalMetadata.outputPorts,
          dynamicInputPorts: schema.additionalMetadata.dynamicInputPorts,
          dynamicOutputPorts: schema.additionalMetadata.dynamicOutputPorts,
        };

        return {
          success: true,
          portsInfo: portsInfo,
          operatorType: operator.operatorType,
          message: `Retrieved port information for operator ${args.operatorId} (type: ${operator.operatorType})`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create getOperatorMetadata tool for getting operator's semantic metadata
 * Returns information about what the operator does, its description, and capabilities
 */
export function createGetOperatorMetadataTool(
  workflowActionService: WorkflowActionService,
  operatorMetadataService: OperatorMetadataService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "getOperatorMetadata",
    description:
      "Get semantic metadata for an operator, including user-friendly name, description, operator group, and capabilities. This is very useful to understand the semantics and purpose of each operator - what it does, how it works, and what kind of data transformation it performs.",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to get metadata for"),
    }),
    execute: async (args: { operatorId: string; signal?: AbortSignal }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Highlight the operator being inspected
        copilotCoeditor.highlightOperators([args.operatorId]);

        // Get the operator to find its type
        const operator = workflowActionService.getTexeraGraph().getOperator(args.operatorId);
        if (!operator) {
          return { success: false, error: `Operator ${args.operatorId} not found` };
        }

        // Get the original operator schema from metadata
        const schema = operatorMetadataService.getOperatorSchema(operator.operatorType);

        // Return the additional metadata which contains semantic information
        const metadata = schema.additionalMetadata;

        return {
          success: true,
          metadata: metadata,
          operatorType: operator.operatorType,
          operatorVersion: schema.operatorVersion,
          message: `Retrieved metadata for operator ${args.operatorId} (type: ${operator.operatorType})`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create getOperatorInputSchema tool for getting operator's input schema from compilation
 */
export function createGetOperatorInputSchemaTool(
  workflowCompilingService: WorkflowCompilingService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "getOperatorInputSchema",
    description:
      "Get the input schema for an operator, which shows what columns/attributes are available from upstream operators. This is determined by workflow compilation and schema propagation.",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to get input schema for"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Highlight the operator being inspected
        copilotCoeditor.highlightOperators([args.operatorId]);

        const inputSchemaMap = workflowCompilingService.getOperatorInputSchemaMap(args.operatorId);

        if (!inputSchemaMap) {
          return {
            success: true,
            inputSchema: null,
            message: `Operator ${args.operatorId} has no input schema (may be a source operator or not connected)`,
          };
        }

        return {
          success: true,
          inputSchema: inputSchemaMap,
          message: `Retrieved input schema for operator ${args.operatorId}`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create getWorkflowCompilationState tool for checking compilation status and errors
 */
export function createGetWorkflowCompilationStateTool(workflowCompilingService: WorkflowCompilingService) {
  return tool({
    name: "getWorkflowCompilationState",
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

/**
 * Create executeWorkflow tool for running the workflow
 */
export function createExecuteWorkflowTool(executeWorkflowService: ExecuteWorkflowService) {
  return tool({
    name: "executeWorkflow",
    description: "Execute the current workflow",
    inputSchema: z.object({
      executionName: z.string().optional().describe("Name for this execution (default: 'Copilot Execution')"),
      targetOperatorId: z
        .string()
        .optional()
        .describe("Optional operator ID to execute up to (executes entire workflow if not specified)"),
    }),
    execute: async (args: { executionName?: string; targetOperatorId?: string }) => {
      try {
        const name = args.executionName || "Copilot Execution";
        executeWorkflowService.executeWorkflow(name, args.targetOperatorId);
        return {
          success: true,
          message: args.targetOperatorId
            ? `Started workflow execution up to operator ${args.targetOperatorId}`
            : "Started workflow execution",
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create getExecutionState tool for checking workflow execution status
 */
export function createGetExecutionStateTool(executeWorkflowService: ExecuteWorkflowService) {
  return tool({
    name: "getExecutionState",
    description: "Get the current execution state of the workflow",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const stateInfo = executeWorkflowService.getExecutionState();
        // Only include essential information, not the entire stateInfo which can be very large
        const result: any = {
          success: true,
          state: stateInfo,
        };
        return result;
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create killWorkflow tool for stopping workflow execution
 */
export function createKillWorkflowTool(executeWorkflowService: ExecuteWorkflowService) {
  return tool({
    name: "killWorkflow",
    description:
      "Kill the currently running workflow execution. Use this when the workflow is stuck or you need to stop it. Cannot kill if workflow is uninitialized or already completed.",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        executeWorkflowService.killWorkflow();
        return {
          success: true,
          message: "Workflow execution killed successfully",
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create hasOperatorResult tool for checking if an operator has results
 */
export function createHasOperatorResultTool(
  workflowResultService: WorkflowResultService,
  workflowActionService: WorkflowActionService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "hasOperatorResult",
    description: "Check if an operator has any execution results available",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to check"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Highlight operator being checked
        copilotCoeditor.highlightOperators([args.operatorId]);

        const hasResult = workflowResultService.hasAnyResult(args.operatorId);

        return {
          success: true,
          hasResult: hasResult,
          message: hasResult
            ? `Operator ${args.operatorId} has results available`
            : `Operator ${args.operatorId} has no results`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create unified getOperatorResult tool that automatically handles both pagination and snapshot modes
 */
export function createGetOperatorResultTool(
  workflowResultService: WorkflowResultService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "getOperatorResult",
    description:
      "Get result data for an operator. Automatically detects and uses the appropriate mode (pagination for tables, snapshot for visualizations). Returns rows limited by token count (~3000 tokens) to avoid overwhelming LLM context.",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to get results for"),
    }),
    execute: async (args: { operatorId: string; signal?: AbortSignal }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Highlight operator being inspected
        copilotCoeditor.highlightOperators([args.operatorId]);

        // First, try pagination mode (for table results)
        const paginatedResultService = workflowResultService.getPaginatedResultService(args.operatorId);
        if (paginatedResultService) {
          try {
            // Request first page with reasonable size (200 rows)
            // We'll filter by token limit after receiving
            const pageSize = 200;
            const resultEvent: any = await new Promise((resolve, reject) => {
              const subscription = paginatedResultService.selectPage(1, pageSize).subscribe({
                next: event => {
                  subscription.unsubscribe();
                  resolve(event);
                },
                error: (err: unknown) => {
                  subscription.unsubscribe();
                  reject(err);
                },
              });

              // Handle abort signal
              if (args.signal) {
                args.signal.addEventListener("abort", () => {
                  subscription.unsubscribe();
                  reject(new Error("Operation aborted"));
                });
              }
            });

            // Filter results by token limit
            const limitedResult: any[] = [];
            let currentTokenCount = 0;

            for (const row of resultEvent.table || []) {
              const rowTokens = estimateTokenCount(row);
              if (currentTokenCount + rowTokens > MAX_OPERATOR_RESULT_TOKEN_LIMIT) {
                break; // Stop if adding this row exceeds limit
              }
              limitedResult.push(row);
              currentTokenCount += rowTokens;
            }

            const totalRows = paginatedResultService.getCurrentTotalNumTuples();
            const wasLimited = limitedResult.length < (resultEvent.table?.length || 0);

            return {
              success: true,
              operatorId: args.operatorId,
              mode: "pagination",
              totalRows: totalRows,
              displayedRows: limitedResult.length,
              estimatedTokens: currentTokenCount,
              truncated: wasLimited,
              result: { ...resultEvent, table: limitedResult },
              message: wasLimited
                ? `Retrieved ${limitedResult.length} rows (out of ${totalRows} total, limited by token count ~${currentTokenCount} tokens) from paginated table results for operator ${args.operatorId}`
                : `Retrieved ${limitedResult.length} rows (out of ${totalRows} total, ~${currentTokenCount} tokens) from paginated table results for operator ${args.operatorId}`,
            };
          } catch (error: any) {
            return {
              success: false,
              error: `Failed to fetch paginated results: ${error.message}. This may be due to backend storage issues or results not being ready yet.`,
            };
          }
        }

        // If pagination mode is not available, try snapshot mode (for visualization results)
        const resultService = workflowResultService.getResultService(args.operatorId);
        if (resultService) {
          const snapshot = resultService.getCurrentResultSnapshot();
          if (!snapshot || snapshot.length === 0) {
            return {
              success: false,
              error: `Result snapshot is empty for operator ${args.operatorId}. Results might not be ready yet.`,
            };
          }

          // Filter by token limit
          const limitedResult: any[] = [];
          let currentTokenCount = 0;

          for (const row of snapshot) {
            const rowTokens = estimateTokenCount(row);
            if (currentTokenCount + rowTokens > MAX_OPERATOR_RESULT_TOKEN_LIMIT) {
              break; // Stop if adding this row exceeds limit
            }
            limitedResult.push(row);
            currentTokenCount += rowTokens;
          }

          const wasLimited = limitedResult.length < snapshot.length;

          return {
            success: true,
            operatorId: args.operatorId,
            mode: "snapshot",
            totalRows: snapshot.length,
            displayedRows: limitedResult.length,
            estimatedTokens: currentTokenCount,
            truncated: wasLimited,
            result: limitedResult,
            message: wasLimited
              ? `Retrieved ${limitedResult.length} rows (out of ${snapshot.length} total, limited by token count ~${currentTokenCount} tokens) from snapshot results for operator ${args.operatorId}`
              : `Retrieved ${limitedResult.length} rows (out of ${snapshot.length} total, ~${currentTokenCount} tokens) from snapshot results for operator ${args.operatorId}`,
          };
        }

        // No results available at all
        return {
          success: false,
          error: `No results available for operator ${args.operatorId}. The operator may not have been executed yet, or it may not produce viewable results.`,
        };
      } catch (error: any) {
        copilotCoeditor.clearEditingOperator();
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create getOperatorResultInfo tool for getting operator result information
 */
export function createGetOperatorResultInfoTool(
  workflowResultService: WorkflowResultService,
  workflowActionService: WorkflowActionService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "getOperatorResultInfo",
    description: "Get information about an operator's results, including total count and pagination details",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to get result info for"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Highlight operator being inspected
        copilotCoeditor.highlightOperators([args.operatorId]);

        const paginatedResultService = workflowResultService.getPaginatedResultService(args.operatorId);
        if (!paginatedResultService) {
          return {
            success: false,
            error: `No paginated results available for operator ${args.operatorId}`,
          };
        }
        const totalTuples = paginatedResultService.getCurrentTotalNumTuples();
        const currentPage = paginatedResultService.getCurrentPageIndex();
        const schema = paginatedResultService.getSchema();

        return {
          success: true,
          operatorId: args.operatorId,
          totalTuples: totalTuples,
          currentPage: currentPage,
          schema: schema,
          message: `Operator ${args.operatorId} has ${totalTuples} result tuples`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create getWorkflowValidationErrors tool for getting workflow validation errors
 */
export function createGetWorkflowValidationErrorsTool(validationWorkflowService: ValidationWorkflowService) {
  return tool({
    name: "getWorkflowValidationErrors",
    description:
      "Get all current validation errors in the workflow. This shows which operators have validation issues and what the errors are. Use this to check if operators are properly configured before execution.",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const validationOutput = validationWorkflowService.getCurrentWorkflowValidationError();
        const errorCount = Object.keys(validationOutput.errors).length;

        return {
          success: true,
          errors: validationOutput.errors,
          errorCount: errorCount,
          message:
            errorCount === 0
              ? "No validation errors in the workflow"
              : `Found ${errorCount} operator(s) with validation errors`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create validateOperator tool for validating a specific operator
 */
export function createValidateOperatorTool(
  validationWorkflowService: ValidationWorkflowService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "validateOperator",
    description:
      "Validate a specific operator to check if it's properly configured. Returns validation status and any error messages if invalid.",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to validate"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Highlight operator being validated
        copilotCoeditor.highlightOperators([args.operatorId]);

        const validation = validationWorkflowService.validateOperator(args.operatorId);

        if (validation.isValid) {
          return {
            success: true,
            isValid: true,
            message: `Operator ${args.operatorId} is valid`,
          };
        } else {
          return {
            success: true,
            isValid: false,
            errors: validation.messages,
            message: `Operator ${args.operatorId} has validation errors`,
          };
        }
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create getValidOperators tool for getting list of valid operators
 */
export function createGetValidOperatorsTool(
  validationWorkflowService: ValidationWorkflowService,
  workflowActionService: WorkflowActionService
) {
  return tool({
    name: "getValidOperators",
    description:
      "Get a list of all valid operators in the workflow. This filters out operators with validation errors and returns only properly configured operators.",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const validGraph = validationWorkflowService.getValidTexeraGraph();
        const validOperators = validGraph.getAllOperators();
        const allOperators = workflowActionService.getTexeraGraph().getAllOperators();

        const validOperatorIds = validOperators.map(op => op.operatorID);
        const invalidCount = allOperators.length - validOperators.length;

        return {
          success: true,
          validOperatorIds: validOperatorIds,
          validCount: validOperators.length,
          totalCount: allOperators.length,
          invalidCount: invalidCount,
          message: `Found ${validOperators.length} valid operator(s) out of ${allOperators.length} total`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Tool to add a data inconsistency to the list
 */
export function createAddInconsistencyTool(service: DataInconsistencyService) {
  return tool({
    name: "addInconsistency",
    description:
      "Add a data inconsistency finding to the inconsistency list. Use this when you find data errors or anomalies in the workflow results.",
    inputSchema: z.object({
      name: z.string().describe("Short name for the inconsistency (e.g., 'Negative Prices', 'Missing Values')"),
      description: z.string().describe("Detailed description of the inconsistency found"),
      operatorId: z.string().describe("ID of the operator that revealed this inconsistency"),
    }),
    execute: async (args: { name: string; description: string; operatorId: string }) => {
      try {
        const inconsistency = service.addInconsistency(args.name, args.description, args.operatorId);
        return {
          success: true,
          message: `Added inconsistency: ${args.name}`,
          inconsistency,
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
 * Tool to list all data inconsistencies
 */
export function createListInconsistenciesTool(service: DataInconsistencyService) {
  return tool({
    name: "listInconsistencies",
    description: "Get all data inconsistencies found so far",
    inputSchema: z.object({}),
    execute: async (args: {}) => {
      try {
        const inconsistencies = service.getAllInconsistencies();
        return {
          success: true,
          count: inconsistencies.length,
          inconsistencies,
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
 * Tool to update an existing data inconsistency
 */
export function createUpdateInconsistencyTool(service: DataInconsistencyService) {
  return tool({
    name: "updateInconsistency",
    description: "Update an existing data inconsistency",
    inputSchema: z.object({
      id: z.string().describe("ID of the inconsistency to update"),
      name: z.string().optional().describe("New name for the inconsistency"),
      description: z.string().optional().describe("New description"),
      operatorId: z.string().optional().describe("New operator ID"),
    }),
    execute: async (args: { id: string; name?: string; description?: string; operatorId?: string }) => {
      try {
        const updates: any = {};
        if (args.name !== undefined) updates.name = args.name;
        if (args.description !== undefined) updates.description = args.description;
        if (args.operatorId !== undefined) updates.operatorId = args.operatorId;

        const updated = service.updateInconsistency(args.id, updates);
        if (!updated) {
          return {
            success: false,
            error: `Inconsistency not found: ${args.id}`,
          };
        }

        return {
          success: true,
          message: `Updated inconsistency: ${args.id}`,
          inconsistency: updated,
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
 * Tool to delete a data inconsistency
 */
export function createDeleteInconsistencyTool(service: DataInconsistencyService) {
  return tool({
    name: "deleteInconsistency",
    description: "Delete a data inconsistency from the list",
    inputSchema: z.object({
      id: z.string().describe("ID of the inconsistency to delete"),
    }),
    execute: async (args: { id: string }) => {
      try {
        const deleted = service.deleteInconsistency(args.id);
        if (!deleted) {
          return {
            success: false,
            error: `Inconsistency not found: ${args.id}`,
          };
        }

        return {
          success: true,
          message: `Deleted inconsistency: ${args.id}`,
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
 * Tool to clear all data inconsistencies
 */
export function createClearInconsistenciesTool(service: DataInconsistencyService) {
  return tool({
    name: "clearInconsistencies",
    description: "Clear all data inconsistencies from the list",
    inputSchema: z.object({}),
    execute: async (args: {}) => {
      try {
        service.clearAll();
        return {
          success: true,
          message: "Cleared all inconsistencies",
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
