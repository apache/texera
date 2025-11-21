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
import { ActionPlanService } from "../../action-plan/action-plan.service";
import { ValidationWorkflowService } from "../../validation/validation-workflow.service";

// Tool name constants
export const TOOL_NAME_ACTION_PLAN = "actionPlan";
export const TOOL_NAME_GET_ACTION_PLAN = "getActionPlan";
export const TOOL_NAME_LIST_ACTION_PLANS = "listActionPlans";
export const TOOL_NAME_DELETE_ACTION_PLAN = "deleteActionPlan";
export const TOOL_NAME_UPDATE_ACTION_PLAN = "updateActionPlan";

/**
 * Create actionPlan tool for managing workflow operations (add, modify, delete)
 */
export function createActionPlanTool(
  workflowActionService: WorkflowActionService,
  workflowUtilService: WorkflowUtilService,
  operatorMetadataService: OperatorMetadataService,
  actionPlanService: ActionPlanService,
  validationWorkflowService: ValidationWorkflowService,
  agentId: string = "",
  agentName: string = ""
) {
  return tool({
    name: TOOL_NAME_ACTION_PLAN,
    description:
      "Plan and execute workflow modifications including adding new operators/links, modifying existing operators, and deleting operators/links. Operations are executed in order: add → modify → delete.",
    inputSchema: z.object({
      summary: z.string().describe("A summary of what this action plan accomplishes"),
      add: z
        .object({
          operators: z
            .array(
              z.object({
                operatorType: z.string().describe("Type of operator (e.g., 'CSVSource', 'Filter', 'Aggregate')"),
                customDisplayName: z
                  .string()
                  .optional()
                  .describe("Brief custom name summarizing what this operator does"),
                properties: z
                  .record(z.any())
                  .optional()
                  .describe("Properties object to set on the operator (e.g., {fileName: 'data.csv', delimiter: ','})"),
              })
            )
            .optional()
            .describe("List of operators to add to the workflow"),
          links: z
            .array(
              z.object({
                sourceOperatorId: z
                  .string()
                  .describe(
                    "ID of source operator - can be existing operator ID or index (e.g., '0', '1') referring to newly added operators (0-based)"
                  ),
                targetOperatorId: z
                  .string()
                  .describe(
                    "ID of target operator - can be existing operator ID or index (e.g., '0', '1') referring to newly added operators (0-based)"
                  ),
                sourcePortId: z.string().optional().describe("Port ID on source operator (e.g., 'output-0')"),
                targetPortId: z.string().optional().describe("Port ID on target operator (e.g., 'input-0')"),
              })
            )
            .optional()
            .describe("List of links to connect operators"),
        })
        .optional()
        .describe("Operations to add new operators and links"),
      modify: z
        .object({
          operators: z
            .array(
              z.object({
                operatorId: z.string().describe("ID of the existing operator to modify"),
                properties: z
                  .record(z.any())
                  .describe("Properties to update on the operator (e.g., {delimiter: '|', limit: 100})"),
              })
            )
            .optional()
            .describe("List of operators to modify"),
        })
        .optional()
        .describe("Operations to modify existing operators"),
      delete: z
        .object({
          operatorIds: z.array(z.string()).optional().describe("List of operator IDs to delete"),
          linkIds: z.array(z.string()).optional().describe("List of link IDs to delete"),
        })
        .optional()
        .describe("Operations to delete operators and links"),
    }),
    execute: async (args: {
      summary: string;
      add?: {
        operators?: Array<{ operatorType: string; customDisplayName?: string; properties?: Record<string, any> }>;
        links?: Array<{
          sourceOperatorId: string;
          targetOperatorId: string;
          sourcePortId?: string;
          targetPortId?: string;
        }>;
      };
      modify?: {
        operators?: Array<{ operatorId: string; properties: Record<string, any> }>;
      };
      delete?: {
        operatorIds?: string[];
        linkIds?: string[];
      };
    }) => {
      try {
        const results = {
          addedOperatorIds: [] as string[],
          addedLinkIds: [] as string[],
          modifiedOperatorIds: [] as string[],
          deletedOperatorIds: [] as string[],
          deletedLinkIds: [] as string[],
        };

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

        // STEP 1: ADD operations
        if (args.add?.operators) {
          // Validate all operator types exist
          for (let i = 0; i < args.add.operators.length; i++) {
            const operatorSpec = args.add.operators[i];
            if (!operatorMetadataService.operatorTypeExists(operatorSpec.operatorType)) {
              return {
                success: false,
                error: `Unknown operator type at index ${i}: ${operatorSpec.operatorType}. Use listOperatorTypes tool to see available types.`,
              };
            }
          }

          const existingOperators = workflowActionService.getTexeraGraph().getAllOperators();
          const startIndex = existingOperators.length;

          for (let i = 0; i < args.add.operators.length; i++) {
            const operatorSpec = args.add.operators[i];

            // Get a new operator predicate with default settings and optional custom display name
            const operator = workflowUtilService.getNewOperatorPredicate(
              operatorSpec.operatorType,
              operatorSpec.customDisplayName
            );

            // Apply custom properties if provided
            if (operatorSpec.properties) {
              Object.assign(operator, operatorSpec.properties);
            }

            // Calculate a default position with better spacing for batch operations
            const defaultX = 100 + ((startIndex + i) % 5) * 200;
            const defaultY = 100 + Math.floor((startIndex + i) / 5) * 150;
            const position = { x: defaultX, y: defaultY };

            // Add the operator to the workflow
            workflowActionService.addOperator(operator, position);
            results.addedOperatorIds.push(operator.operatorID);
          }
        }

        // Add links if specified
        if (args.add?.links) {
          for (let i = 0; i < args.add.links.length; i++) {
            const linkSpec = args.add.links[i];

            // Resolve source and target operator IDs
            const sourceOperatorId = resolveOperatorId(linkSpec.sourceOperatorId, results.addedOperatorIds);
            const targetOperatorId = resolveOperatorId(linkSpec.targetOperatorId, results.addedOperatorIds);

            if (!sourceOperatorId) {
              return {
                success: false,
                error: `Invalid source operator ID at link ${i}: '${linkSpec.sourceOperatorId}'. Must be either an existing operator ID or a valid index (0-${results.addedOperatorIds.length - 1}).`,
              };
            }

            if (!targetOperatorId) {
              return {
                success: false,
                error: `Invalid target operator ID at link ${i}: '${linkSpec.targetOperatorId}'. Must be either an existing operator ID or a valid index (0-${results.addedOperatorIds.length - 1}).`,
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
            results.addedLinkIds.push(link.linkID);
          }
        }

        // STEP 2: MODIFY operations
        if (args.modify?.operators) {
          for (const modifySpec of args.modify.operators) {
            const operator = workflowActionService.getTexeraGraph().getOperator(modifySpec.operatorId);
            if (!operator) {
              return {
                success: false,
                error: `Operator with ID '${modifySpec.operatorId}' not found for modification.`,
              };
            }

            // Apply property updates
            Object.assign(operator, modifySpec.properties);
            workflowActionService.setOperatorProperty(modifySpec.operatorId, modifySpec.properties);
            results.modifiedOperatorIds.push(modifySpec.operatorId);
          }
        }

        // STEP 3: DELETE operations
        if (args.delete?.operatorIds) {
          for (const operatorId of args.delete.operatorIds) {
            const operator = workflowActionService.getTexeraGraph().getOperator(operatorId);
            if (operator) {
              workflowActionService.deleteOperator(operatorId);
              results.deletedOperatorIds.push(operatorId);
            }
          }
        }

        if (args.delete?.linkIds) {
          for (const linkId of args.delete.linkIds) {
            const link = workflowActionService.getTexeraGraph().getLinkWithID(linkId);
            if (link) {
              workflowActionService.deleteLinkWithID(linkId);
              results.deletedLinkIds.push(linkId);
            }
          }
        }

        // Create action plan with all operations
        const allOperatorIds = [...results.addedOperatorIds, ...results.modifiedOperatorIds];
        const allLinkIds = [...results.addedLinkIds];

        const actionPlan = actionPlanService.createActionPlan(
          agentId,
          agentName || "AI Agent",
          args.summary,
          {
            add: {
              operatorIds: results.addedOperatorIds,
              linkIds: results.addedLinkIds,
            },
            modify: {
              operatorIds: results.modifiedOperatorIds,
            },
            delete: {
              operatorIds: results.deletedOperatorIds,
              linkIds: results.deletedLinkIds,
            },
          },
          allOperatorIds,
          allLinkIds
        );

        // Get validation information for the workflow after operations
        const validationOutput = validationWorkflowService.getCurrentWorkflowValidationError();
        const errorCount = Object.keys(validationOutput.errors).length;

        const validGraph = validationWorkflowService.getValidTexeraGraph();
        const validOperators = validGraph.getAllOperators();
        const allOperators = workflowActionService.getTexeraGraph().getAllOperators();

        const validOperatorIds = validOperators.map(op => op.operatorID);
        const invalidCount = allOperators.length - validOperators.length;

        const validationInfo = {
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
        };

        // Return the action plan info with validation
        return {
          success: true,
          summary: args.summary,
          actionPlanId: actionPlan.id,
          results,
          validation: validationInfo,
          message: `Created action plan: ${results.addedOperatorIds.length} operators added, ${results.modifiedOperatorIds.length} modified, ${results.deletedOperatorIds.length} deleted. ${results.addedLinkIds.length} links added, ${results.deletedLinkIds.length} deleted. Waiting for user feedback.`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create getActionPlan tool for retrieving a specific action plan by ID
 */
export function createGetActionPlanTool(actionPlanService: ActionPlanService) {
  return tool({
    name: TOOL_NAME_GET_ACTION_PLAN,
    description: "Retrieve a specific action plan by its ID",
    inputSchema: z.object({
      actionPlanId: z.string().describe("The ID of the action plan to retrieve"),
    }),
    execute: async (args: { actionPlanId: string }) => {
      try {
        const plan = actionPlanService.getActionPlan(args.actionPlanId);
        if (!plan) {
          return { success: false, error: "Action plan not found" };
        }

        // Convert to a serializable format
        return {
          success: true,
          actionPlan: {
            id: plan.id,
            agentId: plan.agentId,
            agentName: plan.agentName,
            executorAgentId: plan.executorAgentId,
            summary: plan.summary,
            createdAt: plan.createdAt.toISOString(),
            operatorIds: plan.operatorIds,
            linkIds: plan.linkIds,
            operations: plan.operations,
          },
        };
      } catch (error) {
        return { success: false, error: error instanceof Error ? error.message : "Failed to retrieve action plan" };
      }
    },
  });
}

/**
 * Create listActionPlans tool for retrieving all action plans
 */
export function createListActionPlansTool(actionPlanService: ActionPlanService) {
  return tool({
    name: TOOL_NAME_LIST_ACTION_PLANS,
    description: "List all action plans in the system",
    inputSchema: z.object({
      filterByAgent: z.string().optional().describe("Optional: Filter by agent ID"),
    }),
    execute: async (args: { filterByAgent?: string }) => {
      try {
        const allPlans = actionPlanService.getAllActionPlans();

        // Apply filters if provided
        let filteredPlans = allPlans;
        if (args.filterByAgent) {
          filteredPlans = filteredPlans.filter(plan => plan.agentId === args.filterByAgent);
        }

        // Convert to serializable format
        const plans = filteredPlans.map(plan => ({
          id: plan.id,
          agentId: plan.agentId,
          agentName: plan.agentName,
          executorAgentId: plan.executorAgentId,
          summary: plan.summary,
          createdAt: plan.createdAt.toISOString(),
          operations: plan.operations,
        }));

        return {
          success: true,
          actionPlans: plans,
          totalCount: plans.length,
        };
      } catch (error) {
        return { success: false, error: error instanceof Error ? error.message : "Failed to list action plans" };
      }
    },
  });
}

/**
 * Create deleteActionPlan tool for deleting an action plan
 */
export function createDeleteActionPlanTool(actionPlanService: ActionPlanService) {
  return tool({
    name: TOOL_NAME_DELETE_ACTION_PLAN,
    description: "Delete an action plan by its ID",
    inputSchema: z.object({
      actionPlanId: z.string().describe("The ID of the action plan to delete"),
    }),
    execute: async (args: { actionPlanId: string }) => {
      try {
        const success = actionPlanService.deleteActionPlan(args.actionPlanId);
        if (!success) {
          return { success: false, error: "Action plan not found or could not be deleted" };
        }
        return { success: true, message: `Action plan ${args.actionPlanId} deleted successfully` };
      } catch (error) {
        return { success: false, error: error instanceof Error ? error.message : "Failed to delete action plan" };
      }
    },
  });
}

/**
 * Create updateActionPlan tool for updating an action plan
 */
export function createUpdateActionPlanTool(actionPlanService: ActionPlanService) {
  return tool({
    name: TOOL_NAME_UPDATE_ACTION_PLAN,
    description: "Update an action plan's properties",
    inputSchema: z.object({
      actionPlanId: z.string().describe("The ID of the action plan to update"),
      summary: z.string().optional().describe("New summary for the action plan"),
    }),
    execute: async (args: { actionPlanId: string; summary?: string }) => {
      try {
        const plan = actionPlanService.getActionPlan(args.actionPlanId);
        if (!plan) {
          return { success: false, error: "Action plan not found" };
        }

        // Update fields if provided
        if (args.summary !== undefined) {
          plan.summary = args.summary;
        }

        return {
          success: true,
          message: `Action plan ${args.actionPlanId} updated successfully`,
          updatedFields: Object.keys(args).filter(k => k !== "actionPlanId"),
        };
      } catch (error) {
        return { success: false, error: error instanceof Error ? error.message : "Failed to update action plan" };
      }
    },
  });
}
