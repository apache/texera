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
                customDisplayName: z.string().describe("Brief custom name summarizing what this operator does"),
                properties: z.record(z.any()).describe("Properties object to set on this operator."),
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
        // Apply agent actions atomically using workflow action service
        const results = workflowActionService.applyAgentAction(args);

        // Check if the action failed
        if (!results.success) {
          return {
            success: false,
            error: results.error || "Failed to apply agent actions",
          };
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
        let plans = actionPlanService.getAllActionPlans();

        // Apply filters if provided
        if (args.filterByAgent) {
          plans = plans.filter(plan => plan.agentId === args.filterByAgent);
        }

        // Convert to serializable format
        const serializedPlans = plans.map(plan => ({
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
          actionPlans: serializedPlans,
          totalCount: serializedPlans.length,
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
