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

/**
 * Create actionPlan tool for adding batch operators and links
 */
export function createActionPlanTool(
  workflowActionService: WorkflowActionService,
  workflowUtilService: WorkflowUtilService,
  operatorMetadataService: OperatorMetadataService,
  actionPlanService: ActionPlanService,
  agentId: string = "",
  agentName: string = ""
) {
  return tool({
    name: "actionPlan",
    description:
      "Add a batch of operators and links to the workflow as part of an action plan. This tool is used to show the structure of what you plan to add without filling in detailed operator properties. It creates a workflow skeleton that demonstrates the planned data flow.",
    inputSchema: z.object({
      summary: z.string().describe("A summary of what this action plan does"),
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
    execute: async (args: {
      summary: string;
      operators: Array<{ operatorType: string; customDisplayName?: string; description?: string }>;
      links: Array<{
        sourceOperatorId: string;
        targetOperatorId: string;
        sourcePortId?: string;
        targetPortId?: string;
      }>;
    }) => {
      try {
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

        // Create action plan with tasks
        const tasks = args.operators.map((operatorSpec, index) => ({
          operatorId: createdOperatorIds[index],
          description: operatorSpec.description || operatorSpec.customDisplayName || operatorSpec.operatorType,
        }));

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

        const actionPlan = actionPlanService.createActionPlan(
          agentId,
          agentName || "AI Agent",
          args.summary,
          tasks,
          createdOperatorIds,
          createdLinkIds
        );

        // Show copilot is adding these operators (after they're added to graph)
        setTimeout(() => {}, 100);

        // Return the action plan info - user feedback will be handled via messages
        return {
          success: true,
          summary: args.summary,
          operatorIds: createdOperatorIds,
          linkIds: createdLinkIds,
          actionPlanId: actionPlan.id,
          message: `Created action plan with ${createdOperatorIds.length} operators and ${createdLinkIds.length} links. Waiting for user feedback.`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create updateActionPlanProgress tool for marking tasks as complete
 */
export function createUpdateActionPlanProgressTool(actionPlanService: ActionPlanService) {
  return tool({
    name: "updateActionPlanProgress",
    description:
      "Mark a specific task in an action plan as completed. Use this after you've finished configuring an operator from an accepted action plan.",
    inputSchema: z.object({
      actionPlanId: z.string().describe("ID of the action plan"),
      operatorId: z.string().describe("ID of the operator task to mark as complete"),
      completed: z.boolean().describe("Whether the task is completed (true) or not (false)"),
    }),
    execute: async (args: { actionPlanId: string; operatorId: string; completed: boolean }) => {
      try {
        const plan = actionPlanService.getActionPlan(args.actionPlanId);
        if (!plan) {
          return {
            success: false,
            error: `Action plan with ID ${args.actionPlanId} not found`,
          };
        }

        const task = plan.tasks.get(args.operatorId);
        if (!task) {
          return {
            success: false,
            error: `Task with operator ID ${args.operatorId} not found in action plan ${args.actionPlanId}`,
          };
        }

        actionPlanService.updateTaskCompletion(args.actionPlanId, args.operatorId, args.completed);

        return {
          success: true,
          message: `Task for operator ${args.operatorId} marked as ${args.completed ? "completed" : "incomplete"}`,
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
    name: "getActionPlan",
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
            status: plan.status$.value,
            createdAt: plan.createdAt.toISOString(),
            userFeedback: plan.userFeedback,
            operatorIds: plan.operatorIds,
            linkIds: plan.linkIds,
            tasks: Array.from(plan.tasks.values()).map(task => ({
              operatorId: task.operatorId,
              description: task.description,
              completed: task.completed$.value,
            })),
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
    name: "listActionPlans",
    description: "List all action plans in the system",
    inputSchema: z.object({
      filterByAgent: z.string().optional().describe("Optional: Filter by agent ID"),
      filterByStatus: z
        .string()
        .optional()
        .describe("Optional: Filter by status (pending, accepted, rejected, completed)"),
    }),
    execute: async (args: { filterByAgent?: string; filterByStatus?: string }) => {
      try {
        const allPlans = actionPlanService.getAllActionPlans();

        // Apply filters if provided
        let filteredPlans = allPlans;
        if (args.filterByAgent) {
          filteredPlans = filteredPlans.filter(plan => plan.agentId === args.filterByAgent);
        }
        if (args.filterByStatus) {
          filteredPlans = filteredPlans.filter(plan => plan.status$.value === args.filterByStatus);
        }

        // Convert to serializable format
        const plans = filteredPlans.map(plan => ({
          id: plan.id,
          agentId: plan.agentId,
          agentName: plan.agentName,
          executorAgentId: plan.executorAgentId,
          summary: plan.summary,
          status: plan.status$.value,
          createdAt: plan.createdAt.toISOString(),
          taskCount: plan.tasks.size,
          completedTasks: Array.from(plan.tasks.values()).filter(t => t.completed$.value).length,
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
    name: "deleteActionPlan",
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
    name: "updateActionPlan",
    description: "Update an action plan's properties",
    inputSchema: z.object({
      actionPlanId: z.string().describe("The ID of the action plan to update"),
      summary: z.string().optional().describe("New summary for the action plan"),
      status: z
        .enum(["pending", "accepted", "rejected", "completed"])
        .optional()
        .describe("New status for the action plan"),
      userFeedback: z.string().optional().describe("User feedback to add"),
    }),
    execute: async (args: { actionPlanId: string; summary?: string; status?: string; userFeedback?: string }) => {
      try {
        const plan = actionPlanService.getActionPlan(args.actionPlanId);
        if (!plan) {
          return { success: false, error: "Action plan not found" };
        }

        // Update fields if provided
        if (args.summary !== undefined) {
          plan.summary = args.summary;
        }
        if (args.status !== undefined) {
          plan.status$.next(args.status as any);
        }
        if (args.userFeedback !== undefined) {
          plan.userFeedback = args.userFeedback;
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
