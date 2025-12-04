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
import { AgentActionService } from "../../agent-action/agent-action.service";
import { ValidationWorkflowService } from "../../validation/validation-workflow.service";
import { WorkflowCompilingService } from "../../compile-workflow/workflow-compiling.service";

// Tool name constants
export const TOOL_NAME_ADD_TO_WORKFLOW = "addToWorkflow";
export const TOOL_NAME_MODIFY_IN_WORKFLOW = "modifyInWorkflow";
export const TOOL_NAME_DELETE_FROM_WORKFLOW = "deleteFromWorkflow";
export const TOOL_NAME_GET_AGENT_ACTION = "getAgentAction";
export const TOOL_NAME_LIST_AGENT_ACTIONS = "listAgentActions";
export const TOOL_NAME_DELETE_AGENT_ACTION = "deleteAgentAction";
export const TOOL_NAME_UPDATE_AGENT_ACTION = "updateAgentAction";

/**
 * Helper to get validation info for the current workflow
 */
function getWorkflowValidationInfo(
  workflowActionService: WorkflowActionService,
  validationWorkflowService: ValidationWorkflowService
) {
  const validationOutput = validationWorkflowService.getCurrentWorkflowValidationError();
  const errorCount = Object.keys(validationOutput.errors).length;
  const validGraph = validationWorkflowService.getValidTexeraGraph();
  const validOperators = validGraph.getAllOperators();
  const allOperators = workflowActionService.getTexeraGraph().getAllOperators();
  const validOperatorIds = validOperators.map(op => op.operatorID);

  return {
    errors: validationOutput.errors,
    errorCount,
    validOperatorIds,
    validCount: validOperators.length,
    totalCount: allOperators.length,
    invalidCount: allOperators.length - validOperators.length,
    message:
      errorCount === 0
        ? "No validation errors in the workflow"
        : `Found ${errorCount} operator(s) with validation errors`,
  };
}

/**
 * Helper to get detailed operator info including schemas
 */
function getOperatorInfo(
  operatorId: string,
  workflowActionService: WorkflowActionService,
  workflowCompilingService: WorkflowCompilingService
) {
  const operator = workflowActionService.getTexeraGraph().getOperator(operatorId);
  const inputSchema = workflowCompilingService.getOperatorInputSchemaMap(operatorId) || {};
  const outputSchema = workflowCompilingService.getOperatorOutputSchemaMap(operatorId) || {};

  return {
    operatorId: operator.operatorID,
    operatorType: operator.operatorType,
    customDisplayName: operator.customDisplayName,
    properties: operator,
    inputSchema,
    outputSchema,
  };
}

/**
 * Helper to create agent action and return its ID
 */
function createAndRecordAgentAction(
  workflowActionService: WorkflowActionService,
  agentActionService: AgentActionService,
  agentId: string,
  agentName: string,
  summary: string,
  operations: {
    add?: { operatorIds: string[]; linkIds: string[] };
    modify?: { operatorIds: string[] };
    delete?: { operatorIds: string[]; linkIds: string[] };
  },
  beforeWorkflowContent: any,
  afterWorkflowContent: any
) {
  const allOperatorIds = [...(operations.add?.operatorIds || []), ...(operations.modify?.operatorIds || [])];
  const allLinkIds = operations.add?.linkIds || [];

  return agentActionService.createAgentAction(
    agentId,
    agentName || "AI Agent",
    summary,
    {
      add: operations.add || { operatorIds: [], linkIds: [] },
      modify: operations.modify || { operatorIds: [] },
      delete: operations.delete || { operatorIds: [], linkIds: [] },
    },
    allOperatorIds,
    allLinkIds,
    workflowActionService.getWorkflowMetadata(),
    beforeWorkflowContent,
    afterWorkflowContent
  );
}

/**
 * Create addToWorkflow tool for adding operators and links
 */
export function createAddToWorkflowTool(
  workflowActionService: WorkflowActionService,
  agentActionService: AgentActionService,
  validationWorkflowService: ValidationWorkflowService,
  workflowCompilingService: WorkflowCompilingService,
  agentId: string = "",
  agentName: string = ""
) {
  return tool({
    name: TOOL_NAME_ADD_TO_WORKFLOW,
    description:
      "Add new operators and/or links to the workflow. Returns detailed info for each added operator including schemas.",
    inputSchema: z.object({
      summary: z.string().describe("A summary of what this add operation accomplishes"),
      operators: z
        .array(
          z.object({
            operatorType: z.string().describe("Type of operator"),
            customDisplayName: z.string().describe("Brief custom name summarizing what this operator does"),
            properties: z
              .record(z.any())
              .describe("Properties to set for this operator. Must obey the schema of this type of operator"),
          })
        )
        .optional()
        .describe("List of operators to add"),
      links: z
        .array(
          z.object({
            sourceOperatorId: z
              .string()
              .describe("ID of source operator - existing ID or index ('0', '1') for newly added operators"),
            targetOperatorId: z
              .string()
              .describe("ID of target operator - existing ID or index ('0', '1') for newly added operators"),
            sourcePortId: z.string().optional().describe("Port ID on source (default: 'output-0')"),
            targetPortId: z.string().optional().describe("Port ID on target (default: 'input-0')"),
          })
        )
        .optional()
        .describe("List of links to connect operators"),
    }),
    execute: async (args: {
      summary: string;
      operators?: Array<{
        operatorType: string;
        customDisplayName?: string;
        properties?: Record<string, any>;
      }>;
      links?: Array<{
        sourceOperatorId: string;
        targetOperatorId: string;
        sourcePortId?: string;
        targetPortId?: string;
      }>;
    }) => {
      try {
        const beforeContent = workflowActionService.getWorkflowContent();
        const results = workflowActionService.applyAgentAction({ add: args });

        if (!results.success) {
          return { success: false, error: results.error || "Failed to add operators/links" };
        }

        const afterContent = workflowActionService.getWorkflowContent();

        // Get detailed info for each added operator
        const addedOperators = results.addedOperatorIds.map(id =>
          getOperatorInfo(id, workflowActionService, workflowCompilingService)
        );

        // Get info for added links
        const addedLinks = results.addedLinkIds.map(linkId => {
          const link = workflowActionService.getTexeraGraph().getLinkWithID(linkId);
          return {
            linkId,
            source: link?.source,
            target: link?.target,
          };
        });

        // Auto layout the workflow after adding operators/links
        workflowActionService.autoLayoutWorkflow();

        const agentAction = createAndRecordAgentAction(
          workflowActionService,
          agentActionService,
          agentId,
          agentName,
          args.summary,
          { add: { operatorIds: results.addedOperatorIds, linkIds: results.addedLinkIds } },
          beforeContent,
          workflowActionService.getWorkflowContent() // Get updated content after auto layout
        );

        return {
          success: true,
          agentActionId: agentAction.id,
          addedOperators,
          addedLinks,
          validation: getWorkflowValidationInfo(workflowActionService, validationWorkflowService),
          message: `Added ${results.addedOperatorIds.length} operator(s) and ${results.addedLinkIds.length} link(s)`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create modifyInWorkflow tool for modifying operator properties
 */
export function createModifyInWorkflowTool(
  workflowActionService: WorkflowActionService,
  agentActionService: AgentActionService,
  validationWorkflowService: ValidationWorkflowService,
  workflowCompilingService: WorkflowCompilingService,
  agentId: string = "",
  agentName: string = ""
) {
  return tool({
    name: TOOL_NAME_MODIFY_IN_WORKFLOW,
    description:
      "Modify properties of existing operators. Returns the updated operator info including schemas after modification.",
    inputSchema: z.object({
      summary: z.string().describe("A summary of what this modification accomplishes"),
      operators: z
        .array(
          z.object({
            operatorId: z.string().describe("ID of the operator to modify"),
            operatorProperties: z.record(z.any()).describe("Operator properties to update"),
          })
        )
        .describe("List of operators to modify with their new properties"),
    }),
    execute: async (args: {
      summary: string;
      operators: Array<{ operatorId: string; operatorProperties: Record<string, any> }>;
    }) => {
      try {
        const beforeContent = workflowActionService.getWorkflowContent();
        const results = workflowActionService.applyAgentAction({ modify: args });

        if (!results.success) {
          return { success: false, error: results.error || "Failed to modify operators" };
        }

        // Get updated operator info after modification
        const modifiedOperators = results.modifiedOperatorIds.map(id =>
          getOperatorInfo(id, workflowActionService, workflowCompilingService)
        );

        // Auto layout the workflow after modifying operators
        workflowActionService.autoLayoutWorkflow();

        const agentAction = createAndRecordAgentAction(
          workflowActionService,
          agentActionService,
          agentId,
          agentName,
          args.summary,
          { modify: { operatorIds: results.modifiedOperatorIds } },
          beforeContent,
          workflowActionService.getWorkflowContent() // Get updated content after auto layout
        );

        return {
          success: true,
          agentActionId: agentAction.id,
          modifiedOperators,
          validation: getWorkflowValidationInfo(workflowActionService, validationWorkflowService),
          message: `Modified ${results.modifiedOperatorIds.length} operator(s)`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create deleteFromWorkflow tool for deleting operators and links
 */
export function createDeleteFromWorkflowTool(
  workflowActionService: WorkflowActionService,
  agentActionService: AgentActionService,
  validationWorkflowService: ValidationWorkflowService,
  agentId: string = "",
  agentName: string = ""
) {
  return tool({
    name: TOOL_NAME_DELETE_FROM_WORKFLOW,
    description: "Delete operators and/or links from the workflow.",
    inputSchema: z.object({
      summary: z.string().describe("A summary of what this delete operation accomplishes"),
      operatorIds: z.array(z.string()).optional().describe("List of operator IDs to delete"),
      linkIds: z.array(z.string()).optional().describe("List of link IDs to delete"),
    }),
    execute: async (args: { summary: string; operatorIds?: string[]; linkIds?: string[] }) => {
      try {
        const beforeContent = workflowActionService.getWorkflowContent();
        const results = workflowActionService.applyAgentAction({ delete: args });

        if (!results.success) {
          return { success: false, error: results.error || "Failed to delete operators/links" };
        }

        // Auto layout the workflow after deleting operators/links
        workflowActionService.autoLayoutWorkflow();

        const agentAction = createAndRecordAgentAction(
          workflowActionService,
          agentActionService,
          agentId,
          agentName,
          args.summary,
          { delete: { operatorIds: results.deletedOperatorIds, linkIds: results.deletedLinkIds } },
          beforeContent,
          workflowActionService.getWorkflowContent() // Get updated content after auto layout
        );

        return {
          success: true,
          agentActionId: agentAction.id,
          deletedOperatorIds: results.deletedOperatorIds,
          deletedLinkIds: results.deletedLinkIds,
          validation: getWorkflowValidationInfo(workflowActionService, validationWorkflowService),
          message: `Deleted ${results.deletedOperatorIds.length} operator(s) and ${results.deletedLinkIds.length} link(s)`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}
