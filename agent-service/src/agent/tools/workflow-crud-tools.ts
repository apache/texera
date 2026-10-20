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
import { WorkflowState } from "../workflow-state";
import { autoLayoutWorkflow } from "../util/auto-layout";
import { WorkflowUtilService } from "../util/workflow-utils";
import type { OperatorLink, PortDescription } from "../../types/workflow";
import {
  createToolResult,
  createErrorResult,
  formatAddOperatorResult,
  formatModifyOperatorResult,
  formatOperatorError,
} from "./tools-utility";
import {
  type WorkflowSystemMetadata,
  formatValidationErrors,
  formatCompactSchemaForError,
} from "../util/workflow-system-metadata";

export interface ToolContext {
  metadataStore?: WorkflowSystemMetadata;
  settings?: {
    maxOperatorResultCharLimit?: number;
    toolTimeoutMs?: number;
    executionTimeoutMs?: number;
  };
}

export const TOOL_NAME_ADD_OPERATOR = "addOperator";
export const TOOL_NAME_MODIFY_OPERATOR = "modifyOperator";
export const TOOL_NAME_DELETE_OPERATOR = "deleteOperator";

function formatInputArgs(args: Record<string, any>): string {
  const compact: Record<string, any> = {};
  for (const [key, value] of Object.entries(args)) {
    if (value !== undefined) compact[key] = value;
  }
  return `Input: ${JSON.stringify(compact)}`;
}

/**
 * Checks every port index and source operator in inputOperatorIds and returns the links to create,
 * or the first error. It doesn't change the workflow, so callers must run it before any mutation.
 */
function resolveInputLinks(
  workflowState: WorkflowState,
  operatorId: string,
  inputPorts: PortDescription[],
  inputOperatorIds: Record<string, string[]> | undefined,
  inputInfo: string
): { links: Omit<OperatorLink, "linkID">[] } | { error: string } {
  const links: Omit<OperatorLink, "linkID">[] = [];
  for (const [portIndexStr, sourceOpIds] of Object.entries(inputOperatorIds ?? {})) {
    if (!/^\d+$/.test(portIndexStr)) {
      return { error: `Invalid input port index: "${portIndexStr}". Must be a non-negative integer. ${inputInfo}` };
    }
    const targetPortIdx = parseInt(portIndexStr, 10);
    if (targetPortIdx >= inputPorts.length) {
      return {
        error: `Input port index ${targetPortIdx} out of range. Operator "${operatorId}" has ${inputPorts.length} input port(s). ${inputInfo}`,
      };
    }
    const targetPortId = inputPorts[targetPortIdx].portID;

    for (const sourceOpId of sourceOpIds) {
      if (sourceOpId === operatorId) {
        return { error: `Operator "${operatorId}" cannot be its own input source. ${inputInfo}` };
      }
      const sourceOp = workflowState.getOperator(sourceOpId);
      if (!sourceOp) {
        return {
          error: `Source operator "${sourceOpId}" not found. Make sure it exists before referencing it in inputOperatorIds. ${inputInfo}`,
        };
      }
      const sourcePortId = sourceOp.outputPorts.length > 0 ? sourceOp.outputPorts[0].portID : "output-0";
      links.push({
        source: { operatorID: sourceOpId, portID: sourcePortId },
        target: { operatorID: operatorId, portID: targetPortId },
      });
    }
  }
  return { links };
}

export function createAddOperatorTool(
  workflowState: WorkflowState,
  operatorSchemas: Map<string, any>,
  context?: ToolContext
) {
  const workflowUtil = context?.metadataStore ? new WorkflowUtilService(context.metadataStore, workflowState) : null;

  return tool({
    description: `Add a new operator to the workflow. Use getOperatorSchema first to understand required properties.

Examples:
1. Add a source operator (no inputs):
   { "operatorId": "op1", "operatorType": "TableFileScan", "properties": { "fileName": "data.csv" }, "summary": "Load CSV data" }

2. Add an operator with input connections:
   { "operatorId": "op2", "operatorType": "TableFilter", "properties": { "predicates": [...] }, "inputOperatorIds": { "0": ["op1"] }, "summary": "Filter rows by condition" }`,
    inputSchema: z.object({
      operatorId: z
        .string()
        .describe(
          "Name of Operator. Use the format 'op' followed by an incrementing number starting from 1 (e.g., op1, op2, op3)."
        ),
      operatorType: z.string().describe("The operator type (e.g., 'DataProcessing', 'Aggregate')"),
      properties: z.record(z.any()).describe("Properties to set on the operator"),
      inputOperatorIds: z
        .record(z.array(z.string()))
        .optional()
        .describe(
          "Mapping from input port index to an ordered list of source operator IDs that connect to that port. " +
            'E.g. {"0": ["opA", "opB"], "1": ["opC"]} connects opA and opB to input port 0, opC to input port 1. ' +
            "Source operators that load files (e.g. CSVFileScan) should NOT have any input operators."
        ),
      summary: z.string().describe("Very brief summary of operator behavior. Within 5 words"),
    }),
    execute: async (args: {
      operatorId: string;
      operatorType: string;
      properties?: Record<string, any>;
      inputOperatorIds?: Record<string, string[]>;
      summary: string;
    }) => {
      try {
        const inputInfo = formatInputArgs(args);

        const schemaEntry = operatorSchemas.get(args.operatorType);
        if (!schemaEntry) {
          return createErrorResult(
            `Unknown operator type: "${args.operatorType}". Available types: ${[...operatorSchemas.keys()].join(", ")}. ${inputInfo}`
          );
        }

        if (context?.metadataStore && args.properties) {
          const validation = context.metadataStore.validateOperatorProperties(args.operatorType, args.properties);
          if (!validation.isValid) {
            const compactSchema = context.metadataStore.getCompactSchema(args.operatorType);
            const schemaStr = compactSchema ? ` Expected: ${formatCompactSchemaForError(compactSchema)}.` : "";
            return createErrorResult(
              `Invalid properties for "${args.operatorType}": ${formatValidationErrors(validation)}.${schemaStr} ${inputInfo}`
            );
          }
        }

        if (!workflowUtil) {
          return createErrorResult(`Metadata store not available for operator creation. ${inputInfo}`);
        }

        if (!/^op\d+$/.test(args.operatorId)) {
          return createErrorResult(
            `Invalid operatorId: "${args.operatorId}". Must follow the format "op" followed by a number (e.g., op1, op2, op3). ${inputInfo}`
          );
        }

        const existing = workflowState.getOperator(args.operatorId);
        if (existing) {
          return createErrorResult(
            `Operator with ID "${args.operatorId}" already exists. Use modifyOperator to update it, or choose a different ID. ${inputInfo}`
          );
        }

        let operator = workflowUtil.getNewOperatorPredicate(args.operatorType, args.summary);
        operator = {
          ...operator,
          operatorID: args.operatorId,
          operatorProperties: { ...operator.operatorProperties, ...args.properties },
        };

        const resolved = resolveInputLinks(
          workflowState,
          args.operatorId,
          operator.inputPorts,
          args.inputOperatorIds,
          inputInfo
        );
        if ("error" in resolved) return createErrorResult(resolved.error);

        workflowState.addOperator(operator);

        const createdLinkPairs: { source: string; target: string }[] = [];
        for (const link of resolved.links) {
          workflowState.addLink({ linkID: workflowState.generateLinkId(), ...link });
          createdLinkPairs.push({ source: link.source.operatorID, target: args.operatorId });
        }

        autoLayoutWorkflow(workflowState);

        const finalOperator = workflowState.getOperator(operator.operatorID) || operator;
        const numInputPorts = finalOperator.inputPorts.length;
        const numOutputPorts = finalOperator.outputPorts.length;

        let resultMsg = formatAddOperatorResult(
          operator.operatorID,
          numInputPorts,
          numOutputPorts,
          createdLinkPairs.length > 0 ? createdLinkPairs : undefined
        );

        return createToolResult(resultMsg);
      } catch (error: any) {
        return createErrorResult(error.message || String(error));
      }
    },
  });
}

export function createModifyOperatorTool(workflowState: WorkflowState, context?: ToolContext) {
  return tool({
    description: `Modify an existing operator's properties, input links, or both.

Examples:
1. Modify properties only:
   { "operatorId": "agg", "properties": { "groupByKeys": ["city"] }, "summary": "Group by city" }

2. Modify input links only (replaces all existing incoming links):
   { "operatorId": "join_op", "inputOperatorIds": { "0": ["users"], "1": ["orders"] }, "summary": "Re-link join inputs" }

3. Modify both properties and links:
   { "operatorId": "filter", "properties": { "predicates": [...] }, "inputOperatorIds": { "0": ["cleaned"] }, "summary": "Update filter and re-link" }`,
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to modify"),
      properties: z.record(z.any()).optional().describe("Properties to update (merged with existing)"),
      inputOperatorIds: z
        .record(z.array(z.string()))
        .optional()
        .describe(
          "Mapping from input port index to an ordered list of source operator IDs. " +
            "If provided, all existing incoming links are deleted and replaced with these. " +
            'E.g. {"0": ["opA", "opB"], "1": ["opC"]} connects opA and opB to input port 0, opC to input port 1.'
        ),
      summary: z.string().describe("Very brief summary of operator behavior after your modification. Within 5 words"),
    }),
    execute: async (args: {
      operatorId: string;
      properties?: Record<string, any>;
      inputOperatorIds?: Record<string, string[]>;
      summary?: string;
    }) => {
      try {
        const inputInfo = formatInputArgs(args);

        const operator = workflowState.getOperator(args.operatorId);
        if (!operator) return createErrorResult(`Operator ${args.operatorId} not found. ${inputInfo}`);

        if (args.properties && context?.metadataStore) {
          const mergedProperties = { ...operator.operatorProperties, ...args.properties };
          const validation = context.metadataStore.validateOperatorProperties(operator.operatorType, mergedProperties);
          if (!validation.isValid) {
            const compactSchema = context.metadataStore.getCompactSchema(operator.operatorType);
            const schemaStr = compactSchema ? ` Expected: ${formatCompactSchemaForError(compactSchema)}.` : "";
            return createErrorResult(
              `Invalid properties for "${operator.operatorType}": ${formatValidationErrors(validation)}.${schemaStr} ${inputInfo}`
            );
          }
        }

        const resolved = resolveInputLinks(
          workflowState,
          args.operatorId,
          operator.inputPorts,
          args.inputOperatorIds,
          inputInfo
        );
        if ("error" in resolved) return createErrorResult(resolved.error);

        const createdLinkPairs: { source: string; target: string }[] = [];
        const deletedLinkPairs: { source: string; target: string }[] = [];

        if (args.properties) {
          workflowState.updateOperatorProperties(args.operatorId, args.properties);
        }

        if (args.summary) {
          workflowState.updateOperatorDisplayName(args.operatorId, args.summary);
        }

        if (args.inputOperatorIds) {
          const currentLinks = workflowState
            .getLinksConnectedToOperator(args.operatorId)
            .filter(link => link.target.operatorID === args.operatorId);
          for (const link of currentLinks) {
            deletedLinkPairs.push({ source: link.source.operatorID, target: link.target.operatorID });
            workflowState.deleteLink(link.linkID);
          }

          for (const link of resolved.links) {
            workflowState.addLink({ linkID: workflowState.generateLinkId(), ...link });
            createdLinkPairs.push({ source: link.source.operatorID, target: args.operatorId });
          }

          autoLayoutWorkflow(workflowState);
        }

        let resultMsg = formatModifyOperatorResult(
          args.operatorId,
          createdLinkPairs.length > 0 ? createdLinkPairs : undefined,
          deletedLinkPairs.length > 0 ? deletedLinkPairs : undefined
        );

        return createToolResult(resultMsg);
      } catch (error: any) {
        return createErrorResult(formatOperatorError(args.operatorId, error.message || String(error)));
      }
    },
  });
}

export function createDeleteOperatorTool(workflowState: WorkflowState, _context?: ToolContext) {
  return tool({
    description: "Delete an operator from the workflow. This also deletes all connected links.",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to delete"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        const deleted = workflowState.deleteOperator(args.operatorId);
        if (!deleted) {
          return createErrorResult(`Operator ${args.operatorId} not found`);
        }
        return createToolResult(`Deleted operator: ${args.operatorId}`);
      } catch (error: any) {
        return createErrorResult(error.message || String(error));
      }
    },
  });
}
