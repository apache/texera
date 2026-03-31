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

/**
 * General operator tools for Texera Agent Service (GENERAL mode).
 * These tools work with any operator type using operator schemas.
 */

import { z } from "zod";
import { tool } from "ai";
import { WorkflowState } from "../workflow/workflow-state";
import { autoLayoutWorkflow } from "../workflow/auto-layout";
import { WorkflowUtilService } from "../workflow/workflow-util";
import type { OperatorLink } from "../types/workflow";
import {
  createToolResult,
  createErrorResult,
  formatAddOperatorResult,
  formatModifyOperatorResult,
  formatOperatorError,
} from "./tools-utility";
import { formatValidationErrors, formatCompactSchemaForError } from "./metadata-tools";

import type { ToolContext } from "./workflow-tools";

// ============================================================================
// Tool Name Constants
// ============================================================================

export const TOOL_NAME_ADD_OPERATOR = "addOperator";
export const TOOL_NAME_MODIFY_OPERATOR = "modifyOperator";

/**
 * Format tool input args as a compact string for inclusion in error messages.
 * Omits undefined values to keep it concise.
 */
function formatInputArgs(args: Record<string, any>): string {
  const compact: Record<string, any> = {};
  for (const [key, value] of Object.entries(args)) {
    if (value !== undefined) compact[key] = value;
  }
  return `Input: ${JSON.stringify(compact)}`;
}

// ============================================================================
// Add Operator Tool
// ============================================================================

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
      operatorId: z.string().describe(
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

        // Validate properties
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

        // Validate operatorId follows the "op{number}" naming convention
        if (!/^op\d+$/.test(args.operatorId)) {
          return createErrorResult(
            `Invalid operatorId: "${args.operatorId}". Must follow the format "op" followed by a number (e.g., op1, op2, op3). ${inputInfo}`
          );
        }

        // Check for duplicate operatorId
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

        workflowState.addOperator(operator);

        // Automatically create links from inputOperatorIds
        const createdLinkPairs: { source: string; target: string }[] = [];
        if (args.inputOperatorIds) {
          const addedOperator = workflowState.getOperator(operator.operatorID)!;
          for (const [portIndexStr, sourceOpIds] of Object.entries(args.inputOperatorIds)) {
            const targetPortIdx = parseInt(portIndexStr, 10);
            if (isNaN(targetPortIdx) || targetPortIdx < 0) {
              return createErrorResult(`Invalid input port index: "${portIndexStr}". Must be a non-negative integer. ${inputInfo}`);
            }
            if (targetPortIdx >= addedOperator.inputPorts.length) {
              return createErrorResult(
                `Input port index ${targetPortIdx} out of range. Operator "${args.operatorId}" has ${addedOperator.inputPorts.length} input port(s). ${inputInfo}`
              );
            }
            const targetPortId = addedOperator.inputPorts[targetPortIdx].portID;

            for (const sourceOpId of sourceOpIds) {
              const sourceOp = workflowState.getOperator(sourceOpId);
              if (!sourceOp) {
                return createErrorResult(
                  `Source operator "${sourceOpId}" not found. Make sure it exists before referencing it in inputOperatorIds. ${inputInfo}`
                );
              }
              const sourcePortId =
                sourceOp.outputPorts.length > 0 ? sourceOp.outputPorts[0].portID : "output-0";

              const linkId = workflowState.generateLinkId();
              const link: OperatorLink = {
                linkID: linkId,
                source: { operatorID: sourceOpId, portID: sourcePortId },
                target: { operatorID: args.operatorId, portID: targetPortId },
              };
              workflowState.addLink(link);
              createdLinkPairs.push({ source: sourceOpId, target: args.operatorId });
            }
          }
        }

        // Auto-layout the workflow after adding the operator and links
        autoLayoutWorkflow(workflowState);

        const finalOperator = workflowState.getOperator(operator.operatorID) || operator;
        const numInputPorts = finalOperator.inputPorts.length;
        const numOutputPorts = finalOperator.outputPorts.length;

        let resultMsg = formatAddOperatorResult(
          operator.operatorID, numInputPorts, numOutputPorts,
          createdLinkPairs.length > 0 ? createdLinkPairs : undefined
        );

        // Auto-execute the operator after adding
        if (context?.executeOperator) {
          const executionResult = await context.executeOperator(operator.operatorID);
          if (executionResult.startsWith("[ERROR]")) {
            resultMsg += `\n\n${executionResult}`;
          } else {
            resultMsg += `\n\n${executionResult}`;
          }
        }

        return createToolResult(resultMsg);
      } catch (error: any) {
        return createErrorResult(error.message || String(error));
      }
    },
  });
}

// ============================================================================
// Modify Operator Tool
// ============================================================================

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

        // Validate properties if provided
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

        const createdLinkPairs: { source: string; target: string }[] = [];
        const deletedLinkPairs: { source: string; target: string }[] = [];

        // Update properties if provided
        if (args.properties) {
          workflowState.updateOperatorProperties(args.operatorId, args.properties);
        }

        // Update summary (customDisplayName) if provided
        if (args.summary) {
          workflowState.updateOperatorDisplayName(args.operatorId, args.summary);
        }

        // Replace incoming links if inputOperatorIds is provided
        if (args.inputOperatorIds) {
          // Delete all existing incoming links
          const currentLinks = workflowState.getLinksConnectedToOperator(args.operatorId)
            .filter(link => link.target.operatorID === args.operatorId);
          for (const link of currentLinks) {
            deletedLinkPairs.push({ source: link.source.operatorID, target: link.target.operatorID });
            workflowState.deleteLink(link.linkID);
          }

          // Create new incoming links
          for (const [portIndexStr, sourceOpIds] of Object.entries(args.inputOperatorIds)) {
            const targetPortIdx = parseInt(portIndexStr, 10);
            if (isNaN(targetPortIdx) || targetPortIdx < 0) {
              return createErrorResult(`Invalid input port index: "${portIndexStr}". Must be a non-negative integer. ${inputInfo}`);
            }
            if (targetPortIdx >= operator.inputPorts.length) {
              return createErrorResult(
                `Input port index ${targetPortIdx} out of range. Operator "${args.operatorId}" has ${operator.inputPorts.length} input port(s). ${inputInfo}`
              );
            }
            const targetPortId = operator.inputPorts[targetPortIdx].portID;

            for (const sourceOpId of sourceOpIds) {
              const sourceOp = workflowState.getOperator(sourceOpId);
              if (!sourceOp) {
                return createErrorResult(
                  `Source operator "${sourceOpId}" not found. Make sure it exists before referencing it in inputOperatorIds. ${inputInfo}`
                );
              }
              const sourcePortId =
                sourceOp.outputPorts.length > 0 ? sourceOp.outputPorts[0].portID : "output-0";

              const linkId = workflowState.generateLinkId();
              const link: OperatorLink = {
                linkID: linkId,
                source: { operatorID: sourceOpId, portID: sourcePortId },
                target: { operatorID: args.operatorId, portID: targetPortId },
              };
              workflowState.addLink(link);
              createdLinkPairs.push({ source: sourceOpId, target: args.operatorId });
            }
          }

          autoLayoutWorkflow(workflowState);
        }

        let resultMsg = formatModifyOperatorResult(
          args.operatorId,
          createdLinkPairs.length > 0 ? createdLinkPairs : undefined,
          deletedLinkPairs.length > 0 ? deletedLinkPairs : undefined
        );

        // Auto-execute the operator after modifying
        if (context?.executeOperator) {
          const executionResult = await context.executeOperator(args.operatorId);
          resultMsg += `\n\n${executionResult}`;
        }

        return createToolResult(resultMsg);
      } catch (error: any) {
        return createErrorResult(formatOperatorError(args.operatorId, error.message || String(error)));
      }
    },
  });
}
