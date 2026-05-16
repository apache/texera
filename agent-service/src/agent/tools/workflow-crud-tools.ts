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
import type { OperatorLink } from "../../types/workflow";
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
  /** Returns the file context for the current message turn, if any. */
  getFileContext?: () => { fileName: string; filePath: string } | undefined;
  /** Tracks how many times each operatorType has been added this turn (for loop detection). */
  operatorTypeAddCount?: Map<string, number>;
  /** Tracks how many times each operatorId has been modified this turn (for loop detection). */
  operatorModifyCount?: Map<string, number>;
  /** Aborts generation immediately (used by loop detector). */
  abort?: () => void;
  /** Lazily creates a workflow when the agent first needs to build operators. */
  ensureWorkflow?: () => Promise<void>;
  /**
   * Clears the current workflow (operators + workflowId) so the next ensureWorkflow()
   * creates a fresh one. Used when the user loads a different file into a workflow that
   * already contains operators from a previous file.
   */
  resetWorkflow?: () => void;
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
      properties: z.record(z.any()).optional().describe("Properties to set on the operator"),
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
        // Resolve the incoming fileName early (may come from args.properties or fileContext
        // auto-injection) so the reset check sees it regardless of which path sets it.
        const isScanOp = args.operatorType.toLowerCase().includes("scan");
        const incomingFileName: string | undefined =
          args.properties?.fileName ||
          (isScanOp && context?.getFileContext ? context.getFileContext()?.filePath : undefined);

        // If the user is loading a different file into a workflow that already has operators,
        // reset to a fresh workflow so we don't mix operators from different analyses.
        if (isScanOp && incomingFileName && context?.resetWorkflow) {
          const existingScans = workflowState.getAllOperators().filter(op =>
            op.operatorType.toLowerCase().includes("scan")
          );
          if (existingScans.length > 0) {
            const existingFile = (existingScans[0] as any).operatorProperties?.fileName;
            if (existingFile && existingFile !== incomingFileName) {
              context.resetWorkflow();
            }
          }
        }

        // Lazily create a workflow the first time the agent needs to build operators.
        if (context?.ensureWorkflow) await context.ensureWorkflow();

        const inputInfo = formatInputArgs(args);

        const schemaEntry = operatorSchemas.get(args.operatorType);
        if (!schemaEntry) {
          // Fuzzy-match: find types whose name contains the requested name (case-insensitive)
          // or whose requested name contains the type name.
          const req = args.operatorType.toLowerCase();
          const allTypes = [...operatorSchemas.keys()];
          const suggestions = allTypes.filter(t => {
            const tl = t.toLowerCase();
            return tl.includes(req) || req.includes(tl) || req.split(/(?=[A-Z])/).some((w: string) => tl.includes(w.toLowerCase()));
          });
          const hint = suggestions.length > 0
            ? `Did you mean: ${suggestions.join(", ")}?`
            : `Search the available operator list in the system prompt for a visualization operator.`;
          return createErrorResult(`Unknown operator type: "${args.operatorType}". ${hint} ${inputInfo}`);
        }

        // Loop detection: if the same operatorType has been added 3+ times this turn, abort.
        if (context?.operatorTypeAddCount) {
          const prev = context.operatorTypeAddCount.get(args.operatorType) ?? 0;
          if (prev >= 3) {
            // Abort generation — the LLM ignores plain errors but abort stops the loop.
            context.abort?.();
            return createErrorResult(
              `You have already added "${args.operatorType}" ${prev} times this turn without success. ` +
                `Generation is stopping. Tell the user what was built so far and that ` +
                `this specific step requires a different approach or manual configuration.`
            );
          }
          context.operatorTypeAddCount.set(args.operatorType, prev + 1);
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

        // Auto-inject fileName from file context when the operator needs one but none was provided.
        let resolvedProperties = args.properties || {};
        if (!resolvedProperties.fileName && context?.getFileContext) {
          const fc = context.getFileContext();
          const schema = context.metadataStore?.getSchema(args.operatorType);
          const needsFileName =
            schema?.properties?.fileName !== undefined ||
            schema?.required?.includes("fileName");
          if (fc && needsFileName) {
            resolvedProperties = { ...resolvedProperties, fileName: fc.filePath };
          }
        }

        const operatorTemplate = workflowUtil.getNewOperatorPredicate(args.operatorType, args.summary);

        // If this operator needs inputs but none were specified, block and explain.
        if (operatorTemplate.inputPorts.length > 0 && !args.inputOperatorIds) {
          const existingOps = workflowState.getAllOperators();
          if (existingOps.length > 0) {
            const opList = existingOps
              .map(o => `${o.operatorID} (${o.operatorType})`)
              .join(", ");
            // Don't count this blocked attempt toward the repetition limit.
            if (context?.operatorTypeAddCount) {
              const cur = context.operatorTypeAddCount.get(args.operatorType) ?? 0;
              context.operatorTypeAddCount.set(args.operatorType, Math.max(0, cur - 1));
            }
            return createErrorResult(
              `Operator "${args.operatorId}" (${args.operatorType}) requires ${operatorTemplate.inputPorts.length} input(s) but no inputOperatorIds was provided. ` +
                `You MUST specify which operator to connect it to using inputOperatorIds in this same addOperator call. ` +
                `Available operators to connect to: ${opList}. ` +
                `Example: {"0": ["${existingOps[0].operatorID}"]}`
            );
          }
        }

        let operator = operatorTemplate;
        operator = {
          ...operator,
          operatorID: args.operatorId,
          operatorProperties: { ...operator.operatorProperties, ...resolvedProperties },
        };

        workflowState.addOperator(operator);

        const createdLinkPairs: { source: string; target: string }[] = [];
        if (args.inputOperatorIds) {
          const addedOperator = workflowState.getOperator(operator.operatorID)!;
          for (const [portIndexStr, sourceOpIds] of Object.entries(args.inputOperatorIds)) {
            const targetPortIdx = parseInt(portIndexStr, 10);
            if (isNaN(targetPortIdx) || targetPortIdx < 0) {
              return createErrorResult(
                `Invalid input port index: "${portIndexStr}". Must be a non-negative integer. ${inputInfo}`
              );
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
              const sourcePortId = sourceOp.outputPorts.length > 0 ? sourceOp.outputPorts[0].portID : "output-0";

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

        // Loop detection: abort if the same operator has been modified 5+ times this turn.
        if (context?.operatorModifyCount) {
          const prev = context.operatorModifyCount.get(args.operatorId) ?? 0;
          if (prev >= 5) {
            context.abort?.();
            return createErrorResult(
              `You have already modified "${args.operatorId}" ${prev} times this turn without success. ` +
                `Generation is stopping. Tell the user what was built and what step is blocking progress.`
            );
          }
          context.operatorModifyCount.set(args.operatorId, prev + 1);
        }

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

          for (const [portIndexStr, sourceOpIds] of Object.entries(args.inputOperatorIds)) {
            const targetPortIdx = parseInt(portIndexStr, 10);
            if (isNaN(targetPortIdx) || targetPortIdx < 0) {
              return createErrorResult(
                `Invalid input port index: "${portIndexStr}". Must be a non-negative integer. ${inputInfo}`
              );
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
              const sourcePortId = sourceOp.outputPorts.length > 0 ? sourceOp.outputPorts[0].portID : "output-0";

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
