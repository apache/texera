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
 * Latest-only message filter.
 *
 * When the agent iterates on a workflow over many steps, the message history
 * accumulates tool calls for operators that were later modified or deleted.
 * This filter keeps only the **latest** tool call/result for each operator
 * that **still exists** in the current workflow, removing stale context.
 *
 * Definition tools (createOrModify, addOperator, modifyOperator) and execution
 * tools (executeOperator) are tracked independently. This ensures that when
 * executeOperator is the latest call, the most recent definition is also kept,
 * preserving the operator's code/configuration in context.
 */

import type { ModelMessage } from "ai";
import type { WorkflowState } from "../workflow/workflow-state";
import { TOOL_NAME_CREATE_OR_MODIFY_OPERATOR } from "../tools/code-op-tools";
import { TOOL_NAME_EXECUTE_OPERATOR } from "../tools/execution-tools";
import {
  TOOL_NAME_DELETE_OPERATOR,
} from "../tools/workflow-tools";
import { TOOL_NAME_ADD_OPERATOR, TOOL_NAME_MODIFY_OPERATOR } from "../tools/general-op-tools";
import { TOOL_NAME_LIST_ALL_AVAILABLE_OPERATOR_TYPES, TOOL_NAME_GET_OPERATOR_SCHEMA } from "../tools/metadata-tools";

// ============================================================================
// Operator ID Extraction
// ============================================================================

/** Tools that never reference a specific operator — always keep. */
const NO_OPERATOR_TOOLS = new Set([
  TOOL_NAME_LIST_ALL_AVAILABLE_OPERATOR_TYPES,
  TOOL_NAME_GET_OPERATOR_SCHEMA,
]);

/**
 * Extract all operator IDs referenced by a tool-call's arguments.
 * Returns an empty array for tools that don't reference operators.
 */
function extractOperatorIds(toolName: string, params: Record<string, any>): string[] {
  if (NO_OPERATOR_TOOLS.has(toolName)) return [];

  const ids: string[] = [];

  // Single operatorId (createOrModifyOperator, executeOperator, deleteOperator,
  // addOperator, modifyOperator)
  if (typeof params.operatorId === "string") {
    ids.push(params.operatorId);
  }

  return ids;
}

// ============================================================================
// Latest-Only Filter
// ============================================================================

interface ToolCallEntry {
  /** Index of the assistant message in the messages array */
  messageIndex: number;
  /** Index of the tool-call part within the message's content array */
  partIndex: number;
  /** The tool call ID (used to match corresponding tool-result) */
  toolCallId: string;
  /** Tool name */
  toolName: string;
  /** Operator IDs referenced by this tool call */
  operatorIds: string[];
}

/**
 * Filter message history to keep only the latest tool call/result for each
 * operator that still exists in the current workflow.
 *
 * Algorithm:
 * 1. Scan assistant messages to build a chronological list of tool-call entries.
 * 2. Reverse-traverse to find the latest call per operator, marking stale ones:
 *    - Deleted operators / stale deletes / stale non-definition calls → full removal.
 *    - Stale definition calls (createOrModify, addOperator, modifyOperator) → trim
 *      only the execution result data, keeping the tool-call (code/args) and
 *      metadata/errors so the agent retains memory of prior attempts.
 * 3. Filter messages: remove fully-stale parts, trim result sections for stale
 *    definitions.
 * 4. Drop empty messages.
 */
export function filterLatestOnlyMessages(messages: ModelMessage[], workflowState: WorkflowState): ModelMessage[] {
  // Build the set of operator IDs currently in the workflow
  const currentOperatorIds = new Set(workflowState.getAllEnabledOperators().map(op => op.operatorID));

  // --- Step 1: Scan assistant messages, collect tool-call entries in order ---
  const entries: ToolCallEntry[] = [];

  for (let msgIdx = 0; msgIdx < messages.length; msgIdx++) {
    const msg = messages[msgIdx];
    if (msg.role !== "assistant" || !Array.isArray(msg.content)) continue;

    for (let partIdx = 0; partIdx < (msg.content as any[]).length; partIdx++) {
      const part = (msg.content as any[])[partIdx];
      if (part.type !== "tool-call") continue;

      const params = part.args || part.input || {};
      const toolName: string = part.toolName;
      const operatorIds = extractOperatorIds(toolName, params);

      entries.push({
        messageIndex: msgIdx,
        partIndex: partIdx,
        toolCallId: part.toolCallId,
        toolName,
        operatorIds,
      });
    }
  }

  // --- Step 2: Reverse traverse to decide which tool calls to remove ---
  //
  // Definition and execution tools are tracked independently so that
  // executeOperator(a) does not suppress the latest createOrModifyOperator(a).
  // This keeps the operator's code/configuration in context alongside its
  // execution results.
  const DEFINITION_TOOLS = new Set([
    TOOL_NAME_CREATE_OR_MODIFY_OPERATOR,
    TOOL_NAME_ADD_OPERATOR,
    TOOL_NAME_MODIFY_OPERATOR,
  ]);

  const seenDefinition = new Set<string>();  // latest definition per operator
  const seenExecution = new Set<string>();   // latest execution per operator
  const toolCallIdsToRemove = new Set<string>();
  // Stale definition calls: keep the tool-call (code/args) but trim execution
  // result data from the tool-result, preserving metadata and errors so the
  // agent can see what it previously tried and why it failed.
  const toolCallIdsToTrimResult = new Set<string>();

  for (let i = entries.length - 1; i >= 0; i--) {
    const entry = entries[i];
    const { toolName, operatorIds } = entry;

    // No operator IDs → keep unconditionally (getCurrentWorkflow, getOperatorSchema, etc.)
    if (operatorIds.length === 0) {
      continue;
    }

    // Check if ALL referenced operators have been deleted from the workflow
    const allDeleted = operatorIds.every(id => !currentOperatorIds.has(id));
    if (allDeleted) {
      toolCallIdsToRemove.add(entry.toolCallId);
      continue;
    }

    // deleteOperator for an operator still in workflow → stale delete (operator was re-created)
    if (toolName === TOOL_NAME_DELETE_OPERATOR && currentOperatorIds.has(operatorIds[0])) {
      toolCallIdsToRemove.add(entry.toolCallId);
      continue;
    }

    if (DEFINITION_TOOLS.has(toolName)) {
      // Definition tool: check against definition-seen set only
      const anyAlreadySeen = operatorIds.some(id => seenDefinition.has(id));
      if (anyAlreadySeen) {
        // Stale definition: trim result data but keep call + metadata/errors
        toolCallIdsToTrimResult.add(entry.toolCallId);
      } else {
        for (const id of operatorIds) seenDefinition.add(id);
      }
    } else if (toolName === TOOL_NAME_EXECUTE_OPERATOR) {
      // Execution tool: check against execution-seen set only
      const anyAlreadySeen = operatorIds.some(id => seenExecution.has(id));
      if (anyAlreadySeen) {
        toolCallIdsToRemove.add(entry.toolCallId);
      } else {
        for (const id of operatorIds) seenExecution.add(id);
      }
    } else {
      // Other tools (addLink, deleteLink, etc.): check against both sets
      const anyAlreadySeen = operatorIds.some(
        id => seenDefinition.has(id) || seenExecution.has(id)
      );
      if (anyAlreadySeen) {
        toolCallIdsToRemove.add(entry.toolCallId);
      } else {
        for (const id of operatorIds) seenDefinition.add(id);
      }
    }
  }

  if (toolCallIdsToRemove.size === 0 && toolCallIdsToTrimResult.size === 0) {
    console.log(`[LatestOnlyFilter] No tool calls removed or trimmed (${entries.length} total)`);
    return messages;
  }

  // --- Step 3: Filter messages ---
  //
  // - toolCallIdsToRemove: fully remove both tool-call and tool-result parts
  // - toolCallIdsToTrimResult: keep tool-call unchanged, but strip the
  //   "--- Execution Result ---" section from the tool-result while preserving
  //   metadata and error messages (same approach as context-optimization.ts)
  const filtered: ModelMessage[] = [];

  for (const msg of messages) {
    if (msg.role === "assistant" && Array.isArray(msg.content)) {
      const newContent = (msg.content as any[]).filter(part => {
        if (part.type === "tool-call") {
          // Remove fully deleted calls; keep trimmed calls unchanged
          return !toolCallIdsToRemove.has(part.toolCallId);
        }
        return true; // keep text parts
      });

      // Drop message only if it had content before and is now empty
      if (newContent.length > 0) {
        filtered.push({ ...msg, content: newContent });
      }
    } else if (msg.role === "tool" && Array.isArray(msg.content)) {
      let modified = false;
      const newContent = (msg.content as any[])
        .filter(part => {
          if (part.type === "tool-result") {
            return !toolCallIdsToRemove.has(part.toolCallId);
          }
          return true;
        })
        .map(part => {
          if (part.type !== "tool-result" || !toolCallIdsToTrimResult.has(part.toolCallId)) {
            return part;
          }

          // Trim execution result section, keep metadata and errors
          const rawResult = part.result ?? part.output;
          let resultStr: string;
          if (typeof rawResult === "string") {
            resultStr = rawResult;
          } else if (rawResult && typeof rawResult === "object" && rawResult.value !== undefined) {
            resultStr = String(rawResult.value);
          } else {
            resultStr = JSON.stringify(rawResult);
          }

          // Always preserve errors — the agent needs to learn from failures
          if (resultStr.startsWith("[ERROR]")) return part;

          // Find the table header (first line starting with \t) to locate table data boundary
          const lines = resultStr.split("\n");
          const headerIdx = lines.findIndex(l => l.startsWith("\t"));
          if (headerIdx < 0) return part; // no table data to trim

          const trimmedText =
            lines.slice(0, headerIdx).join("\n").trimEnd() +
            "\n(execution result trimmed — superseded by a later version of this operator)";

          modified = true;

          // Build replacement preserving the original field format
          if (part.result !== undefined) {
            return { ...part, result: trimmedText };
          }
          if (typeof rawResult === "object" && rawResult.value !== undefined) {
            return { ...part, output: { ...rawResult, value: trimmedText } };
          }
          return { ...part, output: trimmedText };
        });

      if (newContent.length > 0) {
        filtered.push(modified ? { ...msg, content: newContent } : msg);
      }
    } else {
      // user messages and other message types — keep as-is
      filtered.push(msg);
    }
  }

  console.log(
    `[LatestOnlyFilter] Removed ${toolCallIdsToRemove.size}, trimmed ${toolCallIdsToTrimResult.size} ` +
      `of ${entries.length} tool calls (${currentOperatorIds.size} operators in workflow)`
  );

  return filtered;
}
