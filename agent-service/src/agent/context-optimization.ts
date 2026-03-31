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
 * Context optimization v2: Selective execution result trimming.
 *
 * Instead of replacing the entire message history with a condensed format,
 * this approach keeps the full message history intact (preserving all tool calls,
 * assistant reasoning, and ordering) but selectively trims execution results
 * from tool results that belong to non-frontier operators.
 *
 * This preserves the reasoning trace while saving tokens on bulky execution
 * outputs that are no longer relevant.
 */

import type { ModelMessage } from "ai";
import type { WorkflowState } from "../workflow/workflow-state";
import { AgentMode } from "../types/agent";
import { TOOL_NAME_CREATE_OR_MODIFY_OPERATOR } from "../tools/code-op-tools";
import { TOOL_NAME_EXECUTE_OPERATOR } from "../tools/execution-tools";

// ============================================================================
// Frontier Computation
// ============================================================================

/**
 * Compute frontier operator IDs from the workflow state.
 * Delegates to WorkflowState.getFrontierOperators().
 */
export function computeFrontier(workflowState: WorkflowState, depth: number): string[] {
  return workflowState.getFrontierOperators(depth);
}

/**
 * Compute the BFS depth of each operator from leaf nodes (reverse BFS).
 * Leaves (no outgoing links) have depth 1, their predecessors depth 2, etc.
 */
export function computeOperatorDepths(workflowState: WorkflowState): Map<string, number> {
  const depths = new Map<string, number>();
  const operators = workflowState.getAllOperators();
  const links = workflowState.getAllLinks();

  // Build adjacency: for each operator, find its predecessors (incoming links)
  const predecessors = new Map<string, string[]>();
  for (const op of operators) {
    predecessors.set(op.operatorID, []);
  }
  for (const link of links) {
    const preds = predecessors.get(link.target.operatorID);
    if (preds) {
      preds.push(link.source.operatorID);
    }
  }

  // Find leaves: operators with no outgoing links
  const hasOutgoing = new Set<string>();
  for (const link of links) {
    hasOutgoing.add(link.source.operatorID);
  }

  const queue: string[] = [];
  for (const op of operators) {
    if (!hasOutgoing.has(op.operatorID)) {
      depths.set(op.operatorID, 1);
      queue.push(op.operatorID);
    }
  }

  // BFS backward from leaves
  let idx = 0;
  while (idx < queue.length) {
    const current = queue[idx++];
    const currentDepth = depths.get(current)!;
    for (const pred of predecessors.get(current) || []) {
      if (!depths.has(pred)) {
        depths.set(pred, currentDepth + 1);
        queue.push(pred);
      }
    }
  }

  return depths;
}

// ============================================================================
// Selective Execution Result Trimming
// ============================================================================

const TRIMMED_NOTICE = "(execution result skipped due to the context compaction)";

/**
 * Remove or partially keep the table data from a result string while
 * preserving metadata lines (Data lineage, Input/Output shape, etc.).
 *
 * The table boundary is detected by finding the first line starting with \t
 * (the tab-prefixed header row). Everything before it is metadata; everything
 * from it onward is table data (header, data rows). Column stats appear after
 * the table as a separate vertical section and are not part of the table rows.
 *
 * @param resultStr - The full result string
 * @param charLimit - Max chars to keep from table data rows (0 = fully trim)
 * @returns The original string unchanged if no table data is found.
 */
function trimExecutionResultSection(resultStr: string, charLimit: number): string {
  const lines = resultStr.split("\n");

  // Find the table header (first line starting with \t)
  const headerIdx = lines.findIndex(l => l.startsWith("\t"));
  if (headerIdx < 0) return resultStr;

  const metadataLines = lines.slice(0, headerIdx);
  const before = metadataLines.join("\n").trimEnd();

  const headerLine = lines[headerIdx];
  const afterHeader = lines.slice(headerIdx + 1);

  if (charLimit <= 0) {
    // Fully trim: keep metadata only, drop table data
    return before + "\n" + TRIMMED_NOTICE;
  }

  // Filter empty lines from data rows
  const nonEmptyAfter = afterHeader.filter(l => l.trim() !== "");
  if (nonEmptyAfter.length === 0) {
    return before + "\n" + TRIMMED_NOTICE;
  }

  const dataRows = nonEmptyAfter;
  const totalDataRows = dataRows.length;

  // Allocate half the budget for front rows, half for back rows
  const halfLimit = Math.floor(charLimit / 2);

  // Collect front rows
  let frontSize = 0;
  const frontRows: string[] = [];
  for (const row of dataRows) {
    const rowLen = row.length + 1; // +1 for newline
    if (frontSize + rowLen > halfLimit && frontRows.length > 0) break;
    frontRows.push(row);
    frontSize += rowLen;
  }

  // Collect back rows (scan from end, keep within half budget)
  let backSize = 0;
  const backRows: string[] = [];
  for (let i = dataRows.length - 1; i >= frontRows.length; i--) {
    const rowLen = dataRows[i].length + 1;
    if (backSize + rowLen > halfLimit && backRows.length > 0) break;
    backRows.unshift(dataRows[i]);
    backSize += rowLen;
  }

  const keptCount = frontRows.length + backRows.length;
  const keptRows = [...frontRows, ...backRows];
  const keptResult = [headerLine, ...keptRows].join("\n");

  const notice =
    keptCount < totalDataRows
      ? `\n${keptCount}/${totalDataRows} rows are displayed due to the context compaction`
      : "";

  return before + "\n" + keptResult + notice;
}

/**
 * Trim execution results from non-frontier operators while keeping the full
 * message history intact.
 *
 * For each tool-result in the message history:
 * - Errors (results starting with "[ERROR]") are always preserved.
 * - For execution-related tools (`createOrModifyOperator`, `executeOperator`):
 *   Removes the "--- Execution Result ---" section (the raw table/JSON) while
 *   preserving the "--- Execution Metadata ---" section (shape, dataflow, columns).
 * - All other tool results are kept as-is regardless of frontier status.
 *
 * @param messages - The full message history
 * @param workflowState - Current workflow state
 * @param frontierDepth - BFS depth for frontier computation
 * @param agentMode - Current agent mode (code or general)
 * @returns Modified messages array with trimmed execution results
 */
export function trimNonFrontierResults(
  messages: ModelMessage[],
  workflowState: WorkflowState,
  frontierDepth: number,
  agentMode: AgentMode,
  minimumResultCharLimit: number = 0,
  maxResultCharLimit: number = 0,
  noLogFallback: boolean = false
): ModelMessage[] {
  const useLogFallback = !noLogFallback && maxResultCharLimit > 0 && minimumResultCharLimit >= 0;
  const operatorDepths = useLogFallback ? computeOperatorDepths(workflowState) : undefined;
  const frontierOpIds = computeFrontier(workflowState, frontierDepth);
  const frontierSet = new Set(frontierOpIds);

  // First pass: scan assistant messages to build toolCallId -> { toolName, operatorId } map
  // Note: Vercel AI SDK uses `input` (not `args`) for tool-call parameters
  const toolCallMap = new Map<string, { toolName: string; operatorId?: string }>();
  for (const msg of messages) {
    if (msg.role === "assistant" && Array.isArray(msg.content)) {
      for (const part of msg.content as any[]) {
        if (part.type === "tool-call") {
          const params = part.args || part.input || {};
          toolCallMap.set(part.toolCallId, {
            toolName: part.toolName,
            operatorId: params.operatorId,
          });
        }
      }
    }
  }

  // Second pass: clone and trim tool-result messages for non-frontier operators
  // Note: Vercel AI SDK tool-result uses `output` (not `result`), and the output
  // can be a structured object like { type: "text", value: "..." } or a plain string.
  let trimCount = 0;
  const trimmedMessages: ModelMessage[] = messages.map(msg => {
    if (msg.role !== "tool" || !Array.isArray(msg.content)) {
      return msg;
    }

    let modified = false;
    const newContent = (msg.content as any[]).map(part => {
      if (part.type !== "tool-result") return part;

      const info = toolCallMap.get(part.toolCallId);
      if (!info) return part;

      const { toolName, operatorId } = info;

      if (!operatorId) return part;

      // Determine the char limit for this operator
      let opCharLimit: number;
      if (useLogFallback) {
        // Log-fallback mode: frontier (depth 1) = maxResultCharLimit,
        // depth 2 = half, depth 3 = quarter, etc. Floor is minimumResultCharLimit.
        const depth = operatorDepths!.get(operatorId) ?? 1;
        opCharLimit = Math.max(
          Math.floor(maxResultCharLimit / Math.pow(2, depth - 1)),
          minimumResultCharLimit
        );
      } else {
        // Binary mode: frontier = full (no trimming), non-frontier = minimumResultCharLimit
        if (frontierSet.has(operatorId)) return part;
        opCharLimit = minimumResultCharLimit;
      }

      // Extract the result string from whichever field/format the AI SDK uses
      const rawResult = part.result ?? part.output;
      let resultStr: string;
      if (typeof rawResult === "string") {
        resultStr = rawResult;
      } else if (rawResult && typeof rawResult === "object" && rawResult.value !== undefined) {
        // Structured format: { type: "text", value: "..." }
        resultStr = String(rawResult.value);
      } else {
        resultStr = JSON.stringify(rawResult);
      }

      // Always preserve errors — the agent needs to learn from failures
      if (resultStr.startsWith("[ERROR]")) return part;

      // Helper to build the replacement result in the same format as the original
      const buildReplacement = (newText: string): any => {
        if (part.result !== undefined) {
          // Original uses `result` field
          return { ...part, result: newText };
        }
        // Original uses `output` field
        if (typeof rawResult === "object" && rawResult.value !== undefined) {
          // Structured format: preserve the wrapper
          return { ...part, output: { ...rawResult, value: newText } };
        }
        return { ...part, output: newText };
      };

      if (toolName === TOOL_NAME_CREATE_OR_MODIFY_OPERATOR || toolName === TOOL_NAME_EXECUTE_OPERATOR) {
        const trimmedText = trimExecutionResultSection(resultStr, opCharLimit);
        if (trimmedText !== resultStr) {
          modified = true;
          trimCount++;
          return buildReplacement(trimmedText);
        }
        return part;
      }

      return part;
    });

    return modified ? { ...msg, content: newContent } : msg;
  });

  console.log(
    `[ContextOptimization] Trimmed ${trimCount} execution results ` +
      `(frontier: ${frontierOpIds.length} operators, depth: ${frontierDepth}` +
      (useLogFallback ? `, logFallback: ${maxResultCharLimit}→${minimumResultCharLimit}` : "") +
      `)`
  );

  return trimmedMessages;
}
