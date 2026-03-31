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
 * Tool utilities for Texera Agent Service.
 */

import { DEFAULT_AGENT_SETTINGS } from "../types/agent";

// ============================================================================
// Constants (derived from DEFAULT_AGENT_SETTINGS for consistency)
// ============================================================================

/** Default tool execution timeout in milliseconds */
export const DEFAULT_TOOL_TIMEOUT_MS = DEFAULT_AGENT_SETTINGS.toolTimeoutMs;

/** Default maximum character limit for operator result data */
export const DEFAULT_MAX_OPERATOR_RESULT_CHAR_LIMIT = DEFAULT_AGENT_SETTINGS.maxOperatorResultCharLimit;

/** Default execution timeout for workflow execution in milliseconds */
export const DEFAULT_EXECUTION_TIMEOUT_MS = DEFAULT_AGENT_SETTINGS.executionTimeoutMs;

// ============================================================================
// Result Creators
// ============================================================================

/**
 * Creates a successful tool result as a plain string.
 * All tool results are now plain strings for consistency and token efficiency.
 */
export function createToolResult(message: string): string {
  return message;
}

/**
 * Creates a failed tool result as a plain string with [ERROR] prefix.
 */
export function createErrorResult(error: string): string {
  return `[ERROR] ${error}`;
}

// ============================================================================
// Token Estimation
// ============================================================================

/**
 * Estimates the number of tokens in a JSON-serializable object.
 * Uses a common approximation: tokens ≈ characters / 4
 */
export function estimateTokenCount(data: any): number {
  try {
    const jsonString = JSON.stringify(data);
    return Math.ceil(jsonString.length / 4);
  } catch {
    return 0;
  }
}

// ============================================================================
// Plotly Data Extraction
// ============================================================================

/**
 * Essential Plotly chart data structure (stripped of template/theme data)
 */
export interface EssentialPlotlyData {
  data: any[];
  layout?: {
    title?: any;
    xaxis?: { title?: any };
    yaxis?: { title?: any };
    zaxis?: { title?: any };
    legend?: any;
    annotations?: any[];
  };
}

/**
 * Extracts essential data from a full Plotly JSON object.
 * Removes template/theme data that bloats the JSON without adding analytical value.
 */
export function extractEssentialPlotlyData(fullPlotlyJson: any): EssentialPlotlyData {
  if (!fullPlotlyJson || typeof fullPlotlyJson !== "object") {
    return { data: [] };
  }

  const result: EssentialPlotlyData = {
    data: Array.isArray(fullPlotlyJson.data) ? fullPlotlyJson.data : [],
  };

  if (fullPlotlyJson.layout && typeof fullPlotlyJson.layout === "object") {
    const layout = fullPlotlyJson.layout;
    const essentialLayout: EssentialPlotlyData["layout"] = {};

    if (layout.title !== undefined) {
      essentialLayout.title = layout.title;
    }
    if (layout.xaxis?.title !== undefined) {
      essentialLayout.xaxis = { title: layout.xaxis.title };
    }
    if (layout.yaxis?.title !== undefined) {
      essentialLayout.yaxis = { title: layout.yaxis.title };
    }
    if (layout.zaxis?.title !== undefined) {
      essentialLayout.zaxis = { title: layout.zaxis.title };
    }
    if (layout.legend !== undefined) {
      essentialLayout.legend = layout.legend;
    }
    if (Array.isArray(layout.annotations) && layout.annotations.length > 0) {
      essentialLayout.annotations = layout.annotations;
    }

    if (Object.keys(essentialLayout).length > 0) {
      result.layout = essentialLayout;
    }
  }

  return result;
}

// ============================================================================
// Tool Timeout Wrapper
// ============================================================================

/**
 * Wraps a tool's execute function with timeout protection.
 */
export function withTimeout<TArgs, TResult>(
  execute: (args: TArgs) => Promise<TResult>,
  timeoutMs: number = DEFAULT_TOOL_TIMEOUT_MS
): (args: TArgs) => Promise<TResult | string> {
  return async (args: TArgs) => {
    const timeoutPromise = new Promise<string>((_, reject) => {
      setTimeout(() => {
        reject(new Error("timeout"));
      }, timeoutMs);
    });

    try {
      return await Promise.race([execute(args), timeoutPromise]);
    } catch (error: any) {
      if (error.message === "timeout") {
        const timeoutMinutes = Math.round(timeoutMs / 60000);
        return createErrorResult(`Tool execution timeout - operation took longer than ${timeoutMinutes} minute(s).`);
      }
      throw error;
    }
  };
}

// ============================================================================
// Result Character Filtering
// ============================================================================

/**
 * Filters result rows by character limit.
 */
export function filterByCharLimit<T>(
  rows: readonly T[],
  maxCharLimit: number = DEFAULT_MAX_OPERATOR_RESULT_CHAR_LIMIT
): { limited: T[]; charCount: number; truncated: boolean } {
  const limited: T[] = [];
  let charCount = 0;

  for (const row of rows) {
    const rowChars = estimateCharCount(row);
    if (charCount + rowChars > maxCharLimit) break;
    limited.push(row);
    charCount += rowChars;
  }

  return {
    limited,
    charCount,
    truncated: limited.length < rows.length,
  };
}

/**
 * Estimates the number of characters in a JSON-serializable object.
 */
export function estimateCharCount(data: any): number {
  try {
    const jsonString = JSON.stringify(data);
    return jsonString.length;
  } catch {
    return 0;
  }
}

// ============================================================================
// Workflow Tool Result Formatters
// ============================================================================

/**
 * Formats a link as "sourceId --> targetId".
 */
export function formatLinkDescription(sourceOperatorId: string, targetOperatorId: string): string {
  return `${sourceOperatorId} --> ${targetOperatorId}`;
}

/**
 * Formats the result for addOperator tool.
 * The first line is always a brief one-line summary (used by the action detail filter).
 * Created/deleted links are appended to the first line.
 */
export function formatAddOperatorResult(
  operatorId: string,
  numInputPorts: number,
  numOutputPorts: number,
  createdLinks?: { source: string; target: string }[],
  deletedLinks?: { source: string; target: string }[]
): string {
  let summary = `Added operator ${operatorId}, input ports: ${numInputPorts}, output ports: ${numOutputPorts}`;
  if (deletedLinks && deletedLinks.length > 0) {
    summary += `, deleted links: [${deletedLinks.map(l => formatLinkDescription(l.source, l.target)).join(", ")}]`;
  }
  if (createdLinks && createdLinks.length > 0) {
    summary += `, created links: [${createdLinks.map(l => formatLinkDescription(l.source, l.target)).join(", ")}]`;
  }
  return summary;
}

/**
 * Formats the result for modifyOperator tool.
 * Created/deleted links are appended to the first line.
 */
export function formatModifyOperatorResult(
  operatorId: string,
  createdLinks?: { source: string; target: string }[],
  deletedLinks?: { source: string; target: string }[]
): string {
  let summary = `Operator ${operatorId} modified`;
  if (deletedLinks && deletedLinks.length > 0) {
    summary += `, deleted links: [${deletedLinks.map(l => formatLinkDescription(l.source, l.target)).join(", ")}]`;
  }
  if (createdLinks && createdLinks.length > 0) {
    summary += `, created links: [${createdLinks.map(l => formatLinkDescription(l.source, l.target)).join(", ")}]`;
  }
  return summary;
}

/**
 * Formats the brief summary line for executeOperator tool.
 */
export function formatExecuteOperatorResult(operatorId: string): string {
  return `Executed operator ${operatorId}`;
}

/**
 * Formats an error with operator context.
 */
export function formatOperatorError(operatorId: string, error: string): string {
  return `Error on operator ${operatorId}: ${error}`;
}

/**
 * Formats the result for deleteFromWorkflow tool.
 */
export function formatDeleteResult(deletedOperatorIds: string[], deletedLinkIds: string[]): string {
  const parts: string[] = [];
  if (deletedOperatorIds.length > 0) {
    parts.push(`Deleted operators: ${deletedOperatorIds.join(", ")}`);
  }
  if (deletedLinkIds.length > 0) {
    parts.push(`Deleted links: ${deletedLinkIds.join(", ")}`);
  }
  return parts.length > 0 ? parts.join("\n") : "Nothing deleted";
}
