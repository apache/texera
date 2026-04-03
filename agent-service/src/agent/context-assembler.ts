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
 * Context assembler — builds the model's context from pre-serialized
 * historical interactions and a DAG summary of the current workflow state.
 *
 * Returns: [...historicalInteractions, dagSummaryMessage]
 */

import type { ModelMessage } from "ai";
import type { WorkflowState } from "../workflow/workflow-state";
import type { OperatorPredicate } from "../types/workflow";

/**
 * Build the model context from historical interactions and current workflow state.
 *
 * @param historicalInteractions - Pre-serialized interaction summaries (user-type messages)
 * @param workflowState          - Live workflow state (matches HEAD)
 * @param operatorExecutionResults - Map of operatorId → formatted result text
 * @param useRedact               - If true, strip operator properties from the summary
 *                                  (properties are always shown for operators with execution errors)
 * @returns ModelMessage array: [...historicalInteractions, dagSummary]
 */
export function assembleContext(
  historicalInteractions: ModelMessage[],
  workflowState: WorkflowState,
  operatorExecutionResults: Map<string, string>,
  useRedact: boolean = false
): ModelMessage[] {
  const dagSummary = serializeDag(workflowState, operatorExecutionResults, useRedact);

  const result: ModelMessage[] = [...historicalInteractions];
  if (dagSummary) {
    result.push({ role: "user", content: dagSummary });
  }

  console.log(
    `[ContextAssembler] Built context: ${result.length} messages ` +
      `(${historicalInteractions.length} interactions, ${operatorExecutionResults.size} with results, useRedact: ${useRedact})`
  );

  return result;
}

/**
 * Append a single operator entry to the DAG summary lines.
 */
function appendOperatorEntry(
  lines: string[],
  index: number,
  op: OperatorPredicate,
  execResult: string | undefined,
  useRedact: boolean
): void {
  const summary = op.customDisplayName || op.operatorID;
  const hasError = execResult !== undefined && execResult.includes("[ERROR]");

  // Show properties when redaction is off, or when the operator has an execution error
  const showProperties = !useRedact || hasError;

  lines.push("");
  lines.push(`[${index}] Created ${op.operatorType} Operator: ${op.operatorID}`);
  lines.push(`  Summary: ${summary}`);

  if (showProperties) {
    const props = op.operatorProperties;
    if (props && Object.keys(props).length > 0) {
      lines.push(`  Properties:`);
      for (const [key, value] of Object.entries(props)) {
        if (value !== undefined && value !== null && value !== "") {
          const valueStr = typeof value === "string" ? value : JSON.stringify(value);
          lines.push(`    - ${key}: ${valueStr}`);
        }
      }
    }
  }

  if (execResult) {
    lines.push("  Result:");
    const indented = execResult
      .split("\n")
      .map(l => "  " + l)
      .join("\n");
    lines.push(indented);
  } else {
    lines.push("  Not yet executed.");
  }
}

/**
 * Serialize the workflow into a compact text DAG summary.
 * Uses topological order of operators.
 */
function serializeDag(
  workflowState: WorkflowState,
  operatorExecutionResults: Map<string, string>,
  useRedact: boolean
): string | null {
  const allOperators = workflowState.getAllOperators();
  if (allOperators.length === 0) return null;

  const lines: string[] = ["=== Current Workflow ==="];

  // Build topological ordering for consistent display
  const allLinks = workflowState.getAllLinks();
  const opIds = new Set(allOperators.map(op => op.operatorID));
  const inDegree = new Map<string, number>();
  const children = new Map<string, string[]>();
  for (const id of opIds) {
    inDegree.set(id, 0);
    children.set(id, []);
  }
  for (const link of allLinks) {
    children.get(link.source.operatorID)?.push(link.target.operatorID);
    inDegree.set(link.target.operatorID, (inDegree.get(link.target.operatorID) ?? 0) + 1);
  }
  const queue: string[] = [...opIds].filter(id => (inDegree.get(id) ?? 0) === 0);
  const topoOrder = new Map<string, number>();
  let rank = 0;
  while (queue.length > 0) {
    const node = queue.shift()!;
    topoOrder.set(node, rank++);
    for (const child of children.get(node) ?? []) {
      const newDeg = (inDegree.get(child) ?? 1) - 1;
      inDegree.set(child, newDeg);
      if (newDeg === 0) queue.push(child);
    }
  }

  // Sort operators by topological rank
  const sortedOps = [...allOperators].sort(
    (a, b) => (topoOrder.get(a.operatorID) ?? 0) - (topoOrder.get(b.operatorID) ?? 0)
  );

  let index = 1;
  for (const op of sortedOps) {
    appendOperatorEntry(lines, index, op, operatorExecutionResults.get(op.operatorID), useRedact);
    index++;
  }

  // Append links section in topological order
  if (allLinks.length > 0) {
    const sortedLinks = [...allLinks].sort((a, b) => {
      const srcA = topoOrder.get(a.source.operatorID) ?? 0;
      const srcB = topoOrder.get(b.source.operatorID) ?? 0;
      if (srcA !== srcB) return srcA - srcB;
      return (topoOrder.get(a.target.operatorID) ?? 0) - (topoOrder.get(b.target.operatorID) ?? 0);
    });

    lines.push("");
    lines.push("Links:");
    for (const link of sortedLinks) {
      lines.push(`  ${link.source.operatorID} --> ${link.target.operatorID}`);
    }
  }

  return lines.join("\n");
}
