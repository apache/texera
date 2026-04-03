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
 * Context assembler — builds the model's context as a single structured
 * user message containing completed tasks, the ongoing task, and the
 * current workflow DAG with execution results.
 */

import type { ModelMessage } from "ai";
import type { WorkflowState } from "../workflow/workflow-state";
import type { OperatorPredicate } from "../types/workflow";
import type { ReActStep } from "../types/agent";

/**
 * Build the full model context as a single user message.
 *
 * Layout:
 *   # Completed Tasks
 *   <task status="completed">...</task>
 *   ...
 *   # Ongoing Task
 *   <task status="ongoing">...</task>
 *   (instruction line)
 *   # Current Workflow
 *   <operator ...>...</operator>
 *   <links>...</links>
 *
 * @param visibleSteps  - ReActSteps on the HEAD ancestor path (ordered root→HEAD)
 * @param workflowState - Live workflow state
 * @param operatorExecutionResults - Map of operatorId → formatted result text
 * @param useRedact     - If true, strip operator properties (except for errored operators)
 * @returns Single-element ModelMessage array with the assembled context
 */
export function assembleContext(
  visibleSteps: ReActStep[],
  workflowState: WorkflowState,
  operatorExecutionResults: Map<string, string>,
  useRedact: boolean = false
): ModelMessage[] {
  // Group steps by messageId, preserving insertion order
  const messageIds: string[] = [];
  const stepsByMessage = new Map<string, ReActStep[]>();
  for (const step of visibleSteps) {
    let group = stepsByMessage.get(step.messageId);
    if (!group) {
      group = [];
      stepsByMessage.set(step.messageId, group);
      messageIds.push(step.messageId);
    }
    group.push(step);
  }

  const sections: string[] = [];
  let completedCount = 0;
  let hasOngoing = false;

  // Determine completed vs ongoing: a task is ongoing if none of its steps has isEnd=true
  for (const msgId of messageIds) {
    const steps = stepsByMessage.get(msgId)!;
    // A task is completed when an agent step has isEnd=true (not user steps,
    // which always have isEnd=true as they are single-step messages).
    const isCompleted = steps.some(s => s.role === "agent" && s.isEnd);

    if (isCompleted) {
      if (completedCount === 0) {
        sections.push("# Completed Tasks");
      }
      sections.push(serializeTask(steps, "completed"));
      completedCount++;
    } else {
      hasOngoing = true;
      sections.push("");
      sections.push("# Ongoing Task");
      sections.push(serializeTask(steps, "ongoing"));
      sections.push("");
      sections.push("Above is user's request and the steps you already took. You as an assistant please keep working on solving user's request based on the progress of current workflow.");
    }
  }

  // --- Current Workflow ---
  const dagSection = serializeDag(workflowState, operatorExecutionResults, useRedact);
  if (dagSection) {
    sections.push("");
    sections.push("# Current Workflow");
    sections.push(dagSection);
  }

  const content = sections.join("\n");

  console.log(
    `[ContextAssembler] Built context: ${completedCount} completed tasks, ` +
      `${hasOngoing ? 1 : 0} ongoing, ${operatorExecutionResults.size} operator results, useRedact: ${useRedact}`
  );

  return [{ role: "user", content }];
}

// ============================================================================
// Task Serialization
// ============================================================================

/**
 * Serialize a task (one user message + its assistant steps) into XML-like format.
 */
function serializeTask(steps: ReActStep[], status: "completed" | "ongoing"): string {
  const lines: string[] = [];
  lines.push(`<task status="${status}">`);

  const userStep = steps.find(s => s.role === "user");
  const assistantSteps = steps.filter(s => s.role === "agent");

  // User request
  if (userStep) {
    lines.push(`<user-request>`);
    lines.push(userStep.content);
    lines.push(`</user-request>`);
  }

  // Assistant steps
  for (const step of assistantSteps) {
    lines.push("");
    lines.push(`<assistant-step${step.stepId}>`);
    if (step.content) {
      lines.push(`<thought>${step.content}</thought>`);
    }
    if (step.toolCalls && step.toolCalls.length > 0) {
      for (let i = 0; i < step.toolCalls.length; i++) {
        const tc = step.toolCalls[i];
        const tr = step.toolResults?.[i];
        const isError = tr?.isError;
        const statusAttr = isError ? "failed" : "succeeded";
        const outputStr = tr ? (typeof tr.output === "string" ? tr.output : String(tr.output ?? "")) : "";
        lines.push(`<action tool="${tc.toolName}" status="${statusAttr}">${outputStr}</action>`);
      }
    }
    lines.push(`</assistant-step${step.stepId}>`);
  }

  lines.push(`</task>`);
  return lines.join("\n");
}

// ============================================================================
// DAG Serialization
// ============================================================================

/**
 * Serialize the workflow into XML-like operator entries with links.
 */
function serializeDag(
  workflowState: WorkflowState,
  operatorExecutionResults: Map<string, string>,
  useRedact: boolean
): string | null {
  const allOperators = workflowState.getAllOperators();
  if (allOperators.length === 0) return null;

  const lines: string[] = [];

  // Build topological ordering
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

  const sortedOps = [...allOperators].sort(
    (a, b) => (topoOrder.get(a.operatorID) ?? 0) - (topoOrder.get(b.operatorID) ?? 0)
  );

  for (const op of sortedOps) {
    lines.push(serializeOperator(op, operatorExecutionResults.get(op.operatorID), useRedact));
  }

  // Links section
  if (allLinks.length > 0) {
    const sortedLinks = [...allLinks].sort((a, b) => {
      const srcA = topoOrder.get(a.source.operatorID) ?? 0;
      const srcB = topoOrder.get(b.source.operatorID) ?? 0;
      if (srcA !== srcB) return srcA - srcB;
      return (topoOrder.get(a.target.operatorID) ?? 0) - (topoOrder.get(b.target.operatorID) ?? 0);
    });

    lines.push("");
    lines.push("<links>");
    for (const link of sortedLinks) {
      lines.push(`${link.source.operatorID} --> ${link.target.operatorID}`);
    }
    lines.push("</links>");
  }

  return lines.join("\n");
}

/**
 * Serialize a single operator entry.
 */
function serializeOperator(
  op: OperatorPredicate,
  execResult: string | undefined,
  useRedact: boolean
): string {
  const hasError = execResult !== undefined && execResult.includes("[ERROR]");
  const status = execResult
    ? (hasError ? "failed" : "executed")
    : "not-executed";

  const summary = op.customDisplayName || op.operatorID;
  const showProperties = !useRedact || hasError;

  const lines: string[] = [];
  lines.push(`<operator type="${op.operatorType}" id="${op.operatorID}" status="${status}">`);
  lines.push(`  Summary: ${summary}`);

  if (showProperties) {
    const props = op.operatorProperties;
    if (props && Object.keys(props).length > 0) {
      lines.push(`  Properties:`);
      for (const [key, value] of Object.entries(props)) {
        if (value !== undefined && value !== null && value !== "") {
          const valueStr = typeof value === "string" ? value : JSON.stringify(value);
          lines.push(`    ${key}: ${valueStr}`);
        }
      }
    }
  }

  if (execResult) {
    lines.push(`  Result:`);
    const indented = execResult.split("\n").map(l => "  " + l).join("\n");
    lines.push(indented);
  }

  lines.push(`</operator>`);
  return lines.join("\n");
}
