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

import { ReActStep } from "./agent-types";

/**
 * Phase 3 of the profiler-agent-tool plan: structured-proposal channel.
 *
 * The agent surfaces proposed operator-property changes via a non-mutating
 * `proposeOperatorChange` tool call rather than text patterns. This pure
 * module extracts those proposals from a ReActStep so the chat component can
 * render Apply / Reject UI, and applies them via shallow merge when the user
 * accepts.
 *
 * The tool name string is the cross-project contract with the agent-service —
 * see `agent-service/src/agent/tools/proposal-tools.ts`.
 */

export const PROPOSE_OPERATOR_CHANGE_TOOL_NAME = "proposeOperatorChange";
export const PROPOSE_OPTIMIZATION_PLAN_TOOL_NAME = "proposeOptimizationPlan";

export interface OperatorChangeProposal {
  /** Stable id (mirrors the ai-sdk toolCallId) used to track per-proposal state in the UI. */
  toolCallId: string;
  operatorId: string;
  propertyChanges: Record<string, unknown>;
  reasoning: string;
  expectedImpact: string;
  firingHints?: string[];
}

/**
 * Phase 4: a multi-step optimization plan surfaced via the `proposeOptimizationPlan`
 * tool. Steps are ordered; each step has its own UI state (pending/applied/...)
 * tracked under a synthetic stepId of `${toolCallId}::${index}`. The plan as a
 * whole is identified by `toolCallId` (mirrors the AI-SDK tool call id).
 */
export interface OptimizationPlanStep {
  /** Synthetic stable id: `${planToolCallId}::${index}` — used as state key. */
  stepId: string;
  operatorId: string;
  propertyChanges: Record<string, unknown>;
  description: string;
  reasoning: string;
  expectedImpact: string;
}

export interface OptimizationPlanProposal {
  toolCallId: string;
  planTitle: string;
  planRationale: string;
  firingHints?: string[];
  steps: OptimizationPlanStep[];
}

/**
 * Pulls every well-formed `proposeOperatorChange` call out of a single agent
 * step. Malformed calls (wrong tool name, missing required field, wrong types)
 * are silently skipped — the agent occasionally hallucinates partial args, and
 * we never want a bad proposal to crash chat rendering.
 */
export function extractProposals(step: ReActStep): OperatorChangeProposal[] {
  if (step.role !== "agent" || !step.toolCalls || step.toolCalls.length === 0) return [];
  const out: OperatorChangeProposal[] = [];
  for (const tc of step.toolCalls) {
    const proposal = parseProposalFromToolCall(tc);
    if (proposal) out.push(proposal);
  }
  return out;
}

/**
 * Pulls every well-formed `proposeOptimizationPlan` call out of a single agent
 * step. A plan with fewer than 2 valid steps is dropped — that's a malformed
 * plan per the agent-side schema, and a single-step plan should have been a
 * `proposeOperatorChange` instead.
 */
export function extractPlans(step: ReActStep): OptimizationPlanProposal[] {
  if (step.role !== "agent" || !step.toolCalls || step.toolCalls.length === 0) return [];
  const out: OptimizationPlanProposal[] = [];
  for (const tc of step.toolCalls) {
    const plan = parsePlanFromToolCall(tc);
    if (plan) out.push(plan);
  }
  return out;
}

function parsePlanFromToolCall(tc: any): OptimizationPlanProposal | undefined {
  if (!tc || tc.toolName !== PROPOSE_OPTIMIZATION_PLAN_TOOL_NAME) return undefined;
  const toolCallId = typeof tc.toolCallId === "string" ? tc.toolCallId : undefined;
  const input = tc.input;
  if (!toolCallId || !input || typeof input !== "object") return undefined;
  const { planTitle, planRationale, firingHints, steps } = input as Record<string, unknown>;
  if (typeof planTitle !== "string" || planTitle.length === 0) return undefined;
  if (typeof planRationale !== "string" || planRationale.length === 0) return undefined;
  if (!Array.isArray(steps) || steps.length === 0) return undefined;

  const validSteps: OptimizationPlanStep[] = [];
  for (let i = 0; i < steps.length; i++) {
    const parsed = parsePlanStep(steps[i], toolCallId, i);
    if (parsed) validSteps.push(parsed);
  }
  // Per the agent-side schema, a plan must have ≥2 valid steps. We re-enforce
  // here in case the agent returned fewer well-formed steps than declared.
  if (validSteps.length < 2) return undefined;

  return {
    toolCallId,
    planTitle,
    planRationale,
    firingHints:
      Array.isArray(firingHints) && firingHints.every(h => typeof h === "string")
        ? (firingHints as string[])
        : undefined,
    steps: validSteps,
  };
}

function parsePlanStep(raw: unknown, planToolCallId: string, index: number): OptimizationPlanStep | undefined {
  if (!raw || typeof raw !== "object") return undefined;
  const { operatorId, propertyChanges, description, reasoning, expectedImpact } = raw as Record<
    string,
    unknown
  >;
  if (typeof operatorId !== "string" || operatorId.length === 0) return undefined;
  if (!propertyChanges || typeof propertyChanges !== "object" || Array.isArray(propertyChanges)) {
    return undefined;
  }
  if (typeof description !== "string" || description.length === 0) return undefined;
  if (typeof reasoning !== "string" || reasoning.length === 0) return undefined;
  if (typeof expectedImpact !== "string" || expectedImpact.length === 0) return undefined;
  return {
    stepId: `${planToolCallId}::${index}`,
    operatorId,
    propertyChanges: propertyChanges as Record<string, unknown>,
    description,
    reasoning,
    expectedImpact,
  };
}

function parseProposalFromToolCall(tc: any): OperatorChangeProposal | undefined {
  if (!tc || tc.toolName !== PROPOSE_OPERATOR_CHANGE_TOOL_NAME) return undefined;
  const toolCallId = typeof tc.toolCallId === "string" ? tc.toolCallId : undefined;
  const input = tc.input;
  if (!toolCallId || !input || typeof input !== "object") return undefined;
  const { operatorId, propertyChanges, reasoning, expectedImpact, firingHints } = input as Record<
    string,
    unknown
  >;
  if (typeof operatorId !== "string" || operatorId.length === 0) return undefined;
  if (!propertyChanges || typeof propertyChanges !== "object" || Array.isArray(propertyChanges)) {
    return undefined;
  }
  if (typeof reasoning !== "string" || typeof expectedImpact !== "string") return undefined;
  return {
    toolCallId,
    operatorId,
    propertyChanges: propertyChanges as Record<string, unknown>,
    reasoning,
    expectedImpact,
    firingHints:
      Array.isArray(firingHints) && firingHints.every(h => typeof h === "string")
        ? (firingHints as string[])
        : undefined,
  };
}

/**
 * Shallow-merge a proposal's changes into the operator's current properties.
 * Used when the user clicks Apply — `WorkflowActionService.setOperatorProperty`
 * replaces the whole property object, so we must include all unchanged keys.
 */
export function mergeProposalIntoProperties(
  existing: Readonly<Record<string, unknown>> | undefined,
  changes: Readonly<Record<string, unknown>>
): Record<string, unknown> {
  return { ...(existing ?? {}), ...changes };
}

export type ProposalState = "pending" | "applied" | "rejected" | "missing-operator" | "failed";

/**
 * Helper: produce a one-line summary of the property changes for the proposal
 * card header. Keeps things grep-friendly for review.
 */
export function summarizePropertyChanges(changes: Readonly<Record<string, unknown>>): string {
  const entries = Object.entries(changes);
  if (entries.length === 0) return "(no changes)";
  return entries
    .map(([k, v]) => `${k} → ${formatValue(v)}`)
    .join(", ");
}

function formatValue(v: unknown): string {
  if (v === null) return "null";
  if (typeof v === "string") return JSON.stringify(v);
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  return JSON.stringify(v);
}
