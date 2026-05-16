/*
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

import { tool } from "ai";
import { z } from "zod";
import { createToolResult } from "./tools-utility";

/**
 * Phase 3 of the profiler-agent-tool plan: structured-proposal channel.
 *
 * `proposeOperatorChange` is a NON-MUTATING tool. It records a proposed
 * operator-property change as a tool call in the chat transcript so the
 * frontend can render an Apply/Reject card next to the agent's message.
 *
 * The actual mutation runs on the frontend when the user clicks Apply — via
 * the existing `WorkflowActionService.setOperatorProperty` — bypassing a
 * round-trip back to the agent. This keeps the confirmation gate UI-side and
 * makes it harder for a hallucinating agent to silently rewrite the workflow.
 */

export const TOOL_NAME_PROPOSE_OPERATOR_CHANGE = "proposeOperatorChange";
export const TOOL_NAME_PROPOSE_OPTIMIZATION_PLAN = "proposeOptimizationPlan";

export interface OperatorChangeProposal {
  kind: "operator_change_proposal";
  operatorId: string;
  propertyChanges: Record<string, unknown>;
  reasoning: string;
  expectedImpact: string;
  firingHints?: string[];
}

/**
 * Phase 4: a multi-step optimization plan — an ordered sequence of related
 * operator-property changes presented as one card with per-step Apply/Reject.
 * Use this when the steps build on each other (e.g. "first restructure, then
 * tune workers"); for independent single-operator suggestions, prefer
 * proposeOperatorChange instead — one card per change is clearer.
 */
export interface OptimizationPlanStep {
  operatorId: string;
  propertyChanges: Record<string, unknown>;
  description: string;
  reasoning: string;
  expectedImpact: string;
}

export interface OptimizationPlanProposal {
  kind: "optimization_plan_proposal";
  planTitle: string;
  planRationale: string;
  firingHints?: string[];
  steps: OptimizationPlanStep[];
}

export function createProposeOperatorChangeTool() {
  return tool({
    description:
      "Surface a structured proposal to change an operator's properties — does NOT apply the change. " +
      "Use this (instead of asking for text confirmation) whenever a profiler hint suggests a concrete, mechanical edit " +
      "(e.g. LOW_PARALLELISM_HOT_OP → increase 'workers'). The frontend will render an Apply / Reject card next to your " +
      "message; do not also call 'modifyOperator' for the same change — the UI handles the mutation. " +
      "Each call must include: the exact operatorId, a propertyChanges object listing ONLY the keys to change " +
      "(merge-style, not full replacement), the reasoning citing the firing hint(s), and the expected impact.",
    inputSchema: z.object({
      operatorId: z
        .string()
        .describe("The exact operatorId targeted by the proposal (not the display name)."),
      propertyChanges: z
        .record(z.string(), z.unknown())
        .describe(
          "Sparse object of operator-property keys → new values. Frontend will merge into the existing properties. " +
            "Do NOT include unchanged keys."
        ),
      reasoning: z
        .string()
        .describe(
          "Why this change — must cite the firing profiler hint(s) (e.g. 'RUNTIME_OUTLIER and LOW_PARALLELISM_HOT_OP on python-udf-1')."
        ),
      expectedImpact: z
        .string()
        .describe(
          "What the user should see after applying (e.g. 'cuts python-udf-1 runtime via more parallelism')."
        ),
      firingHints: z
        .array(z.string())
        .optional()
        .describe("Optional list of ruleIds (e.g. ['RUNTIME_OUTLIER']) that justify this proposal."),
    }),
    execute: async args => {
      const proposal: OperatorChangeProposal = {
        kind: "operator_change_proposal",
        operatorId: args.operatorId,
        propertyChanges: args.propertyChanges,
        reasoning: args.reasoning,
        expectedImpact: args.expectedImpact,
        firingHints: args.firingHints,
      };
      return createToolResult(JSON.stringify(proposal));
    },
  });
}

export function createProposeOptimizationPlanTool() {
  return tool({
    description:
      "Surface a structured multi-step optimization plan — does NOT apply any of the steps. " +
      "Use this (instead of multiple proposeOperatorChange calls) when the suggested changes are RELATED and ORDERED " +
      "(e.g. 'first push the Filter upstream, then bump the UDF workers'). For independent single-operator suggestions, " +
      "prefer proposeOperatorChange — one card per change is clearer. The frontend renders the plan as one card with " +
      "per-step Apply / Reject buttons plus an 'Apply All' button.",
    inputSchema: z.object({
      planTitle: z
        .string()
        .min(1)
        .describe("Short title for the plan (e.g. 'Optimize the Python UDF bottleneck')."),
      planRationale: z
        .string()
        .min(1)
        .describe(
          "Plan-level rationale — why these steps together. Cite firing hint(s) (e.g. 'RUNTIME_OUTLIER and LOW_PARALLELISM_HOT_OP on python-udf-1')."
        ),
      firingHints: z
        .array(z.string())
        .optional()
        .describe("Optional list of ruleIds justifying the plan as a whole."),
      steps: z
        .array(
          z.object({
            operatorId: z.string().describe("Exact operatorId targeted by this step."),
            propertyChanges: z
              .record(z.string(), z.unknown())
              .describe(
                "Sparse property update for this step (merge-style; only changed keys)."
              ),
            description: z
              .string()
              .min(1)
              .describe("One-line description of what this step does."),
            reasoning: z
              .string()
              .min(1)
              .describe("Why this specific step is in the plan."),
            expectedImpact: z
              .string()
              .min(1)
              .describe("What the user should see after applying this step."),
          })
        )
        .min(2)
        .max(10)
        .describe(
          "Ordered list of steps. Must contain at least 2 entries — a single-step 'plan' should be a proposeOperatorChange call instead. Maximum 10 steps to avoid overwhelming the user."
        ),
    }),
    execute: async args => {
      const plan: OptimizationPlanProposal = {
        kind: "optimization_plan_proposal",
        planTitle: args.planTitle,
        planRationale: args.planRationale,
        firingHints: args.firingHints,
        steps: args.steps,
      };
      return createToolResult(JSON.stringify(plan));
    },
  });
}

export function createProposalTools(): Record<string, any> {
  return {
    [TOOL_NAME_PROPOSE_OPERATOR_CHANGE]: createProposeOperatorChangeTool(),
    [TOOL_NAME_PROPOSE_OPTIMIZATION_PLAN]: createProposeOptimizationPlanTool(),
  };
}
