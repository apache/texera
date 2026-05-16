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

import { generateObject, type LanguageModel } from "ai";
import { z } from "zod";

/**
 * Deferred items from profiler-agent-tool-plan.md: smarter ghost-suggestion
 * materialization. The frontend's static rule-based defaults (first column,
 * 4 workers) are safe but uninformed. These generators ask the LLM for an
 * informed value via schema-constrained structured output (no ReAct loop,
 * no tools — one LLM call, returns JSON matching the Zod schema).
 *
 * Caller policy: any thrown error is a "miss" — the frontend keeps its
 * rule-based default. We do not retry; the agent is enhancement, not
 * load-bearing.
 */

const FILTER_CONDITION_VALUES = [
  "=",
  "!=",
  ">",
  ">=",
  "<",
  "<=",
  "is null",
  "is not null",
  "contains",
  "does not contain",
  "regex",
] as const;

/** The frontend's filter-predicate row shape. */
export const FilterPredicateSchema = z.object({
  attribute: z.string().min(1).describe("Column name from the upstream schema."),
  condition: z.enum(FILTER_CONDITION_VALUES).describe("Filter condition."),
  value: z
    .string()
    .describe(
      "Filter value. Empty string is allowed and required for 'is null' / 'is not null' conditions."
    ),
});

export const FilterPredicatesResponseSchema = z.object({
  predicates: z
    .array(FilterPredicateSchema)
    .min(1)
    .max(5)
    .describe("Ordered list of predicate rows the user will see pre-filled."),
  reasoning: z
    .string()
    .min(1)
    .describe("One-line rationale citing why these columns / conditions are likely useful."),
});

export type FilterPredicate = z.infer<typeof FilterPredicateSchema>;
export type FilterPredicatesResponse = z.infer<typeof FilterPredicatesResponseSchema>;

export interface ProposeFilterPredicateInput {
  upstreamOpId: string;
  downstreamOpId: string;
  /** Output columns of the upstream operator: name + type. Required. */
  upstreamSchema: { attributeName: string; attributeType: string }[];
  /** Operator type of the downstream operator (e.g. "Aggregate"). Optional. */
  downstreamType?: string;
  /** Downstream operator properties for context (e.g. groupBy keys). Optional. */
  downstreamProperties?: Record<string, unknown>;
  /** Optional sample rows for grounding (omit if expensive to fetch). */
  upstreamSamples?: Record<string, unknown>[];
}

const FILTER_PREDICATE_SYSTEM_PROMPT = `You are a data-pipeline assistant that suggests useful Filter predicates for an Apache Texera dataflow.

Given an upstream operator's output schema (and optionally sample rows + the downstream operator's context), propose 1 to 5 filter predicate rows that would plausibly be useful to the user.

Rules:
- Each predicate has: attribute (column name from the upstream schema), condition (one of the enum values), value.
- Use empty string for value when condition is 'is null' or 'is not null'.
- Prefer predicates that reduce data volume meaningfully without being too aggressive.
- If the downstream is an Aggregate/groupBy, prefer predicates on columns that are NOT in the groupBy keys (filtering by group key is usually a no-op for the downstream).
- If sample values are provided, ground your value choices in them (avoid hallucinating unseen values).
- If you cannot pick a useful predicate, return a single "is not null" predicate on the most semantically meaningful non-null-looking column — that's still better than the rule-based default of picking the first column blindly.
- Return AT MOST 5 predicates, ordered by likely usefulness.`;

export async function proposeFilterPredicate(
  model: LanguageModel,
  input: ProposeFilterPredicateInput
): Promise<FilterPredicatesResponse> {
  const schemaLines = input.upstreamSchema
    .map(c => `- ${c.attributeName} (${c.attributeType})`)
    .join("\n");
  const samplesBlock =
    input.upstreamSamples && input.upstreamSamples.length > 0
      ? `Sample rows (up to 5):\n${input.upstreamSamples
          .slice(0, 5)
          .map(r => JSON.stringify(r))
          .join("\n")}`
      : "Sample rows: not available.";
  const downstreamBlock = input.downstreamType
    ? `Downstream operator: ${input.downstreamType}${
        input.downstreamProperties
          ? ` with properties ${JSON.stringify(input.downstreamProperties)}`
          : ""
      }`
    : "Downstream operator: unspecified.";

  const userPrompt = `Upstream operator: ${input.upstreamOpId}
Upstream output schema:
${schemaLines}

${downstreamBlock}

${samplesBlock}

Propose 1 to 5 useful Filter predicates.`;

  const result = await generateObject({
    model,
    system: FILTER_PREDICATE_SYSTEM_PROMPT,
    prompt: userPrompt,
    schema: FilterPredicatesResponseSchema,
    temperature: 0.1,
  });
  return result.object;
}

/** Response shape for proposeWorkerCount. */
export const WorkerCountResponseSchema = z.object({
  workers: z
    .number()
    .int()
    .min(1)
    .max(64)
    .describe("Proposed number of parallel workers for the operator (1..64)."),
  reasoning: z
    .string()
    .min(1)
    .describe(
      "One-line rationale citing runtime, idle ratio, operator type, or data size that informed the choice."
    ),
});

export type WorkerCountResponse = z.infer<typeof WorkerCountResponseSchema>;

export interface ProposeWorkerCountInput {
  operatorId: string;
  operatorType: string;
  currentWorkers: number;
  runtimeMs?: number | null;
  idleRatio?: number | null;
  inputRows?: number | null;
  outputRows?: number | null;
}

const WORKER_COUNT_SYSTEM_PROMPT = `You are a performance-tuning assistant for Apache Texera dataflow operators.

Given an operator's current metrics, propose a worker count (parallelism) that would likely reduce its runtime. Output JSON matching the provided schema.

Rules:
- For Python UDF or other CPU-bound operators with low idle ratio, parallelism scales well — propose 4 to 8 workers depending on runtime.
- For high idle ratio (>0.5), the operator is upstream-bound; parallelism won't help much — propose at most 2.
- For Sort, Aggregate, or other inherently serial / coordinator-heavy operators, conservative (1 to 2).
- Never propose more than 8 unless runtime is extreme (> 30s) AND idle ratio is low (< 0.3).
- The current rule-based default is 4. Beat it when you have signal; otherwise return 4.`;

export async function proposeWorkerCount(
  model: LanguageModel,
  input: ProposeWorkerCountInput
): Promise<WorkerCountResponse> {
  const metricsLines = [
    `Operator id: ${input.operatorId}`,
    `Operator type: ${input.operatorType}`,
    `Current workers: ${input.currentWorkers}`,
    input.runtimeMs != null ? `Runtime: ${input.runtimeMs} ms` : "Runtime: unknown",
    input.idleRatio != null ? `Idle ratio: ${input.idleRatio.toFixed(2)}` : "Idle ratio: unknown",
    input.inputRows != null ? `Input rows: ${input.inputRows}` : "Input rows: unknown",
    input.outputRows != null ? `Output rows: ${input.outputRows}` : "Output rows: unknown",
  ].join("\n");

  const result = await generateObject({
    model,
    system: WORKER_COUNT_SYSTEM_PROMPT,
    prompt: `Propose a worker count for the operator below.\n\n${metricsLines}`,
    schema: WorkerCountResponseSchema,
    temperature: 0.1,
  });
  return result.object;
}
