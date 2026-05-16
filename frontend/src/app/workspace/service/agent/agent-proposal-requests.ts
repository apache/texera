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
 * Pure helpers for the on-demand proposal HTTP endpoints — deferred items from
 * the profiler-agent-tool plan. Used to make smarter defaults when the user
 * materializes a ghost suggestion (Filter predicate, bump-workers target).
 *
 * Architecture: AgentService POSTs to /api/proposals/* and the response is
 * validated here. Any malformed shape → return undefined so the caller's
 * rule-based fallback kicks in (the agent is enhancement, not load-bearing).
 */

/** Conditions accepted by the Filter operator's predicate rows. Mirrors the backend Zod enum. */
export const FILTER_CONDITIONS = [
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

export type FilterCondition = (typeof FILTER_CONDITIONS)[number];

export interface FilterPredicateRow {
  attribute: string;
  condition: FilterCondition;
  value: string;
}

export interface FilterPredicatesProposal {
  predicates: FilterPredicateRow[];
  reasoning: string;
}

export interface WorkerCountProposal {
  workers: number;
  reasoning: string;
}

/**
 * Validate a raw response body from /api/proposals/filter-predicate. Returns
 * undefined on any malformed shape (missing fields, wrong types, empty array,
 * unknown condition, etc.) so the caller falls back to the rule-based default.
 */
export function parseFilterPredicatesResponse(raw: unknown): FilterPredicatesProposal | undefined {
  if (!raw || typeof raw !== "object") return undefined;
  const { predicates, reasoning } = raw as Record<string, unknown>;
  if (!Array.isArray(predicates) || predicates.length === 0 || predicates.length > 5) return undefined;
  if (typeof reasoning !== "string" || reasoning.length === 0) return undefined;
  const valid: FilterPredicateRow[] = [];
  for (const p of predicates) {
    const row = parsePredicateRow(p);
    if (!row) return undefined; // any bad row poisons the whole proposal
    valid.push(row);
  }
  return { predicates: valid, reasoning };
}

function parsePredicateRow(raw: unknown): FilterPredicateRow | undefined {
  if (!raw || typeof raw !== "object") return undefined;
  const { attribute, condition, value } = raw as Record<string, unknown>;
  if (typeof attribute !== "string" || attribute.length === 0) return undefined;
  if (typeof condition !== "string") return undefined;
  if (!(FILTER_CONDITIONS as readonly string[]).includes(condition)) return undefined;
  // The backend allows empty value only when condition is is null / is not null.
  // We don't enforce that on parse — the user can edit it, and being strict
  // here would discard otherwise-usable proposals.
  if (typeof value !== "string") return undefined;
  return { attribute, condition: condition as FilterCondition, value };
}

/**
 * Validate a raw response body from /api/proposals/worker-count. Returns
 * undefined on any malformed shape.
 */
export function parseWorkerCountResponse(raw: unknown): WorkerCountProposal | undefined {
  if (!raw || typeof raw !== "object") return undefined;
  const { workers, reasoning } = raw as Record<string, unknown>;
  if (typeof workers !== "number" || !Number.isInteger(workers) || workers < 1 || workers > 64) {
    return undefined;
  }
  if (typeof reasoning !== "string" || reasoning.length === 0) return undefined;
  return { workers, reasoning };
}

/** Request body for POST /api/proposals/filter-predicate. */
export interface FilterPredicateRequest {
  upstreamOpId: string;
  downstreamOpId: string;
  upstreamSchema: { attributeName: string; attributeType: string }[];
  downstreamType?: string;
  downstreamProperties?: Record<string, unknown>;
  upstreamSamples?: Record<string, unknown>[];
  modelType?: string;
}

/** Request body for POST /api/proposals/worker-count. */
export interface WorkerCountRequest {
  operatorId: string;
  operatorType: string;
  currentWorkers: number;
  runtimeMs?: number | null;
  idleRatio?: number | null;
  inputRows?: number | null;
  outputRows?: number | null;
  modelType?: string;
}
