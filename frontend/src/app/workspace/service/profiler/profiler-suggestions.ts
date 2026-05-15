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
 * Pure suggestion engine. Converts the same `HintContext` used by `profiler-hints.ts`
 * into a list of *structural* canvas suggestions (initially: "insert a Filter on this edge").
 *
 * Stays dependency-free (no Angular, no RxJS) so it's fully unit-testable.
 * Re-derives the same thresholds as `profiler-hints.ts` rather than parsing hint
 * messages — keeps the modules independent and the math co-located with rules.
 */

import type { HintContext } from "./profiler-hints";

export type SuggestionId = string;
export type SuggestionType = "INSERT_FILTER" | "BUMP_WORKERS";
export type SuggestionReason =
  | "SCAN_FULL_TABLE_NO_FILTER"
  | "UPSTREAM_OVERPRODUCTION"
  | "JOIN_HIGH_FANIN_LOW_FANOUT"
  | "LOW_PARALLELISM_HOT_OP";

export interface InsertFilterSuggestion {
  readonly id: SuggestionId;
  readonly type: "INSERT_FILTER";
  readonly upstreamOpId: string;
  readonly downstreamOpId: string;
  readonly reasonRuleId: SuggestionReason;
  /** Human-readable explanation, mirrors the hint message for reuse in tooltips. */
  readonly reasonMessage: string;
}

/**
 * Proposes increasing the worker count on a hot single-worker operator.
 * Mirrors the LOW_PARALLELISM_HOT_OP hint rule: fires when an operator's
 * normalized score is at/above the hot threshold AND it's running with <= 1 worker.
 */
export interface BumpWorkersSuggestion {
  readonly id: SuggestionId;
  readonly type: "BUMP_WORKERS";
  readonly operatorId: string;
  readonly currentWorkers: number;
  readonly proposedWorkers: number;
  readonly reasonRuleId: "LOW_PARALLELISM_HOT_OP";
  readonly reasonMessage: string;
}

export type Suggestion = InsertFilterSuggestion | BumpWorkersSuggestion;

/**
 * Default target worker count for the bump-workers suggestion. Picked to be a
 * meaningful jump (4× over the current 1-worker state) but conservative enough
 * to not over-allocate. Users can adjust in the property panel after clicking.
 */
export const BUMP_WORKERS_TARGET = 4;

// Thresholds — kept in sync with `profiler-hints.ts`. Duplicated rather than imported
// so this module remains independent of hint internals; if a threshold changes,
// update it in both places. (Tests guard the parity.)
const JOIN_TYPE_PATTERN = /join/i;
const FILTER_TYPE_PATTERN = /filter/i;
const SCAN_TYPE_PATTERN = /(scan|source)/i;
const JOIN_LOW_FANOUT_RATIO = 0.05;
const UPSTREAM_OVERPRODUCE_RATIO = 10;
const SCAN_LARGE_OUTPUT = 1_000_000;

/**
 * Computes all structural suggestions for the given context, deduplicated by
 * `(upstreamOpId, downstreamOpId)` edge and filtered to exclude dismissed ids.
 *
 * Order is deterministic (sorted by suggestion id) so the canvas rendering is stable.
 */
export function computeSuggestions(
  ctx: HintContext,
  dismissed: ReadonlySet<SuggestionId> = new Set()
): readonly Suggestion[] {
  const out: Map<string, Suggestion> = new Map();
  const opIds = Object.keys(ctx.stats);

  for (const opId of opIds) {
    pushIfDefined(out, scanNoFilterSuggestion(opId, ctx));
    pushIfDefined(out, upstreamOverproductionSuggestion(opId, ctx));
    pushIfDefined(out, joinHighFaninLowFanoutSuggestion(opId, ctx));
    pushIfDefined(out, bumpWorkersSuggestion(opId, ctx));
  }

  const result: Suggestion[] = [];
  for (const s of out.values()) {
    if (!dismissed.has(s.id)) result.push(s);
  }
  result.sort((a, b) => a.id.localeCompare(b.id));
  return result;
}

/** Build a stable suggestion id for an edge. */
export function edgeSuggestionId(upstreamOpId: string, downstreamOpId: string): SuggestionId {
  return `INSERT_FILTER:${upstreamOpId}->${downstreamOpId}`;
}

/** Build a stable suggestion id for an operator-attached bump-workers ghost. */
export function bumpWorkersSuggestionId(operatorId: string): SuggestionId {
  return `BUMP_WORKERS:${operatorId}`;
}

function pushIfDefined(out: Map<string, Suggestion>, s: Suggestion | undefined): void {
  // First reason wins for a given edge — keeps output deterministic and avoids
  // duplicate ghosts on the same edge.
  if (s && !out.has(s.id)) out.set(s.id, s);
}

/**
 * SCAN_FULL_TABLE_NO_FILTER → ghost between scan and its first downstream op.
 * Only fires when the scan emits a lot of rows AND its immediate downstream is not
 * already a Filter (otherwise the hint wouldn't fire either).
 */
function scanNoFilterSuggestion(opId: string, ctx: HintContext): Suggestion | undefined {
  const s = ctx.stats[opId];
  if (!s) return undefined;
  const type = ctx.operatorType(opId);
  if (!type || !SCAN_TYPE_PATTERN.test(type)) return undefined;
  const out = s.aggregatedOutputRowCount ?? 0;
  if (out <= SCAN_LARGE_OUTPUT) return undefined;

  const downstream = ctx.downstreamOps(opId);
  if (downstream.length === 0) return undefined;
  // Find a non-filter downstream to insert before.
  const target = downstream.find(id => {
    const t = ctx.operatorType(id);
    return !t || !FILTER_TYPE_PATTERN.test(t);
  });
  if (!target) return undefined;

  return {
    id: edgeSuggestionId(opId, target),
    type: "INSERT_FILTER",
    upstreamOpId: opId,
    downstreamOpId: target,
    reasonRuleId: "SCAN_FULL_TABLE_NO_FILTER",
    reasonMessage: `Scan emits ${out.toLocaleString()} rows with no filter downstream. Insert a Filter to reduce data volume early.`,
  };
}

/**
 * UPSTREAM_OVERPRODUCTION → ghost between the over-producing upstream and the
 * keeps-little downstream. The hint fires *on* the downstream; we look at its
 * upstreams to find the offender.
 */
function upstreamOverproductionSuggestion(opId: string, ctx: HintContext): Suggestion | undefined {
  const myStats = ctx.stats[opId];
  if (!myStats) return undefined;
  const myInputs = myStats.aggregatedInputRowCount ?? 0;
  if (myInputs <= 0) return undefined;
  for (const upstream of ctx.upstreamOps(opId)) {
    const upStats = ctx.stats[upstream];
    if (!upStats) continue;
    const upOut = upStats.aggregatedOutputRowCount ?? 0;
    if (upOut > myInputs * UPSTREAM_OVERPRODUCE_RATIO) {
      return {
        id: edgeSuggestionId(upstream, opId),
        type: "INSERT_FILTER",
        upstreamOpId: upstream,
        downstreamOpId: opId,
        reasonRuleId: "UPSTREAM_OVERPRODUCTION",
        reasonMessage: `'${ctx.displayName(upstream)}' produces ${upOut.toLocaleString()} rows but '${ctx.displayName(opId)}' keeps only ${myInputs.toLocaleString()}. Insert a Filter on this edge to push the predicate upstream.`,
      };
    }
  }
  return undefined;
}

/**
 * JOIN_HIGH_FANIN_LOW_FANOUT → ghost upstream of the join, on the edge from the
 * input that contributed the most rows. The intuition: filter the biggest side
 * before the join to reduce shuffle.
 */
/**
 * LOW_PARALLELISM_HOT_OP → operator-attached "bump workers" suggestion.
 * Fires when the operator's normalized score is at/above the hot threshold
 * AND it's running with <= 1 worker. Mirrors the existing hint rule's
 * conditions so the canvas suggestion stays consistent with the side panel.
 */
function bumpWorkersSuggestion(opId: string, ctx: HintContext): Suggestion | undefined {
  const s = ctx.stats[opId];
  if (!s) return undefined;
  const score = ctx.scores[opId] ?? 0;
  if (score < ctx.hotThreshold) return undefined;
  const currentWorkers = s.numWorkers ?? 1;
  if (currentWorkers > 1) return undefined;
  return {
    id: bumpWorkersSuggestionId(opId),
    type: "BUMP_WORKERS",
    operatorId: opId,
    currentWorkers,
    proposedWorkers: BUMP_WORKERS_TARGET,
    reasonRuleId: "LOW_PARALLELISM_HOT_OP",
    reasonMessage: `Hot operator '${ctx.displayName(opId)}' is running with ${currentWorkers} worker. Increasing to ${BUMP_WORKERS_TARGET} workers may improve runtime proportionally.`,
  };
}

function joinHighFaninLowFanoutSuggestion(opId: string, ctx: HintContext): Suggestion | undefined {
  const s = ctx.stats[opId];
  if (!s) return undefined;
  const type = ctx.operatorType(opId);
  if (!type || !JOIN_TYPE_PATTERN.test(type)) return undefined;
  const inp = s.aggregatedInputRowCount ?? 0;
  const out = s.aggregatedOutputRowCount ?? 0;
  if (inp <= 0) return undefined;
  if (out / inp >= JOIN_LOW_FANOUT_RATIO) return undefined;

  // Pick the upstream with the most output rows — the "fat" side.
  const upstreams = ctx.upstreamOps(opId);
  if (upstreams.length === 0) return undefined;
  let fattest: string | undefined;
  let fattestOut = -1;
  for (const up of upstreams) {
    const upStats = ctx.stats[up];
    if (!upStats) continue;
    const upOut = upStats.aggregatedOutputRowCount ?? 0;
    if (upOut > fattestOut) {
      fattestOut = upOut;
      fattest = up;
    }
  }
  if (!fattest) return undefined;

  return {
    id: edgeSuggestionId(fattest, opId),
    type: "INSERT_FILTER",
    upstreamOpId: fattest,
    downstreamOpId: opId,
    reasonRuleId: "JOIN_HIGH_FANIN_LOW_FANOUT",
    reasonMessage: `Join '${ctx.displayName(opId)}' keeps <${Math.round(JOIN_LOW_FANOUT_RATIO * 100)}% of its input. Insert a Filter before '${ctx.displayName(opId)}' to reduce shuffle.`,
  };
}
