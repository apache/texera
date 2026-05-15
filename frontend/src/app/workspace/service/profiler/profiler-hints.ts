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
 * Pure rule engine that converts the latest profiler stats snapshot into a list of
 * human-readable optimization hints for an individual operator.
 *
 * Kept dependency-free (no Angular, no RxJS) so that:
 *   1. Each rule can be unit-tested with a synthetic `HintContext`.
 *   2. Callers (currently the side panel) can invoke it synchronously on demand.
 */

import { OperatorState, OperatorStatistics } from "../../types/execute-workflow.interface";

export type HintRuleId =
  | "JOIN_HIGH_FANIN_LOW_FANOUT"
  | "UPSTREAM_OVERPRODUCTION"
  | "RUNTIME_OUTLIER"
  | "LOW_PARALLELISM_HOT_OP"
  | "IDLE_HEAVY"
  | "SCAN_FULL_TABLE_NO_FILTER";

export type HintSeverity = "info" | "warning";

export interface Hint {
  readonly ruleId: HintRuleId;
  readonly severity: HintSeverity;
  readonly message: string;
}

export interface HintContext {
  /** Latest per-operator stats snapshot, keyed by operator id. */
  readonly stats: Readonly<Record<string, OperatorStatistics>>;
  /** Normalized scores in [0,1] from ProfilerService, keyed by operator id. */
  readonly scores: Readonly<Record<string, number>>;
  /** Threshold (in [0,1]) at or above which an operator counts as "hot". */
  readonly hotThreshold: number;
  /** Returns the operator's static type (e.g. "HashJoin", "CSVScan"), or undefined if unknown. */
  readonly operatorType: (opId: string) => string | undefined;
  /**
   * Returns a human-readable label for the operator (customDisplayName, falling back to
   * operatorType, falling back to the raw id). Used in hint messages so end users don't
   * see internal ids like "HashJoin-operator-abc123ef".
   */
  readonly displayName: (opId: string) => string;
  /** Operator ids that feed directly into the given op. */
  readonly upstreamOps: (opId: string) => readonly string[];
  /** Operator ids that the given op feeds directly into. */
  readonly downstreamOps: (opId: string) => readonly string[];
}

const JOIN_TYPE_PATTERN = /join/i;
const FILTER_TYPE_PATTERN = /filter/i;
const SCAN_TYPE_PATTERN = /(scan|source)/i;

const JOIN_LOW_FANOUT_RATIO = 0.05;
const UPSTREAM_OVERPRODUCE_RATIO = 10;
const RUNTIME_OUTLIER_FACTOR = 3;
const IDLE_HEAVY_RATIO = 0.7;
const SCAN_LARGE_OUTPUT = 1_000_000;

/**
 * Computes all applicable hints for a single operator given a context snapshot.
 * Returns hints in stable order (rule id alphabetical) so the UI is deterministic.
 */
export function computeHintsForOperator(opId: string, ctx: HintContext): readonly Hint[] {
  const stats = ctx.stats[opId];
  if (!stats) return [];

  const hints: Hint[] = [];
  const type = ctx.operatorType(opId);

  pushIfDefined(hints, joinHighFaninLowFanoutRule(opId, stats, type));
  pushIfDefined(hints, upstreamOverproductionRule(opId, ctx));
  pushIfDefined(hints, runtimeOutlierRule(opId, ctx));
  pushIfDefined(hints, lowParallelismHotOpRule(opId, stats, ctx));
  pushIfDefined(hints, idleHeavyRule(stats));
  pushIfDefined(hints, scanFullTableNoFilterRule(opId, stats, ctx, type));

  return hints.sort((a, b) => a.ruleId.localeCompare(b.ruleId));
}

function pushIfDefined(hints: Hint[], hint: Hint | undefined): void {
  if (hint) hints.push(hint);
}

function joinHighFaninLowFanoutRule(
  _opId: string,
  s: OperatorStatistics,
  type: string | undefined
): Hint | undefined {
  if (!type || !JOIN_TYPE_PATTERN.test(type)) return undefined;
  const inp = s.aggregatedInputRowCount ?? 0;
  const out = s.aggregatedOutputRowCount ?? 0;
  if (inp <= 0) return undefined;
  if (out / inp >= JOIN_LOW_FANOUT_RATIO) return undefined;
  return {
    ruleId: "JOIN_HIGH_FANIN_LOW_FANOUT",
    severity: "warning",
    message: `Join emits <${Math.round(JOIN_LOW_FANOUT_RATIO * 100)}% of its input (${out.toLocaleString()} of ${inp.toLocaleString()} rows). Consider filtering upstream to reduce shuffle.`,
  };
}

function upstreamOverproductionRule(opId: string, ctx: HintContext): Hint | undefined {
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
        ruleId: "UPSTREAM_OVERPRODUCTION",
        severity: "warning",
        message: `Upstream '${ctx.displayName(upstream)}' produces ${upOut.toLocaleString()} rows but '${ctx.displayName(opId)}' keeps only ${myInputs.toLocaleString()}. Push a filter upstream.`,
      };
    }
  }
  return undefined;
}

function runtimeOutlierRule(opId: string, ctx: HintContext): Hint | undefined {
  const myStats = ctx.stats[opId];
  const myTime = myStats?.aggregatedDataProcessingTime ?? 0;
  if (myTime <= 0) return undefined;

  const peerTimes: number[] = [];
  for (const id of Object.keys(ctx.stats)) {
    const t = ctx.stats[id].aggregatedDataProcessingTime ?? 0;
    if (t > 0) peerTimes.push(t);
  }
  if (peerTimes.length < 2) return undefined;
  const median = computeMedian(peerTimes);
  if (median <= 0) return undefined;
  if (myTime <= RUNTIME_OUTLIER_FACTOR * median) return undefined;

  return {
    ruleId: "RUNTIME_OUTLIER",
    severity: "warning",
    message: `Runtime is ${(myTime / median).toFixed(1)}× the median across operators — likely the workflow bottleneck.`,
  };
}

function lowParallelismHotOpRule(opId: string, s: OperatorStatistics, ctx: HintContext): Hint | undefined {
  const score = ctx.scores[opId] ?? 0;
  if (score < ctx.hotThreshold) return undefined;
  const workers = s.numWorkers ?? 1;
  if (workers > 1) return undefined;
  return {
    ruleId: "LOW_PARALLELISM_HOT_OP",
    severity: "info",
    message: `Hot operator is running with ${workers} worker. Increasing parallelism may improve runtime.`,
  };
}

function idleHeavyRule(s: OperatorStatistics): Hint | undefined {
  if (s.operatorState !== OperatorState.Running) return undefined;
  const data = s.aggregatedDataProcessingTime ?? 0;
  const ctrl = s.aggregatedControlProcessingTime ?? 0;
  const idle = s.aggregatedIdleTime ?? 0;
  const total = data + ctrl + idle;
  if (total <= 0) return undefined;
  const ratio = idle / total;
  if (ratio <= IDLE_HEAVY_RATIO) return undefined;
  return {
    ruleId: "IDLE_HEAVY",
    severity: "info",
    message: `Operator is idle ${Math.round(ratio * 100)}% of the time — the bottleneck is likely upstream.`,
  };
}

function scanFullTableNoFilterRule(
  opId: string,
  s: OperatorStatistics,
  ctx: HintContext,
  type: string | undefined
): Hint | undefined {
  if (!type || !SCAN_TYPE_PATTERN.test(type)) return undefined;
  const out = s.aggregatedOutputRowCount ?? 0;
  if (out <= SCAN_LARGE_OUTPUT) return undefined;
  const downstream = ctx.downstreamOps(opId);
  const hasFilterChild = downstream.some(id => {
    const childType = ctx.operatorType(id);
    return !!childType && FILTER_TYPE_PATTERN.test(childType);
  });
  if (hasFilterChild) return undefined;
  return {
    ruleId: "SCAN_FULL_TABLE_NO_FILTER",
    severity: "warning",
    message: `Scan emits ${out.toLocaleString()} rows with no immediate filter downstream. Apply a filter to reduce data volume early.`,
  };
}

function computeMedian(values: readonly number[]): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = sorted.length >> 1;
  return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
}
