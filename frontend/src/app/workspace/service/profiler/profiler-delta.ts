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
 * Pure helpers for the "compare across runs" feature.
 *
 * Treats the JSON report produced by `profiler-report.ts` as the canonical
 * baseline snapshot format. Users upload a previously-downloaded report, we
 * parse it defensively, and produce per-operator deltas vs. the current run.
 *
 * Dependency-free (no Angular, no RxJS) so the math is unit-testable.
 */

import type { OperatorStatistics } from "../../types/execute-workflow.interface";

/**
 * Minimum operator shape needed for comparison. Matches a subset of
 * `ReportTopOperator` from `profiler-report.ts` — keeping it separate here
 * lets this module stand alone for testing and avoids tight coupling.
 */
export interface ComparableOperator {
  readonly operatorId: string;
  readonly displayName: string;
  readonly operatorType: string | null;
  readonly score: number;
  readonly runtimeMs: number | null;
  readonly throughputRowsPerSec: number | null;
  readonly inputRows: number;
  readonly outputRows: number;
  readonly inputSize: number | null;
  readonly outputSize: number | null;
  readonly workers: number | null;
  readonly idleRatio: number | null;
}

export type MatchStatus = "matched" | "new-in-current" | "removed-since-baseline";
export type DeltaDirection = "improved" | "regressed" | "unchanged" | "n/a";

export interface OperatorDelta {
  readonly operatorId: string;
  readonly displayName: string;
  readonly matchStatus: MatchStatus;
  readonly current?: ComparableOperator;
  readonly baseline?: ComparableOperator;
  /** Current minus baseline. `null` when either side is missing the metric. */
  readonly runtimeMsDelta: number | null;
  readonly throughputRowsPerSecDelta: number | null;
  readonly outputRowsDelta: number | null;
  readonly inputRowsDelta: number | null;
  readonly scoreDelta: number | null;
  /** Derived from runtimeMsDelta primarily, falling back to outputRowsDelta. */
  readonly direction: DeltaDirection;
}

export interface BaselineReport {
  readonly header: {
    readonly workflowName: string;
    readonly executionName: string | null;
    readonly generatedAt: string;
    readonly view: string;
    readonly hotThresholdPercentile: number;
    readonly operatorCount: number;
  };
  readonly operators: readonly ComparableOperator[];
}

const RUNTIME_UNCHANGED_ABS_MS = 1; // below this is rounding noise
const RUNTIME_UNCHANGED_RELATIVE = 0.05; // ±5% counts as unchanged

/**
 * Compute deltas for every operator id in either map. Operators missing on
 * one side surface as `new-in-current` or `removed-since-baseline`.
 */
export function computeAllDeltas(
  currentByOpId: Readonly<Record<string, ComparableOperator>>,
  baselineByOpId: Readonly<Record<string, ComparableOperator>>
): Readonly<Record<string, OperatorDelta>> {
  const out: Record<string, OperatorDelta> = {};
  const allIds = new Set<string>([...Object.keys(currentByOpId), ...Object.keys(baselineByOpId)]);
  for (const id of allIds) {
    out[id] = computeOperatorDelta(id, currentByOpId[id], baselineByOpId[id]);
  }
  return out;
}

/**
 * Compute a single operator's delta. Either side may be undefined — match
 * status and `direction` reflect that.
 */
export function computeOperatorDelta(
  opId: string,
  current: ComparableOperator | undefined,
  baseline: ComparableOperator | undefined
): OperatorDelta {
  if (current && !baseline) {
    return {
      operatorId: opId,
      displayName: current.displayName,
      matchStatus: "new-in-current",
      current,
      runtimeMsDelta: null,
      throughputRowsPerSecDelta: null,
      outputRowsDelta: null,
      inputRowsDelta: null,
      scoreDelta: null,
      direction: "n/a",
    };
  }
  if (!current && baseline) {
    return {
      operatorId: opId,
      displayName: baseline.displayName,
      matchStatus: "removed-since-baseline",
      baseline,
      runtimeMsDelta: null,
      throughputRowsPerSecDelta: null,
      outputRowsDelta: null,
      inputRowsDelta: null,
      scoreDelta: null,
      direction: "n/a",
    };
  }
  // matched
  const c = current!;
  const b = baseline!;
  const runtimeMsDelta = nullableDelta(c.runtimeMs, b.runtimeMs);
  const throughputRowsPerSecDelta = nullableDelta(c.throughputRowsPerSec, b.throughputRowsPerSec);
  const outputRowsDelta = c.outputRows - b.outputRows;
  const inputRowsDelta = c.inputRows - b.inputRows;
  const scoreDelta = c.score - b.score;
  return {
    operatorId: opId,
    displayName: c.displayName,
    matchStatus: "matched",
    current: c,
    baseline: b,
    runtimeMsDelta,
    throughputRowsPerSecDelta,
    outputRowsDelta,
    inputRowsDelta,
    scoreDelta,
    direction: deriveDirection(runtimeMsDelta, outputRowsDelta, b.runtimeMs),
  };
}

/**
 * Defensive parse of an uploaded JSON file purporting to be a P3 profiler
 * report. Returns `undefined` if the shape is not recognizable so callers
 * can show a friendly "unrecognized file" error instead of crashing.
 */
export function parseBaselineReport(raw: unknown): BaselineReport | undefined {
  if (!raw || typeof raw !== "object") return undefined;
  const obj = raw as Record<string, unknown>;
  const operatorsRaw = obj.operators;
  if (!Array.isArray(operatorsRaw)) return undefined;

  const operators: ComparableOperator[] = [];
  for (const op of operatorsRaw) {
    const parsed = parseOperator(op);
    if (parsed) operators.push(parsed);
  }
  if (operators.length === 0) return undefined;

  const header = parseHeader(obj.header) ?? defaultHeader(operators.length);
  return { header, operators };
}

function parseOperator(raw: unknown): ComparableOperator | undefined {
  if (!raw || typeof raw !== "object") return undefined;
  const o = raw as Record<string, unknown>;
  const operatorId = typeof o.operatorId === "string" ? o.operatorId : undefined;
  if (!operatorId) return undefined;
  return {
    operatorId,
    displayName: typeof o.displayName === "string" ? o.displayName : operatorId,
    operatorType: typeof o.operatorType === "string" ? o.operatorType : null,
    score: typeof o.score === "number" && Number.isFinite(o.score) ? o.score : 0,
    runtimeMs: numOrNull(o.runtimeMs),
    throughputRowsPerSec: numOrNull(o.throughputRowsPerSec),
    inputRows: typeof o.inputRows === "number" && Number.isFinite(o.inputRows) ? o.inputRows : 0,
    outputRows: typeof o.outputRows === "number" && Number.isFinite(o.outputRows) ? o.outputRows : 0,
    inputSize: numOrNull(o.inputSize),
    outputSize: numOrNull(o.outputSize),
    workers: numOrNull(o.workers),
    idleRatio: numOrNull(o.idleRatio),
  };
}

function parseHeader(raw: unknown): BaselineReport["header"] | undefined {
  if (!raw || typeof raw !== "object") return undefined;
  const o = raw as Record<string, unknown>;
  return {
    workflowName: typeof o.workflowName === "string" ? o.workflowName : "(unknown)",
    executionName: typeof o.executionName === "string" ? o.executionName : null,
    generatedAt: typeof o.generatedAt === "string" ? o.generatedAt : "(unknown)",
    view: typeof o.view === "string" ? o.view : "runtime",
    hotThresholdPercentile:
      typeof o.hotThresholdPercentile === "number" && Number.isFinite(o.hotThresholdPercentile)
        ? o.hotThresholdPercentile
        : 80,
    operatorCount:
      typeof o.operatorCount === "number" && Number.isFinite(o.operatorCount) ? o.operatorCount : 0,
  };
}

function defaultHeader(operatorCount: number): BaselineReport["header"] {
  return {
    workflowName: "(uploaded baseline)",
    executionName: null,
    generatedAt: "(unknown)",
    view: "runtime",
    hotThresholdPercentile: 80,
    operatorCount,
  };
}

/** Index a baseline by operator id for cheap delta lookup. */
export function indexBaseline(baseline: BaselineReport): Readonly<Record<string, ComparableOperator>> {
  const out: Record<string, ComparableOperator> = {};
  for (const op of baseline.operators) {
    out[op.operatorId] = op;
  }
  return out;
}

/**
 * Converts the live profiler state (raw `OperatorStatistics` + a normalized score
 * and identity metadata) into the comparable shape used for delta computation.
 *
 * Mirrors the metric derivations done by the report builder so the two are
 * apples-to-apples when diffing.
 */
export function statsToComparable(input: {
  readonly operatorId: string;
  readonly displayName: string;
  readonly operatorType: string | null | undefined;
  readonly score: number;
  readonly stats: OperatorStatistics;
}): ComparableOperator {
  const s = input.stats;
  const runtimeNs = s.aggregatedDataProcessingTime;
  const runtimeMs = runtimeNs && runtimeNs > 0 ? runtimeNs / 1_000_000 : null;
  const outRows = s.aggregatedOutputRowCount ?? 0;
  const throughputRowsPerSec =
    runtimeNs && runtimeNs > 0 && outRows > 0 ? outRows / (runtimeNs / 1_000_000_000) : null;

  const dataNs = s.aggregatedDataProcessingTime ?? 0;
  const ctrlNs = s.aggregatedControlProcessingTime ?? 0;
  const idleNs = s.aggregatedIdleTime ?? 0;
  const totalNs = dataNs + ctrlNs + idleNs;
  const idleRatio = totalNs > 0 ? idleNs / totalNs : null;

  return {
    operatorId: input.operatorId,
    displayName: input.displayName,
    operatorType: input.operatorType ?? null,
    score: input.score,
    runtimeMs,
    throughputRowsPerSec,
    inputRows: s.aggregatedInputRowCount ?? 0,
    outputRows: outRows,
    inputSize: s.aggregatedInputSize ?? null,
    outputSize: s.aggregatedOutputSize ?? null,
    workers: s.numWorkers ?? null,
    idleRatio,
  };
}

function numOrNull(v: unknown): number | null {
  return typeof v === "number" && Number.isFinite(v) ? v : null;
}

function nullableDelta(a: number | null, b: number | null): number | null {
  if (a === null || b === null) return null;
  return a - b;
}

function deriveDirection(
  runtimeDelta: number | null,
  outputRowsDelta: number,
  baselineRuntime: number | null
): DeltaDirection {
  if (runtimeDelta === null) {
    // No runtime change is computable. Fall back to output-row change: more
    // output is generally a sign the operator is running further along, so
    // treat sign as a weak indicator of progress (improved) only when the
    // runtime side is uncomputable.
    if (outputRowsDelta === 0) return "unchanged";
    return outputRowsDelta > 0 ? "improved" : "regressed";
  }
  const absUnchanged = Math.abs(runtimeDelta) < RUNTIME_UNCHANGED_ABS_MS;
  const relUnchanged =
    baselineRuntime !== null && baselineRuntime > 0
      ? Math.abs(runtimeDelta) / baselineRuntime < RUNTIME_UNCHANGED_RELATIVE
      : false;
  if (absUnchanged || relUnchanged) return "unchanged";
  return runtimeDelta < 0 ? "improved" : "regressed";
}
