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

import { BaselineReport, ComparableOperator } from "./profiler-delta";

/**
 * P6 (compare across runs): pure helpers that convert the backend's persisted
 * per-execution stats into the same `BaselineReport` shape used by the existing
 * upload-baseline flow. Lets us reuse all the downstream delta math + UI for
 * free — the only new path is "fetch a previous execution → set baseline."
 *
 * Wire format mirrors `WorkflowExecutionsResource.WorkflowRuntimeStatistics`:
 * each row is one cumulative snapshot of one operator at one timestamp.
 * Multiple rows per operator are normal (polled at the engine's update interval);
 * the LATEST row per operator carries the final cumulative totals.
 */

/** One row from `GET /api/executions/{wid}/stats/{eid}`. */
export interface WorkflowRuntimeStatsRow {
  operatorId: string;
  /** ISO-8601 string or millisecond epoch — converted to ms via `Date.parse` / `+ts`. */
  timestamp: string | number;
  inputTupleCount: number;
  inputTupleSize: number;
  outputTupleCount: number;
  outputTupleSize: number;
  /** Nanoseconds, matching the live stream. */
  dataProcessingTime: number;
  /** Nanoseconds. */
  controlProcessingTime: number;
  /** Nanoseconds. */
  idleTime: number;
  numWorkers: number;
  status: number;
}

/** One row from `GET /api/executions/{wid}` (the executions list). */
export interface WorkflowExecutionEntry {
  eId: number;
  vId: number;
  cuId: number;
  userName: string;
  googleAvatar: string;
  status: number;
  result: string;
  /** ISO-8601 string or millisecond epoch. */
  startingTime: string | number;
  /** ISO-8601 string or millisecond epoch. */
  completionTime: string | number;
  bookmarked: boolean;
  name: string;
  logLocation: string;
}

/**
 * From a list of cumulative snapshot rows, return the LAST row per operator
 * (by timestamp). Defensive: rows with non-parseable timestamps are dropped.
 */
export function latestRowPerOperator(
  rows: readonly WorkflowRuntimeStatsRow[]
): WorkflowRuntimeStatsRow[] {
  const byOp = new Map<string, { row: WorkflowRuntimeStatsRow; ts: number }>();
  for (const row of rows) {
    const ts = toEpochMs(row.timestamp);
    if (!Number.isFinite(ts)) continue;
    const existing = byOp.get(row.operatorId);
    if (!existing || ts >= existing.ts) {
      byOp.set(row.operatorId, { row, ts });
    }
  }
  return Array.from(byOp.values()).map(v => v.row);
}

/**
 * Convert a single (already-deduplicated) snapshot row into the comparable
 * shape used by the delta engine. Mirrors the live derivations in
 * `profiler-delta.statsToComparable` so historical and live runs are
 * apples-to-apples.
 */
function rowToComparable(row: WorkflowRuntimeStatsRow): ComparableOperator {
  const dataNs = nonNegOrZero(row.dataProcessingTime);
  const ctrlNs = nonNegOrZero(row.controlProcessingTime);
  const idleNs = nonNegOrZero(row.idleTime);
  const totalNs = dataNs + ctrlNs + idleNs;
  const runtimeMs = dataNs > 0 ? dataNs / 1_000_000 : null;
  const outRows = nonNegOrZero(row.outputTupleCount);
  const inRows = nonNegOrZero(row.inputTupleCount);
  const throughputRowsPerSec =
    dataNs > 0 && outRows > 0 ? outRows / (dataNs / 1_000_000_000) : null;
  const idleRatio = totalNs > 0 ? idleNs / totalNs : null;

  return {
    operatorId: row.operatorId,
    // The backend's persisted stats don't carry a friendly display name, so
    // mirror the operator id. The current workflow will provide the real name
    // when the delta is rendered (via the live operator-by-id lookup).
    displayName: row.operatorId,
    operatorType: null,
    // Score is recomputed on the live side; baseline scores aren't needed.
    score: 0,
    runtimeMs,
    throughputRowsPerSec,
    inputRows: inRows,
    outputRows: outRows,
    inputSize: row.inputTupleSize > 0 ? row.inputTupleSize : null,
    outputSize: row.outputTupleSize > 0 ? row.outputTupleSize : null,
    workers: row.numWorkers > 0 ? row.numWorkers : null,
    idleRatio,
  };
}

export interface ConvertRowsToBaselineInput {
  rows: readonly WorkflowRuntimeStatsRow[];
  /** Used in the report's header so the UI can show "Comparing to: …". */
  workflowName: string;
  executionName: string | null;
  /** When the baseline run was generated — typically the execution's completion time. */
  generatedAt: string;
  /** Profiler view active at the time the historical run was captured (or now). */
  view?: string;
  /** Hot-threshold percentile in effect (default 80 to match the frontend). */
  hotThresholdPercentile?: number;
}

/**
 * Build a `BaselineReport` from raw runtime-statistics rows. Handles the
 * "multiple snapshot rows per operator" case by keeping only the latest row
 * (highest timestamp) per operatorId.
 *
 * Returns `undefined` if the input rows yield zero valid operators — caller
 * should treat that as "no baseline data available for this execution" and
 * skip calling `setBaseline`.
 */
export function convertStatsRowsToBaseline(
  input: ConvertRowsToBaselineInput
): BaselineReport | undefined {
  const latest = latestRowPerOperator(input.rows);
  if (latest.length === 0) return undefined;
  const operators = latest.map(rowToComparable);
  return {
    header: {
      workflowName: input.workflowName,
      executionName: input.executionName,
      generatedAt: input.generatedAt,
      view: input.view ?? "runtime",
      hotThresholdPercentile:
        typeof input.hotThresholdPercentile === "number" ? input.hotThresholdPercentile : 80,
      operatorCount: operators.length,
    },
    operators,
  };
}

function toEpochMs(ts: string | number | undefined | null): number {
  if (ts == null) return NaN;
  if (typeof ts === "number") return ts;
  const parsed = Date.parse(ts);
  return Number.isFinite(parsed) ? parsed : NaN;
}

function nonNegOrZero(v: number | null | undefined): number {
  if (typeof v !== "number" || !Number.isFinite(v) || v < 0) return 0;
  return v;
}
