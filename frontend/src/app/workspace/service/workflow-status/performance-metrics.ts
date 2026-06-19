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

import { OperatorStatistics } from "../../types/execute-workflow.interface";

/**
 * Derived, normalized-ready per-operator performance metrics.
 *
 * This is the ground-truth model consumed by the workflow heat-map overlay. It
 * is a flat, defensively-defaulted projection of the raw {@link OperatorStatistics}
 * the backend streams over the websocket — every field is a finite, non-negative
 * number so downstream scoring never has to re-validate.
 */
export interface OperatorPerformanceMetrics
  extends Readonly<{
    operatorId: string;
    dataProcessingTimeNs: number;
    controlProcessingTimeNs: number;
    idleTimeNs: number;
    inputRows: number;
    outputRows: number;
    inputSize: number;
    outputSize: number;
    numWorkers: number;
  }> {}

/**
 * The three heat-map views. Each answers a different "where is the problem?"
 * question; see {@link rawMetricForView} for the per-operator cost each one uses.
 * String-valued so the selection serializes readably (e.g. into localStorage).
 */
export enum HeatmapView {
  Runtime = "runtime",
  Throughput = "throughput",
  IoImbalance = "ioImbalance",
}

/**
 * Coerce an untrusted numeric field (it arrives over the websocket) into a
 * finite, non-negative number. Anything missing, non-numeric, NaN, infinite, or
 * negative collapses to 0 so no NaN/Infinity can leak into the scoring math.
 */
function toFiniteNonNegative(value: number | undefined): number {
  return typeof value === "number" && Number.isFinite(value) && value > 0 ? value : 0;
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

/**
 * Project a single raw {@link OperatorStatistics} into the flat performance model,
 * defaulting every optional/missing field to 0. Data and control processing time
 * are kept separate (the Runtime view uses data time only).
 */
export function toPerformanceMetrics(operatorId: string, stats: OperatorStatistics): OperatorPerformanceMetrics {
  return {
    operatorId,
    dataProcessingTimeNs: toFiniteNonNegative(stats.aggregatedDataProcessingTime),
    controlProcessingTimeNs: toFiniteNonNegative(stats.aggregatedControlProcessingTime),
    idleTimeNs: toFiniteNonNegative(stats.aggregatedIdleTime),
    inputRows: toFiniteNonNegative(stats.aggregatedInputRowCount),
    outputRows: toFiniteNonNegative(stats.aggregatedOutputRowCount),
    inputSize: toFiniteNonNegative(stats.aggregatedInputSize),
    outputSize: toFiniteNonNegative(stats.aggregatedOutputSize),
    numWorkers: toFiniteNonNegative(stats.numWorkers),
  };
}

/**
 * Per-operator raw cost for a view, BEFORE normalization. Bottleneck-oriented:
 * a higher cost means "hotter" (more of a problem).
 *
 * - Runtime:     data processing time — slower operators are hotter.
 * - Throughput:  1 / outputRows — slow producers are hotter; no output -> 0 (cold).
 * - IoImbalance: clamp(1 - out/in) — row-dropping operators are hotter; an
 *                amplifier (out > in) or a missing input clamps to 0 (cold).
 *
 * The metrics are already finite and non-negative (see {@link toPerformanceMetrics}),
 * so this never produces NaN/Infinity.
 */
export function rawMetricForView(metrics: OperatorPerformanceMetrics, view: HeatmapView): number {
  switch (view) {
    case HeatmapView.Runtime:
      return metrics.dataProcessingTimeNs;
    case HeatmapView.Throughput:
      return metrics.outputRows > 0 ? 1 / metrics.outputRows : 0;
    case HeatmapView.IoImbalance:
      return metrics.inputRows <= 0 ? 0 : clamp(1 - metrics.outputRows / metrics.inputRows, 0, 1);
    default:
      return 0;
  }
}

/**
 * Normalize per-operator raw costs into [0, 1] heat scores.
 *
 * Uses log1p compression then min-max across operators, so a single dominant
 * operator does not flatten everyone else toward 0. Rules:
 * - empty input            -> {}
 * - single operator        -> 1 if it did measurable work, else 0.5 (neutral)
 * - all values equal        -> 0.5 for everyone (no spread to show; avoids /0)
 * - otherwise              -> min maps to 0, max maps to 1, rest interpolated
 *
 * Non-finite / negative raw costs are treated as 0 before scoring.
 */
export function normalizeScores(rawById: Record<string, number>): Record<string, number> {
  const operatorIds = Object.keys(rawById);
  if (operatorIds.length === 0) {
    return {};
  }

  // Defensive coercion + log1p compression in one pass.
  const compressed: Record<string, number> = {};
  for (const operatorId of operatorIds) {
    compressed[operatorId] = Math.log1p(toFiniteNonNegative(rawById[operatorId]));
  }

  if (operatorIds.length === 1) {
    const onlyId = operatorIds[0];
    return { [onlyId]: compressed[onlyId] > 0 ? 1 : 0.5 };
  }

  const values = Object.values(compressed);
  const min = Math.min(...values);
  const max = Math.max(...values);

  // All operators have the same cost (covers the all-zero case): no spread to
  // render, so paint everyone neutral rather than dividing by zero.
  if (max === min) {
    const neutral: Record<string, number> = {};
    for (const operatorId of operatorIds) {
      neutral[operatorId] = 0.5;
    }
    return neutral;
  }

  const range = max - min;
  const scores: Record<string, number> = {};
  for (const operatorId of operatorIds) {
    scores[operatorId] = clamp((compressed[operatorId] - min) / range, 0, 1);
  }
  return scores;
}
