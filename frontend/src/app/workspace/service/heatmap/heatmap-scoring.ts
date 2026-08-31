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

import { OperatorPerformanceMetrics } from "../workflow-status/performance-metrics";

/**
 * The three heat-map views. Each answers a different "where should I look?"
 * question; see {@link rawMetricForView} for the per-operator cost each uses.
 * String-valued so the selection serializes readably (e.g. to localStorage).
 */
export enum HeatmapView {
  Runtime = "runtime",
  // Seconds per output row — the reciprocal of throughput, so it grows (hotter) as throughput
  // falls. Named directionally rather than "throughput" to match how the ramp reads.
  TimePerRow = "time-per-row",
  IoImbalance = "io-imbalance",
}

/**
 * Per-operator raw cost for a view, BEFORE normalization. Higher = hotter (more
 * worth a look).
 *
 * Returns undefined when the view's metric is NOT MEASURABLE for the operator,
 * as opposed to measured-as-zero. Callers must exclude undefined from
 * normalization and the legend range and render it as no-data; folding it into
 * 0 would paint the operator coldest and anchor the scale minimum — e.g. a
 * blocking operator mid-run (rows consumed, none emitted yet) would read as
 * the fastest thing on the canvas.
 *
 * - Runtime:     data + control processing time — slower operators are hotter.
 *                Always measurable (0 = genuinely idle).
 * - TimePerRow:  seconds per output tuple — slow producers (low throughput) are
 *                hotter; undefined until the operator has emitted a row.
 * - IoImbalance: |out - in| / (out + in) — operators that drop OR amplify rows
 *                are hotter; a balanced operator -> 0 (cold); undefined without
 *                input rows (sources, or nothing consumed yet). Bounded to
 *                [0, 1] so an extreme amplifier can't dominate the scale.
 */
export function rawMetricForView(metrics: OperatorPerformanceMetrics, view: HeatmapView): number | undefined {
  switch (view) {
    case HeatmapView.Runtime:
      return metrics.dataProcessingTimeNs + metrics.controlProcessingTimeNs;
    case HeatmapView.TimePerRow: {
      if (metrics.outputRows === 0) {
        return undefined;
      }
      const timeSec = (metrics.dataProcessingTimeNs + metrics.controlProcessingTimeNs) / 1e9;
      return timeSec / metrics.outputRows;
    }
    case HeatmapView.IoImbalance:
      return metrics.inputRows > 0
        ? Math.abs(metrics.outputRows - metrics.inputRows) / (metrics.outputRows + metrics.inputRows)
        : undefined;
    default:
      return undefined;
  }
}

/** Human-readable title for a view, shown in the legend and hover tooltip. */
export function heatmapViewTitle(view: HeatmapView): string {
  switch (view) {
    case HeatmapView.Runtime:
      return "Runtime";
    case HeatmapView.TimePerRow:
      return "Time / row";
    case HeatmapView.IoImbalance:
      return "I/O imbalance";
    default:
      // Total function: guards against an unexpected (e.g. persisted) value slipping through.
      return "Performance";
  }
}

function formatNanos(ns: number): string {
  if (ns >= 1e9) return `${(ns / 1e9).toFixed(2)} s`;
  if (ns >= 1e6) return `${(ns / 1e6).toFixed(0)} ms`;
  if (ns >= 1e3) return `${(ns / 1e3).toFixed(0)} µs`;
  return `${Math.round(ns)} ns`;
}

function formatSeconds(seconds: number): string {
  if (seconds >= 1) return `${seconds.toFixed(2)} s`;
  if (seconds >= 1e-3) return `${(seconds * 1e3).toFixed(1)} ms`;
  return `${(seconds * 1e6).toFixed(0)} µs`;
}

/**
 * Human-readable label for a raw view metric, used by the legend to show the actual value range
 * behind the color scale and by the hover tooltip. Units match each view: Runtime is a duration,
 * Time/row is seconds per row, I/O imbalance is a unitless ratio. An undefined value (the view is
 * not measurable for the operator, see {@link rawMetricForView}) renders as "—" so it cannot be
 * confused with a genuine zero.
 */
export function formatMetricForView(value: number | undefined, view: HeatmapView): string {
  if (value === undefined) {
    return "—";
  }
  if (!Number.isFinite(value) || value <= 0) {
    return "0";
  }
  switch (view) {
    case HeatmapView.Runtime:
      return formatNanos(value);
    case HeatmapView.TimePerRow:
      return `${formatSeconds(value)}/row`;
    case HeatmapView.IoImbalance:
      return value.toFixed(2);
    default:
      return String(value);
  }
}

/**
 * Normalize per-operator raw costs into [0, 1] heat scores.
 *
 * Uses log1p compression then min-max across operators, so a single dominant
 * operator does not flatten everyone else toward 0. Rules:
 * - empty input     -> {}
 * - single operator -> 1 if it did measurable work, else 0.5 (neutral)
 * - all values equal -> 0.5 for everyone (no spread to show; avoids /0)
 * - otherwise       -> min maps to 0, max maps to 1, rest interpolated
 */
export function normalizeScores(rawById: Record<string, number>): Record<string, number> {
  const ids = Object.keys(rawById);
  if (ids.length === 0) {
    return {};
  }

  // Log-compress every raw cost so a heavy tail doesn't flatten everyone else.
  const compressed: Record<string, number> = {};
  for (const id of ids) {
    compressed[id] = Math.log1p(rawById[id]);
  }

  // A single operator is trivially the hottest, unless it did no measurable work.
  if (ids.length === 1) {
    return { [ids[0]]: compressed[ids[0]] > 0 ? 1 : 0.5 };
  }

  const values = Object.values(compressed);
  const min = Math.min(...values);
  const max = Math.max(...values);

  const scores: Record<string, number> = {};

  // Everything equal (covers all-zero): no spread to show, and avoids /0.
  if (max === min) {
    for (const id of ids) {
      scores[id] = 0.5;
    }
    return scores;
  }

  // Linear min-max into [0, 1].
  const range = max - min;
  for (const id of ids) {
    scores[id] = (compressed[id] - min) / range;
  }
  return scores;
}
