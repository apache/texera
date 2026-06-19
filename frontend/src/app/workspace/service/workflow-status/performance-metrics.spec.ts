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

import {
  HeatmapView,
  OperatorPerformanceMetrics,
  normalizeScores,
  rawMetricForView,
  toPerformanceMetrics,
} from "./performance-metrics";
import { OperatorState, OperatorStatistics } from "../../types/execute-workflow.interface";

/**
 * A complete statistics object, mirroring what the backend sends once every
 * field is typed. All five timing/size fields are present.
 */
const fullStats: OperatorStatistics = {
  operatorState: OperatorState.Running,
  aggregatedInputRowCount: 1_000_000,
  aggregatedInputSize: 84_000_000,
  inputPortMetrics: { "0": 1_000_000 },
  aggregatedOutputRowCount: 12_000,
  aggregatedOutputSize: 1_010_000,
  outputPortMetrics: { "0": 12_000 },
  numWorkers: 4,
  aggregatedDataProcessingTime: 8_500_000_000,
  aggregatedControlProcessingTime: 120_000_000,
  aggregatedIdleTime: 300_000_000,
};

/**
 * The partial shape produced by WorkflowStatusService.resetStatus(): only the
 * required fields, none of the five optional timing/size fields, no numWorkers.
 * The mapper must survive this without emitting NaN/undefined.
 */
const partialStats: OperatorStatistics = {
  operatorState: OperatorState.Uninitialized,
  aggregatedInputRowCount: 0,
  inputPortMetrics: {},
  aggregatedOutputRowCount: 0,
  outputPortMetrics: {},
};

/** Build an OperatorPerformanceMetrics for the rawMetricForView tests. */
function makeMetrics(overrides: Partial<OperatorPerformanceMetrics>): OperatorPerformanceMetrics {
  return {
    operatorId: "op",
    dataProcessingTimeNs: 0,
    controlProcessingTimeNs: 0,
    idleTimeNs: 0,
    inputRows: 0,
    outputRows: 0,
    inputSize: 0,
    outputSize: 0,
    numWorkers: 0,
    ...overrides,
  };
}

describe("toPerformanceMetrics", () => {
  it("maps every field from a full statistics object", () => {
    const m = toPerformanceMetrics("Filter-operator-a1b2", fullStats);
    expect(m).toEqual({
      operatorId: "Filter-operator-a1b2",
      dataProcessingTimeNs: 8_500_000_000,
      controlProcessingTimeNs: 120_000_000,
      idleTimeNs: 300_000_000,
      inputRows: 1_000_000,
      outputRows: 12_000,
      inputSize: 84_000_000,
      outputSize: 1_010_000,
      numWorkers: 4,
    });
  });

  it("keeps data and control processing time as separate fields", () => {
    const m = toPerformanceMetrics("op", fullStats);
    expect(m.dataProcessingTimeNs).toBe(fullStats.aggregatedDataProcessingTime);
    expect(m.controlProcessingTimeNs).toBe(fullStats.aggregatedControlProcessingTime);
  });

  it("defaults every optional/missing field to 0 (no NaN, no undefined)", () => {
    const m = toPerformanceMetrics("op", partialStats);
    expect(m).toEqual({
      operatorId: "op",
      dataProcessingTimeNs: 0,
      controlProcessingTimeNs: 0,
      idleTimeNs: 0,
      inputRows: 0,
      outputRows: 0,
      inputSize: 0,
      outputSize: 0,
      numWorkers: 0,
    });
    // explicit guard: nothing leaked through as NaN
    for (const value of Object.values(m)) {
      if (typeof value === "number") {
        expect(Number.isNaN(value)).toBe(false);
      }
    }
  });

  it("preserves a unicode operator id verbatim", () => {
    const id = "算子-✓-1";
    expect(toPerformanceMetrics(id, fullStats).operatorId).toBe(id);
  });
});

describe("rawMetricForView", () => {
  // Bottleneck-oriented semantics: a higher raw cost means "hotter" (more of a
  // problem), before normalization.

  it("Runtime returns the data processing time (hotter = slower)", () => {
    const m = makeMetrics({ dataProcessingTimeNs: 8_500_000_000 });
    expect(rawMetricForView(m, HeatmapView.Runtime)).toBe(8_500_000_000);
  });

  it("Throughput inverts output rows so slow producers are hot", () => {
    const m = makeMetrics({ outputRows: 4 });
    expect(rawMetricForView(m, HeatmapView.Throughput)).toBe(0.25);
  });

  it("Throughput returns 0 when there is no output (no divide-by-zero)", () => {
    const m = makeMetrics({ outputRows: 0 });
    const score = rawMetricForView(m, HeatmapView.Throughput);
    expect(score).toBe(0);
    expect(Number.isFinite(score)).toBe(true);
  });

  it("IoImbalance scores the row-drop rate (1 - out/in)", () => {
    const m = makeMetrics({ inputRows: 1_000, outputRows: 250 });
    expect(rawMetricForView(m, HeatmapView.IoImbalance)).toBe(0.75);
  });

  it("IoImbalance returns 0 for an amplifier (out > in), clamped to [0,1]", () => {
    const m = makeMetrics({ inputRows: 100, outputRows: 500 });
    expect(rawMetricForView(m, HeatmapView.IoImbalance)).toBe(0);
  });

  it("IoImbalance returns 0 when inputRows is 0 (no divide-by-zero)", () => {
    const m = makeMetrics({ inputRows: 0, outputRows: 250 });
    const score = rawMetricForView(m, HeatmapView.IoImbalance);
    expect(score).toBe(0);
    expect(Number.isFinite(score)).toBe(true);
  });
});

describe("normalizeScores", () => {
  it("returns an empty object for empty input", () => {
    expect(normalizeScores({})).toEqual({});
  });

  it("scores a single operator as 1 (it is the hottest)", () => {
    expect(normalizeScores({ a: 42 })).toEqual({ a: 1 });
  });

  it("scores all-equal values as 0.5 (avoids divide-by-zero)", () => {
    expect(normalizeScores({ a: 5, b: 5, c: 5 })).toEqual({ a: 0.5, b: 0.5, c: 0.5 });
  });

  it("scores all-zero values as 0.5", () => {
    expect(normalizeScores({ a: 0, b: 0 })).toEqual({ a: 0.5, b: 0.5 });
  });

  it("maps the min to 0 and the max to 1 for two distinct values", () => {
    const scores = normalizeScores({ low: 1, high: 100 });
    expect(scores["low"]).toBe(0);
    expect(scores["high"]).toBe(1);
  });

  it("keeps all scores within [0, 1]", () => {
    const scores = normalizeScores({ a: 3, b: 50, c: 900, d: 12 });
    for (const s of Object.values(scores)) {
      expect(s).toBeGreaterThanOrEqual(0);
      expect(s).toBeLessThanOrEqual(1);
    }
  });

  it("compresses heavy-tailed values so the middle is not flattened to ~0", () => {
    // With plain linear min-max, 100 would map to (100-1)/(1000-1) ≈ 0.1.
    // Log scaling lifts the middle well above 0.5, which is the point.
    const scores = normalizeScores({ small: 1, mid: 100, big: 1000 });
    expect(scores["small"]).toBe(0);
    expect(scores["big"]).toBe(1);
    expect(scores["mid"]).toBeGreaterThan(0.5);
  });

  it("treats non-finite raw values as 0 rather than propagating NaN/Infinity", () => {
    const scores = normalizeScores({ bad: Number.POSITIVE_INFINITY, worse: NaN, good: 100 });
    for (const s of Object.values(scores)) {
      expect(Number.isFinite(s)).toBe(true);
    }
    // the only real magnitude wins the top of the scale
    expect(scores["good"]).toBe(1);
  });

  it("preserves unicode operator ids as keys", () => {
    const scores = normalizeScores({ "算子-✓": 10, b: 20 });
    expect(Object.keys(scores).sort()).toEqual(["b", "算子-✓"].sort());
  });
});
