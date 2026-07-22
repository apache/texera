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
  formatMetricForView,
  HeatmapView,
  heatmapViewTitle,
  normalizeScores,
  rawMetricForView,
} from "./heatmap-scoring";
import { OperatorPerformanceMetrics } from "../workflow-status/performance-metrics";

function makeMetrics(overrides: Partial<OperatorPerformanceMetrics>): OperatorPerformanceMetrics {
  return {
    dataProcessingTimeNs: 0,
    controlProcessingTimeNs: 0,
    idleTimeNs: 0,
    inputRows: 0,
    outputRows: 0,
    inputSize: 0,
    outputSize: 0,
    numWorkers: 1,
    ...overrides,
  };
}

describe("rawMetricForView", () => {
  it("Runtime sums data and control processing time", () => {
    const m = makeMetrics({ dataProcessingTimeNs: 5_000_000, controlProcessingTimeNs: 2_000_000 });
    expect(rawMetricForView(m, HeatmapView.Runtime)).toBe(7_000_000);
  });

  it("Throughput returns seconds per output tuple (slow producers are hotter)", () => {
    // 2s of processing over 4 rows -> 0.5s per tuple
    const m = makeMetrics({ dataProcessingTimeNs: 2_000_000_000, outputRows: 4 });
    expect(rawMetricForView(m, HeatmapView.Throughput)).toBe(0.5);
  });

  it("Throughput returns 0 when there is no output (cold)", () => {
    const m = makeMetrics({ dataProcessingTimeNs: 2_000_000_000, outputRows: 0 });
    expect(rawMetricForView(m, HeatmapView.Throughput)).toBe(0);
  });

  it("Throughput returns 0 when there is no processing time (infinitely fast -> cold)", () => {
    const m = makeMetrics({ dataProcessingTimeNs: 0, controlProcessingTimeNs: 0, outputRows: 10 });
    expect(rawMetricForView(m, HeatmapView.Throughput)).toBe(0);
  });

  it("IoImbalance scores a row-dropping operator (out < in)", () => {
    // |250 - 1000| / (250 + 1000) = 0.6
    const m = makeMetrics({ inputRows: 1_000, outputRows: 250 });
    expect(rawMetricForView(m, HeatmapView.IoImbalance)).toBe(0.6);
  });

  it("IoImbalance scores an amplifying operator (out > in)", () => {
    // |300 - 100| / (300 + 100) = 0.5
    const m = makeMetrics({ inputRows: 100, outputRows: 300 });
    expect(rawMetricForView(m, HeatmapView.IoImbalance)).toBe(0.5);
  });

  it("IoImbalance scores a total drop (out = 0) as maximally imbalanced", () => {
    const m = makeMetrics({ inputRows: 1_000, outputRows: 0 });
    expect(rawMetricForView(m, HeatmapView.IoImbalance)).toBe(1);
  });

  it("IoImbalance stays within [0, 1] even for an extreme amplifier", () => {
    const m = makeMetrics({ inputRows: 1, outputRows: 1_000_000 });
    const score = rawMetricForView(m, HeatmapView.IoImbalance);
    expect(score).toBeGreaterThan(0.99);
    expect(score).toBeLessThanOrEqual(1);
  });

  it("IoImbalance is 0 for a balanced operator (out == in)", () => {
    const m = makeMetrics({ inputRows: 1_000, outputRows: 1_000 });
    expect(rawMetricForView(m, HeatmapView.IoImbalance)).toBe(0);
  });

  it("IoImbalance is 0 when there is no input (cold)", () => {
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

  it("scores a single operator that did work as 1", () => {
    expect(normalizeScores({ a: 42 })).toEqual({ a: 1 });
  });

  it("scores a single operator that did no work as 0.5", () => {
    expect(normalizeScores({ a: 0 })).toEqual({ a: 0.5 });
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
    // Linear min-max would map 100 to ~0.1; log scaling lifts it above 0.5.
    const scores = normalizeScores({ small: 1, mid: 100, big: 1000 });
    expect(scores["small"]).toBe(0);
    expect(scores["big"]).toBe(1);
    expect(scores["mid"]).toBeGreaterThan(0.5);
  });
});

describe("formatMetricForView", () => {
  it("formats Runtime nanoseconds as a human duration", () => {
    expect(formatMetricForView(8_620_000_000, HeatmapView.Runtime)).toBe("8.62 s");
    expect(formatMetricForView(5_000_000, HeatmapView.Runtime)).toBe("5 ms");
    expect(formatMetricForView(2_000, HeatmapView.Runtime)).toBe("2 µs");
    expect(formatMetricForView(500, HeatmapView.Runtime)).toBe("500 ns");
  });

  it("formats Throughput as time-per-row", () => {
    expect(formatMetricForView(2, HeatmapView.Throughput)).toBe("2.00 s/row");
    expect(formatMetricForView(0.0015, HeatmapView.Throughput)).toBe("1.5 ms/row");
    expect(formatMetricForView(0.0005, HeatmapView.Throughput)).toBe("500 µs/row");
  });

  it("formats I/O imbalance as a 2-decimal ratio", () => {
    expect(formatMetricForView(0.75, HeatmapView.IoImbalance)).toBe("0.75");
    expect(formatMetricForView(4, HeatmapView.IoImbalance)).toBe("4.00");
  });

  it("renders 0 for non-positive or non-finite values", () => {
    expect(formatMetricForView(0, HeatmapView.Runtime)).toBe("0");
    expect(formatMetricForView(Number.NaN, HeatmapView.Throughput)).toBe("0");
    expect(formatMetricForView(Number.POSITIVE_INFINITY, HeatmapView.IoImbalance)).toBe("0");
  });
});

describe("heatmapViewTitle", () => {
  it("returns a human-readable title for each view", () => {
    expect(heatmapViewTitle(HeatmapView.Runtime)).toBe("Runtime");
    expect(heatmapViewTitle(HeatmapView.Throughput)).toBe("Throughput");
    expect(heatmapViewTitle(HeatmapView.IoImbalance)).toBe("I/O imbalance");
  });

  it("falls back to a generic title for an unexpected value", () => {
    expect(heatmapViewTitle("bogus" as HeatmapView)).toBe("Performance");
  });
});
