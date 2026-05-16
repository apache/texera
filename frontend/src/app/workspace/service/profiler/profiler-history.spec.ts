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

import { describe, it, expect } from "vitest";
import {
  convertStatsRowsToBaseline,
  latestRowPerOperator,
  WorkflowRuntimeStatsRow,
} from "./profiler-history";

function makeRow(overrides: Partial<WorkflowRuntimeStatsRow> = {}): WorkflowRuntimeStatsRow {
  return {
    operatorId: "op-1",
    timestamp: "2026-05-15T12:00:00.000Z",
    inputTupleCount: 0,
    inputTupleSize: 0,
    outputTupleCount: 0,
    outputTupleSize: 0,
    dataProcessingTime: 0,
    controlProcessingTime: 0,
    idleTime: 0,
    numWorkers: 1,
    status: 0,
    ...overrides,
  };
}

describe("latestRowPerOperator", () => {
  it("returns [] for empty input", () => {
    expect(latestRowPerOperator([])).toEqual([]);
  });

  it("keeps the single row when only one is present", () => {
    const r = makeRow({ operatorId: "x" });
    expect(latestRowPerOperator([r])).toEqual([r]);
  });

  it("keeps the highest-timestamp row per operator (string timestamps)", () => {
    const early = makeRow({ operatorId: "x", timestamp: "2026-05-15T12:00:00.000Z", outputTupleCount: 1 });
    const late = makeRow({ operatorId: "x", timestamp: "2026-05-15T12:05:00.000Z", outputTupleCount: 9 });
    const out = latestRowPerOperator([early, late]);
    expect(out).toHaveLength(1);
    expect(out[0].outputTupleCount).toBe(9);
  });

  it("keeps the highest-timestamp row per operator (numeric ms timestamps)", () => {
    const early = makeRow({ operatorId: "x", timestamp: 1000, outputTupleCount: 1 });
    const late = makeRow({ operatorId: "x", timestamp: 5000, outputTupleCount: 9 });
    const out = latestRowPerOperator([late, early]);
    expect(out[0].outputTupleCount).toBe(9);
  });

  it("preserves the LAST row when timestamps tie (stable upsert)", () => {
    const a = makeRow({ operatorId: "x", timestamp: 100, outputTupleCount: 1 });
    const b = makeRow({ operatorId: "x", timestamp: 100, outputTupleCount: 2 });
    const out = latestRowPerOperator([a, b]);
    expect(out[0].outputTupleCount).toBe(2);
  });

  it("returns one row per operator even when interleaved", () => {
    const rows: WorkflowRuntimeStatsRow[] = [
      makeRow({ operatorId: "x", timestamp: 100, outputTupleCount: 1 }),
      makeRow({ operatorId: "y", timestamp: 200, outputTupleCount: 10 }),
      makeRow({ operatorId: "x", timestamp: 300, outputTupleCount: 3 }),
      makeRow({ operatorId: "y", timestamp: 400, outputTupleCount: 30 }),
    ];
    const out = latestRowPerOperator(rows);
    expect(out).toHaveLength(2);
    const byId = Object.fromEntries(out.map(r => [r.operatorId, r.outputTupleCount]));
    expect(byId).toEqual({ x: 3, y: 30 });
  });

  it("drops rows with non-parseable timestamps (defensive)", () => {
    const ok = makeRow({ operatorId: "x", timestamp: 200, outputTupleCount: 9 });
    const bad = makeRow({ operatorId: "y", timestamp: "not a date", outputTupleCount: 999 });
    const out = latestRowPerOperator([ok, bad]);
    expect(out).toHaveLength(1);
    expect(out[0].operatorId).toBe("x");
  });
});

describe("convertStatsRowsToBaseline", () => {
  it("returns undefined when no rows yield a valid latest entry", () => {
    expect(
      convertStatsRowsToBaseline({
        rows: [],
        workflowName: "wf",
        executionName: null,
        generatedAt: "2026-05-15T12:00:00.000Z",
      })
    ).toBeUndefined();
    // Single row with an unparseable timestamp also yields nothing.
    expect(
      convertStatsRowsToBaseline({
        rows: [makeRow({ timestamp: "garbage" })],
        workflowName: "wf",
        executionName: null,
        generatedAt: "now",
      })
    ).toBeUndefined();
  });

  it("populates the header with workflowName / executionName / generatedAt / operatorCount", () => {
    const out = convertStatsRowsToBaseline({
      rows: [
        makeRow({ operatorId: "a" }),
        makeRow({ operatorId: "b", timestamp: "2026-05-15T12:01:00.000Z" }),
      ],
      workflowName: "My Workflow",
      executionName: "run-12",
      generatedAt: "2026-05-15T12:30:00.000Z",
    });
    expect(out).toBeDefined();
    expect(out!.header.workflowName).toBe("My Workflow");
    expect(out!.header.executionName).toBe("run-12");
    expect(out!.header.generatedAt).toBe("2026-05-15T12:30:00.000Z");
    expect(out!.header.operatorCount).toBe(2);
    // Defaults: view "runtime", hot threshold 80.
    expect(out!.header.view).toBe("runtime");
    expect(out!.header.hotThresholdPercentile).toBe(80);
  });

  it("honors explicit view + hotThresholdPercentile when supplied", () => {
    const out = convertStatsRowsToBaseline({
      rows: [makeRow({ operatorId: "a" })],
      workflowName: "w",
      executionName: null,
      generatedAt: "x",
      view: "throughput",
      hotThresholdPercentile: 95,
    });
    expect(out!.header.view).toBe("throughput");
    expect(out!.header.hotThresholdPercentile).toBe(95);
  });

  it("converts ns data-processing-time to runtimeMs", () => {
    const out = convertStatsRowsToBaseline({
      rows: [makeRow({ operatorId: "a", dataProcessingTime: 2_000_000_000 })], // 2 seconds
      workflowName: "w",
      executionName: null,
      generatedAt: "x",
    });
    expect(out!.operators[0].runtimeMs).toBe(2000);
  });

  it("derives throughput as outputRows / runtimeSeconds when both > 0", () => {
    const out = convertStatsRowsToBaseline({
      rows: [
        makeRow({ operatorId: "a", dataProcessingTime: 1_000_000_000, outputTupleCount: 5000 }),
      ],
      workflowName: "w",
      executionName: null,
      generatedAt: "x",
    });
    expect(out!.operators[0].throughputRowsPerSec).toBe(5000);
  });

  it("derives idleRatio as idle / (data + control + idle)", () => {
    const out = convertStatsRowsToBaseline({
      rows: [
        makeRow({
          operatorId: "a",
          dataProcessingTime: 1,
          controlProcessingTime: 1,
          idleTime: 2,
        }),
      ],
      workflowName: "w",
      executionName: null,
      generatedAt: "x",
    });
    expect(out!.operators[0].idleRatio).toBe(0.5);
  });

  it("emits null for runtime / throughput / idleRatio when there is no measurable work", () => {
    const out = convertStatsRowsToBaseline({
      rows: [makeRow({ operatorId: "a" })], // all zero
      workflowName: "w",
      executionName: null,
      generatedAt: "x",
    });
    const op = out!.operators[0];
    expect(op.runtimeMs).toBeNull();
    expect(op.throughputRowsPerSec).toBeNull();
    expect(op.idleRatio).toBeNull();
  });

  it("emits null for inputSize / outputSize when 0 (treated as unmeasured)", () => {
    const out = convertStatsRowsToBaseline({
      rows: [makeRow({ operatorId: "a", inputTupleSize: 0, outputTupleSize: 0 })],
      workflowName: "w",
      executionName: null,
      generatedAt: "x",
    });
    expect(out!.operators[0].inputSize).toBeNull();
    expect(out!.operators[0].outputSize).toBeNull();
  });

  it("displayName mirrors the operatorId (backend persists no friendly name)", () => {
    const out = convertStatsRowsToBaseline({
      rows: [makeRow({ operatorId: "scan-1" })],
      workflowName: "w",
      executionName: null,
      generatedAt: "x",
    });
    expect(out!.operators[0].displayName).toBe("scan-1");
  });

  it("zero / negative metric values are clamped to 0 (defensive against wire glitches)", () => {
    const out = convertStatsRowsToBaseline({
      rows: [
        makeRow({
          operatorId: "a",
          inputTupleCount: -5 as any,
          outputTupleCount: -1 as any,
        }),
      ],
      workflowName: "w",
      executionName: null,
      generatedAt: "x",
    });
    expect(out!.operators[0].inputRows).toBe(0);
    expect(out!.operators[0].outputRows).toBe(0);
  });

  it("deduplicates multiple snapshot rows per operator end-to-end (keeps the latest cumulative totals)", () => {
    const out = convertStatsRowsToBaseline({
      rows: [
        makeRow({ operatorId: "a", timestamp: 1000, outputTupleCount: 100 }),
        makeRow({ operatorId: "a", timestamp: 2000, outputTupleCount: 500 }),
        makeRow({ operatorId: "a", timestamp: 3000, outputTupleCount: 999 }),
      ],
      workflowName: "w",
      executionName: null,
      generatedAt: "x",
    });
    expect(out!.operators).toHaveLength(1);
    expect(out!.operators[0].outputRows).toBe(999);
  });
});
