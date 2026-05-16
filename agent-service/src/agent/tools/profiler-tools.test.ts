/*
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

import { describe, expect, test } from "bun:test";
import {
  createCompareToBaselineTool,
  createGetOperatorMetricsTool,
  createGetOptimizationHintsTool,
  createGetProfilerSummaryTool,
  createListHotOperatorsTool,
  createProfilerTools,
  parseSnapshot,
} from "./profiler-tools";

/**
 * Minimal snapshot matching the shape produced by the frontend's `buildProfilerSnapshot`.
 * Real frontend snapshots include more fields; tests pass the minimum needed per case.
 */
function makeSnapshot(overrides: any = {}) {
  return {
    header: {
      enabled: true,
      view: "runtime",
      hotThresholdPercentile: 80,
      operatorCount: 2,
      generatedAt: "2026-05-15T12:00:00.000Z",
      ...overrides.header,
    },
    operators: overrides.operators ?? [
      {
        operatorId: "op-1",
        displayName: "Python UDF",
        operatorType: "PythonUDFV2",
        score: 0.97,
        runtimeMs: 2710,
        throughputRowsPerSec: 1714,
        inputRows: 4645,
        outputRows: 4645,
        inputSize: 21000000,
        outputSize: 23000000,
        workers: 1,
        idleRatio: 0.64,
      },
      {
        operatorId: "op-2",
        displayName: "Aggregate",
        operatorType: "Aggregate",
        score: 0.12,
        runtimeMs: 80,
        throughputRowsPerSec: 12000,
        inputRows: 4645,
        outputRows: 11,
        inputSize: null,
        outputSize: null,
        workers: 1,
        idleRatio: null,
      },
    ],
    hintsByOperator: overrides.hintsByOperator ?? [
      {
        operatorId: "op-1",
        displayName: "Python UDF",
        hints: [
          {
            ruleId: "RUNTIME_OUTLIER",
            severity: "warning",
            message: "Runtime is 7.8× the median across operators — likely the workflow bottleneck.",
          },
        ],
      },
    ],
    baseline: overrides.baseline,
  };
}

describe("parseSnapshot", () => {
  test("returns undefined for non-object inputs", () => {
    expect(parseSnapshot(undefined)).toBeUndefined();
    expect(parseSnapshot(null)).toBeUndefined();
    expect(parseSnapshot("string")).toBeUndefined();
    expect(parseSnapshot(42)).toBeUndefined();
  });

  test("returns undefined when header or operators are missing", () => {
    expect(parseSnapshot({ operators: [] })).toBeUndefined();
    expect(parseSnapshot({ header: {} })).toBeUndefined();
  });

  test("returns the snapshot when shape is recognized", () => {
    const snap = makeSnapshot();
    expect(parseSnapshot(snap)).toBe(snap);
  });
});

describe("getProfilerSummary", () => {
  test("returns NO_DATA_MSG when no snapshot is available", async () => {
    const t = createGetProfilerSummaryTool(() => undefined);
    const result = (await t.execute!({} as any, {} as any)) as string;
    expect(result).toContain("No profiler data available");
  });

  test("returns a structured summary including the hottest operator", async () => {
    const snap = makeSnapshot();
    const t = createGetProfilerSummaryTool(() => snap);
    const result = (await t.execute!({} as any, {} as any)) as string;
    const parsed = JSON.parse(result);
    expect(parsed.view).toBe("runtime");
    expect(parsed.operatorCount).toBe(2);
    expect(parsed.hintsCount).toBe(1);
    expect(parsed.baselineLoaded).toBe(false);
    expect(parsed.hottestOperator.operatorId).toBe("op-1");
    expect(parsed.hottestOperator.score).toBe(0.97);
    expect(parsed.totalRuntimeMs).toBe(2790);
  });

  test("reflects baseline-loaded state in the summary", async () => {
    const snap = makeSnapshot({
      baseline: {
        header: { workflowName: "Prev Run", executionName: null, generatedAt: "x" },
        deltas: [],
      },
    });
    const t = createGetProfilerSummaryTool(() => snap);
    const result = (await t.execute!({} as any, {} as any)) as string;
    const parsed = JSON.parse(result);
    expect(parsed.baselineLoaded).toBe(true);
    expect(parsed.baselineWorkflow).toBe("Prev Run");
  });
});

describe("listHotOperators", () => {
  test("returns top-N sorted (snapshot is pre-sorted by score desc)", async () => {
    const snap = makeSnapshot();
    const t = createListHotOperatorsTool(() => snap);
    const result = (await t.execute!({ limit: 1 } as any, {} as any)) as string;
    const parsed = JSON.parse(result);
    expect(parsed).toHaveLength(1);
    expect(parsed[0].operatorId).toBe("op-1");
  });

  test("defaults to limit=5 when no limit is provided", async () => {
    const snap = makeSnapshot();
    const t = createListHotOperatorsTool(() => snap);
    const result = (await t.execute!({} as any, {} as any)) as string;
    const parsed = JSON.parse(result);
    // snapshot only has 2; the limit just caps the slice
    expect(parsed).toHaveLength(2);
  });

  test("returns NO_DATA_MSG when no snapshot", async () => {
    const t = createListHotOperatorsTool(() => undefined);
    const result = (await t.execute!({} as any, {} as any)) as string;
    expect(result).toContain("No profiler data available");
  });
});

describe("getOperatorMetrics", () => {
  test("returns the operator when found", async () => {
    const snap = makeSnapshot();
    const t = createGetOperatorMetricsTool(() => snap);
    const result = (await t.execute!({ operatorId: "op-2" } as any, {} as any)) as string;
    const parsed = JSON.parse(result);
    expect(parsed.operatorId).toBe("op-2");
    expect(parsed.runtimeMs).toBe(80);
  });

  test("returns an error result when the operator is not found", async () => {
    const snap = makeSnapshot();
    const t = createGetOperatorMetricsTool(() => snap);
    const result = (await t.execute!({ operatorId: "missing" } as any, {} as any)) as string;
    expect(result).toContain("[ERROR]");
    expect(result).toContain("missing");
  });

  test("returns NO_DATA_MSG when no snapshot", async () => {
    const t = createGetOperatorMetricsTool(() => undefined);
    const result = (await t.execute!({ operatorId: "op-1" } as any, {} as any)) as string;
    expect(result).toContain("No profiler data available");
  });
});

describe("getOptimizationHints", () => {
  test("returns all hints when no operatorId is given", async () => {
    const snap = makeSnapshot();
    const t = createGetOptimizationHintsTool(() => snap);
    const result = (await t.execute!({} as any, {} as any)) as string;
    const parsed = JSON.parse(result);
    expect(parsed).toHaveLength(1);
    expect(parsed[0].operatorId).toBe("op-1");
  });

  test("filters to a single operator when operatorId is given", async () => {
    const snap = makeSnapshot();
    const t = createGetOptimizationHintsTool(() => snap);
    const result = (await t.execute!({ operatorId: "op-1" } as any, {} as any)) as string;
    const parsed = JSON.parse(result);
    expect(parsed).toHaveLength(1);
    expect(parsed[0].hints[0].ruleId).toBe("RUNTIME_OUTLIER");
  });

  test("returns friendly text when no hints fired for the requested operator", async () => {
    const snap = makeSnapshot();
    const t = createGetOptimizationHintsTool(() => snap);
    const result = (await t.execute!({ operatorId: "op-2" } as any, {} as any)) as string;
    expect(result).toContain("No optimization hints fired for operator 'op-2'");
  });

  test("returns friendly text when nothing fired across the workflow", async () => {
    const snap = makeSnapshot({ hintsByOperator: [] });
    const t = createGetOptimizationHintsTool(() => snap);
    const result = (await t.execute!({} as any, {} as any)) as string;
    expect(result).toContain("No optimization hints fired across the workflow");
  });
});

describe("compareToBaseline", () => {
  test("returns a friendly message when no baseline is loaded", async () => {
    const snap = makeSnapshot();
    const t = createCompareToBaselineTool(() => snap);
    const result = (await t.execute!({} as any, {} as any)) as string;
    expect(result).toContain("No baseline loaded");
  });

  test("returns all deltas + baseline header when baseline is loaded", async () => {
    const snap = makeSnapshot({
      baseline: {
        header: {
          workflowName: "Prev Run",
          executionName: "run-1",
          generatedAt: "2026-05-14T00:00:00Z",
        },
        deltas: [
          {
            operatorId: "op-1",
            displayName: "Python UDF",
            matchStatus: "matched",
            direction: "improved",
            runtimeMsDelta: -55,
            throughputRowsPerSecDelta: 400,
            outputRowsDelta: 0,
            inputRowsDelta: 0,
            scoreDelta: 0.29,
          },
        ],
      },
    });
    const t = createCompareToBaselineTool(() => snap);
    const result = (await t.execute!({} as any, {} as any)) as string;
    const parsed = JSON.parse(result);
    expect(parsed.baselineWorkflow).toBe("Prev Run");
    expect(parsed.baselineExecution).toBe("run-1");
    expect(parsed.deltas).toHaveLength(1);
    expect(parsed.deltas[0].direction).toBe("improved");
  });

  test("filters deltas by operatorId when provided", async () => {
    const snap = makeSnapshot({
      baseline: {
        header: { workflowName: "Prev", executionName: null, generatedAt: "x" },
        deltas: [
          { operatorId: "op-1", direction: "improved" } as any,
          { operatorId: "op-2", direction: "regressed" } as any,
        ],
      },
    });
    const t = createCompareToBaselineTool(() => snap);
    const result = (await t.execute!({ operatorId: "op-2" } as any, {} as any)) as string;
    const parsed = JSON.parse(result);
    expect(parsed.deltas).toHaveLength(1);
    expect(parsed.deltas[0].operatorId).toBe("op-2");
  });
});

describe("createProfilerTools (factory)", () => {
  test("returns all 5 expected tools keyed by their TOOL_NAME constants", () => {
    const tools = createProfilerTools(() => undefined);
    expect(Object.keys(tools).sort()).toEqual(
      [
        "compareToBaseline",
        "getOperatorMetrics",
        "getOptimizationHints",
        "getProfilerSummary",
        "listHotOperators",
      ].sort()
    );
  });

  test("getter is called lazily — different calls see different snapshots", async () => {
    let currentSnap: any = makeSnapshot();
    const tools = createProfilerTools(() => currentSnap);
    const summaryBefore = (await tools.getProfilerSummary.execute!({} as any, {} as any)) as string;
    expect(JSON.parse(summaryBefore).operatorCount).toBe(2);

    // Mutate underlying value — same tool instance, fresh result.
    currentSnap = makeSnapshot({ header: { operatorCount: 99 } });
    const summaryAfter = (await tools.getProfilerSummary.execute!({} as any, {} as any)) as string;
    expect(JSON.parse(summaryAfter).operatorCount).toBe(99);
  });
});
