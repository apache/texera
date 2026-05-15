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

import { OperatorState, OperatorStatistics } from "../../types/execute-workflow.interface";
import {
  BaselineReport,
  ComparableOperator,
  computeAllDeltas,
  computeDeltaIntensity,
  computeOperatorDelta,
  indexBaseline,
  maxAbsRuntimeDelta,
  parseBaselineReport,
  statsToComparable,
} from "./profiler-delta";

function op(partial: Partial<ComparableOperator> & { operatorId: string }): ComparableOperator {
  return {
    displayName: partial.operatorId,
    operatorType: null,
    score: 0,
    runtimeMs: null,
    throughputRowsPerSec: null,
    inputRows: 0,
    outputRows: 0,
    inputSize: null,
    outputSize: null,
    workers: null,
    idleRatio: null,
    ...partial,
  };
}

describe("computeOperatorDelta — matched", () => {
  it("computes runtime/throughput/row/score deltas as current minus baseline", () => {
    const current = op({
      operatorId: "a",
      runtimeMs: 1000,
      throughputRowsPerSec: 500,
      inputRows: 100,
      outputRows: 100,
      score: 0.5,
    });
    const baseline = op({
      operatorId: "a",
      runtimeMs: 2000,
      throughputRowsPerSec: 250,
      inputRows: 80,
      outputRows: 80,
      score: 0.7,
    });
    const d = computeOperatorDelta("a", current, baseline);
    expect(d.matchStatus).toBe("matched");
    expect(d.runtimeMsDelta).toBe(-1000);
    expect(d.throughputRowsPerSecDelta).toBe(250);
    expect(d.outputRowsDelta).toBe(20);
    expect(d.inputRowsDelta).toBe(20);
    expect(d.scoreDelta).toBeCloseTo(-0.2, 5);
  });

  it('returns direction "improved" when runtime drops by more than 5%', () => {
    const d = computeOperatorDelta(
      "a",
      op({ operatorId: "a", runtimeMs: 500 }),
      op({ operatorId: "a", runtimeMs: 1000 })
    );
    expect(d.direction).toBe("improved");
  });

  it('returns direction "regressed" when runtime rises by more than 5%', () => {
    const d = computeOperatorDelta(
      "a",
      op({ operatorId: "a", runtimeMs: 2000 }),
      op({ operatorId: "a", runtimeMs: 1000 })
    );
    expect(d.direction).toBe("regressed");
  });

  it('returns direction "unchanged" when runtime change is within 5%', () => {
    const d = computeOperatorDelta(
      "a",
      op({ operatorId: "a", runtimeMs: 1020 }),
      op({ operatorId: "a", runtimeMs: 1000 })
    );
    expect(d.direction).toBe("unchanged");
  });

  it('returns direction "unchanged" when absolute delta is under 1ms', () => {
    const d = computeOperatorDelta(
      "a",
      op({ operatorId: "a", runtimeMs: 0.5 }),
      op({ operatorId: "a", runtimeMs: 0 })
    );
    expect(d.direction).toBe("unchanged");
  });

  it("falls back to outputRowsDelta when runtime is missing on one side", () => {
    const current = op({ operatorId: "a", runtimeMs: null, outputRows: 200 });
    const baseline = op({ operatorId: "a", runtimeMs: null, outputRows: 100 });
    const d = computeOperatorDelta("a", current, baseline);
    expect(d.runtimeMsDelta).toBeNull();
    expect(d.outputRowsDelta).toBe(100);
    expect(d.direction).toBe("improved"); // more output flowed -> progress
  });

  it("preserves displayName from the current run when matched", () => {
    const current = op({ operatorId: "a", displayName: "New Name" });
    const baseline = op({ operatorId: "a", displayName: "Old Name" });
    expect(computeOperatorDelta("a", current, baseline).displayName).toBe("New Name");
  });
});

describe("computeOperatorDelta — unmatched", () => {
  it('marks new-in-current when baseline is missing', () => {
    const d = computeOperatorDelta("a", op({ operatorId: "a", runtimeMs: 100 }), undefined);
    expect(d.matchStatus).toBe("new-in-current");
    expect(d.runtimeMsDelta).toBeNull();
    expect(d.direction).toBe("n/a");
    expect(d.current).toBeDefined();
    expect(d.baseline).toBeUndefined();
  });

  it('marks removed-since-baseline when current is missing', () => {
    const d = computeOperatorDelta("a", undefined, op({ operatorId: "a", runtimeMs: 100 }));
    expect(d.matchStatus).toBe("removed-since-baseline");
    expect(d.direction).toBe("n/a");
    expect(d.current).toBeUndefined();
    expect(d.baseline).toBeDefined();
  });
});

describe("computeAllDeltas", () => {
  it("returns one entry per id across both maps", () => {
    const current = { a: op({ operatorId: "a" }), b: op({ operatorId: "b" }) };
    const baseline = { b: op({ operatorId: "b" }), c: op({ operatorId: "c" }) };
    const all = computeAllDeltas(current, baseline);
    expect(Object.keys(all).sort()).toEqual(["a", "b", "c"]);
    expect(all["a"].matchStatus).toBe("new-in-current");
    expect(all["b"].matchStatus).toBe("matched");
    expect(all["c"].matchStatus).toBe("removed-since-baseline");
  });

  it("returns an empty map when both inputs are empty", () => {
    expect(computeAllDeltas({}, {})).toEqual({});
  });
});

describe("parseBaselineReport", () => {
  it("returns undefined for null / non-object inputs", () => {
    expect(parseBaselineReport(null)).toBeUndefined();
    expect(parseBaselineReport(undefined)).toBeUndefined();
    expect(parseBaselineReport(42)).toBeUndefined();
    expect(parseBaselineReport("string")).toBeUndefined();
  });

  it("returns undefined when operators array is missing", () => {
    expect(parseBaselineReport({ header: {} })).toBeUndefined();
  });

  it("returns undefined when operators array yields no parseable entries", () => {
    expect(parseBaselineReport({ operators: [{ noId: true }, "garbage"] })).toBeUndefined();
  });

  it("parses a valid report and exposes the operators", () => {
    const result = parseBaselineReport({
      header: {
        workflowName: "TikTok Pipeline",
        executionName: "run-1",
        generatedAt: "2026-05-14T12:00:00Z",
        view: "runtime",
        hotThresholdPercentile: 80,
        operatorCount: 1,
      },
      operators: [
        {
          operatorId: "op-1",
          displayName: "Python UDF",
          operatorType: "PythonUDFV2",
          score: 0.97,
          runtimeMs: 2710.8,
          throughputRowsPerSec: 1714,
          inputRows: 4645,
          outputRows: 4645,
          inputSize: 21628576,
          outputSize: 23790952,
          workers: 1,
          idleRatio: 0.64,
        },
      ],
    });
    expect(result?.operators).toHaveLength(1);
    expect(result?.operators[0].displayName).toBe("Python UDF");
    expect(result?.header.workflowName).toBe("TikTok Pipeline");
  });

  it("backfills sane defaults when fields are partially missing on an operator entry", () => {
    const result = parseBaselineReport({
      operators: [{ operatorId: "lonely-op" }],
    });
    expect(result?.operators[0].displayName).toBe("lonely-op"); // falls back to id
    expect(result?.operators[0].score).toBe(0);
    expect(result?.operators[0].runtimeMs).toBeNull();
    expect(result?.operators[0].inputRows).toBe(0);
  });

  it("falls back to a synthesized header when the header is missing", () => {
    const result = parseBaselineReport({
      operators: [{ operatorId: "x" }],
    });
    expect(result?.header.workflowName).toContain("uploaded baseline");
  });

  it("skips garbage entries and keeps valid ones", () => {
    const result = parseBaselineReport({
      operators: [
        { operatorId: "a" },
        { noId: "garbage" },
        null,
        42,
        { operatorId: "b" },
      ],
    });
    expect(result?.operators.map(o => o.operatorId)).toEqual(["a", "b"]);
  });
});

describe("statsToComparable", () => {
  function rawStats(partial: Partial<OperatorStatistics> = {}): OperatorStatistics {
    return {
      operatorState: OperatorState.Completed,
      aggregatedInputRowCount: 0,
      inputPortMetrics: {},
      aggregatedOutputRowCount: 0,
      outputPortMetrics: {},
      ...partial,
    };
  }

  it("derives runtimeMs and throughput from raw nanosecond fields", () => {
    const out = statsToComparable({
      operatorId: "a",
      displayName: "Python UDF",
      operatorType: "PythonUDFV2",
      score: 0.5,
      stats: rawStats({
        aggregatedDataProcessingTime: 2_000_000_000,
        aggregatedOutputRowCount: 1000,
      }),
    });
    expect(out.runtimeMs).toBe(2_000);
    expect(out.throughputRowsPerSec).toBe(500);
  });

  it("returns null for runtimeMs / throughput when unmeasurable", () => {
    const out = statsToComparable({
      operatorId: "a",
      displayName: "a",
      operatorType: null,
      score: 0,
      stats: rawStats({}),
    });
    expect(out.runtimeMs).toBeNull();
    expect(out.throughputRowsPerSec).toBeNull();
  });

  it("computes idle ratio from data + control + idle nanos", () => {
    const out = statsToComparable({
      operatorId: "a",
      displayName: "a",
      operatorType: null,
      score: 0,
      stats: rawStats({
        aggregatedDataProcessingTime: 100,
        aggregatedControlProcessingTime: 100,
        aggregatedIdleTime: 300,
      }),
    });
    expect(out.idleRatio).toBeCloseTo(0.6, 5);
  });

  it("normalizes undefined operatorType to null", () => {
    const out = statsToComparable({
      operatorId: "a",
      displayName: "a",
      operatorType: undefined,
      score: 0,
      stats: rawStats(),
    });
    expect(out.operatorType).toBeNull();
  });
});

describe("maxAbsRuntimeDelta", () => {
  it("returns 0 for an empty map", () => {
    expect(maxAbsRuntimeDelta({})).toBe(0);
  });

  it("returns the largest absolute runtime delta, ignoring sign", () => {
    const a = computeOperatorDelta("a", op({ operatorId: "a", runtimeMs: 100 }), op({ operatorId: "a", runtimeMs: 500 }));
    const b = computeOperatorDelta("b", op({ operatorId: "b", runtimeMs: 700 }), op({ operatorId: "b", runtimeMs: 200 }));
    expect(maxAbsRuntimeDelta({ a, b })).toBe(500); // both -400 and +500, max abs is 500
  });

  it("skips operators with null runtime delta", () => {
    const a = computeOperatorDelta("a", op({ operatorId: "a" }), op({ operatorId: "a" }));
    expect(maxAbsRuntimeDelta({ a })).toBe(0);
  });
});

describe("computeDeltaIntensity", () => {
  it("returns negative intensity for improved operators (lower runtime)", () => {
    const d = computeOperatorDelta(
      "a",
      op({ operatorId: "a", runtimeMs: 500 }),
      op({ operatorId: "a", runtimeMs: 1000 })
    );
    expect(computeDeltaIntensity(d, 500)).toBeLessThan(0);
  });

  it("returns positive intensity for regressed operators (higher runtime)", () => {
    const d = computeOperatorDelta(
      "a",
      op({ operatorId: "a", runtimeMs: 1500 }),
      op({ operatorId: "a", runtimeMs: 1000 })
    );
    expect(computeDeltaIntensity(d, 500)).toBeGreaterThan(0);
  });

  it("returns 0 for unchanged operators (within the 5% / 1ms band)", () => {
    const d = computeOperatorDelta(
      "a",
      op({ operatorId: "a", runtimeMs: 1020 }),
      op({ operatorId: "a", runtimeMs: 1000 })
    );
    // direction is "unchanged" for this 2% change
    expect(computeDeltaIntensity(d, 100)).toBe(0);
  });

  it('returns 0 for "new-in-current" and "removed-since-baseline" operators', () => {
    const newOp = computeOperatorDelta("a", op({ operatorId: "a", runtimeMs: 500 }), undefined);
    const removedOp = computeOperatorDelta("a", undefined, op({ operatorId: "a", runtimeMs: 500 }));
    expect(computeDeltaIntensity(newOp, 500)).toBe(0);
    expect(computeDeltaIntensity(removedOp, 500)).toBe(0);
  });

  it("clamps intensity to [-1, 1]", () => {
    const huge = computeOperatorDelta(
      "a",
      op({ operatorId: "a", runtimeMs: 10_000 }),
      op({ operatorId: "a", runtimeMs: 1_000 })
    );
    // delta is +9000 ms; with maxAbs=1 we'd otherwise overshoot.
    expect(computeDeltaIntensity(huge, 1)).toBe(1);
  });

  it("returns 0 when maxAbsDeltaMs is 0", () => {
    const d = computeOperatorDelta(
      "a",
      op({ operatorId: "a", runtimeMs: 500 }),
      op({ operatorId: "a", runtimeMs: 1000 })
    );
    expect(computeDeltaIntensity(d, 0)).toBe(0);
  });

  it("returns 0 when runtimeMsDelta is null (e.g. one side missing runtime)", () => {
    const d = computeOperatorDelta(
      "a",
      op({ operatorId: "a", runtimeMs: null }),
      op({ operatorId: "a", runtimeMs: 1000 })
    );
    expect(computeDeltaIntensity(d, 100)).toBe(0);
  });
});

describe("indexBaseline", () => {
  it("indexes operators by operatorId", () => {
    const baseline: BaselineReport = {
      header: {
        workflowName: "x",
        executionName: null,
        generatedAt: "",
        view: "runtime",
        hotThresholdPercentile: 80,
        operatorCount: 2,
      },
      operators: [op({ operatorId: "a" }), op({ operatorId: "b" })],
    };
    const idx = indexBaseline(baseline);
    expect(idx["a"].operatorId).toBe("a");
    expect(idx["b"].operatorId).toBe("b");
    expect(idx["nope"]).toBeUndefined();
  });
});
