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
import { computeHintsForOperator, HintContext, HintRuleId } from "./profiler-hints";

function stat(partial: Partial<OperatorStatistics> = {}): OperatorStatistics {
  return {
    operatorState: OperatorState.Completed,
    aggregatedInputRowCount: 0,
    inputPortMetrics: {},
    aggregatedOutputRowCount: 0,
    outputPortMetrics: {},
    ...partial,
  };
}

interface BuildCtxInput {
  stats?: Record<string, OperatorStatistics>;
  scores?: Record<string, number>;
  hotThreshold?: number;
  types?: Record<string, string>;
  displayNames?: Record<string, string>;
  upstreams?: Record<string, string[]>;
  downstreams?: Record<string, string[]>;
}

function buildCtx(input: BuildCtxInput): HintContext {
  return {
    stats: input.stats ?? {},
    scores: input.scores ?? {},
    hotThreshold: input.hotThreshold ?? 0.8,
    operatorType: id => input.types?.[id],
    displayName: id => input.displayNames?.[id] ?? id,
    upstreamOps: id => input.upstreams?.[id] ?? [],
    downstreamOps: id => input.downstreams?.[id] ?? [],
  };
}

function ruleIds(opId: string, ctx: HintContext): HintRuleId[] {
  return computeHintsForOperator(opId, ctx).map(h => h.ruleId);
}

describe("computeHintsForOperator", () => {
  it("returns empty list when operator has no stats entry", () => {
    expect(computeHintsForOperator("missing", buildCtx({}))).toEqual([]);
  });

  it("returns hints in deterministic (alphabetical-by-ruleId) order", () => {
    // Construct a context where multiple rules fire simultaneously.
    const ctx = buildCtx({
      stats: {
        scan: stat({ aggregatedOutputRowCount: 2_000_000, operatorState: OperatorState.Running }),
        join: stat({
          aggregatedInputRowCount: 2_000_000,
          aggregatedOutputRowCount: 100,
          aggregatedDataProcessingTime: 10_000,
          numWorkers: 1,
          operatorState: OperatorState.Running,
        }),
        peer: stat({ aggregatedDataProcessingTime: 1000 }),
      },
      scores: { join: 1.0 },
      types: { scan: "CSVScan", join: "HashJoin" },
      upstreams: { join: ["scan"] },
    });
    const ids = ruleIds("join", ctx);
    const sorted = [...ids].sort((a, b) => a.localeCompare(b));
    expect(ids).toEqual(sorted);
  });

  describe("JOIN_HIGH_FANIN_LOW_FANOUT", () => {
    it("fires when a Join keeps <5% of its input", () => {
      const ctx = buildCtx({
        stats: { j: stat({ aggregatedInputRowCount: 1000, aggregatedOutputRowCount: 10 }) },
        types: { j: "HashJoin" },
      });
      expect(ruleIds("j", ctx)).toContain("JOIN_HIGH_FANIN_LOW_FANOUT");
    });

    it("does not fire when output is >=5% of input", () => {
      const ctx = buildCtx({
        stats: { j: stat({ aggregatedInputRowCount: 1000, aggregatedOutputRowCount: 100 }) },
        types: { j: "HashJoin" },
      });
      expect(ruleIds("j", ctx)).not.toContain("JOIN_HIGH_FANIN_LOW_FANOUT");
    });

    it("does not fire for non-join operator types", () => {
      const ctx = buildCtx({
        stats: { f: stat({ aggregatedInputRowCount: 1000, aggregatedOutputRowCount: 10 }) },
        types: { f: "Filter" },
      });
      expect(ruleIds("f", ctx)).not.toContain("JOIN_HIGH_FANIN_LOW_FANOUT");
    });

    it("does not fire when the join has not consumed any rows yet", () => {
      const ctx = buildCtx({
        stats: { j: stat({ aggregatedInputRowCount: 0, aggregatedOutputRowCount: 0 }) },
        types: { j: "HashJoin" },
      });
      expect(ruleIds("j", ctx)).not.toContain("JOIN_HIGH_FANIN_LOW_FANOUT");
    });
  });

  describe("UPSTREAM_OVERPRODUCTION", () => {
    it("fires when an upstream emits >10× what this op consumes", () => {
      const ctx = buildCtx({
        stats: {
          src: stat({ aggregatedOutputRowCount: 100_000 }),
          downstream: stat({ aggregatedInputRowCount: 1_000 }),
        },
        upstreams: { downstream: ["src"] },
      });
      expect(ruleIds("downstream", ctx)).toContain("UPSTREAM_OVERPRODUCTION");
    });

    it("does not fire when input/output ratios are reasonable", () => {
      const ctx = buildCtx({
        stats: {
          src: stat({ aggregatedOutputRowCount: 1_000 }),
          downstream: stat({ aggregatedInputRowCount: 1_000 }),
        },
        upstreams: { downstream: ["src"] },
      });
      expect(ruleIds("downstream", ctx)).not.toContain("UPSTREAM_OVERPRODUCTION");
    });

    it("does not fire when there are no upstream operators", () => {
      const ctx = buildCtx({
        stats: { onlyOp: stat({ aggregatedInputRowCount: 1_000 }) },
      });
      expect(ruleIds("onlyOp", ctx)).not.toContain("UPSTREAM_OVERPRODUCTION");
    });

    it("uses displayName (not raw operator id) in the hint message", () => {
      const ctx = buildCtx({
        stats: {
          "Scan-operator-abc123": stat({ aggregatedOutputRowCount: 100_000 }),
          "Filter-operator-xyz789": stat({ aggregatedInputRowCount: 1_000 }),
        },
        displayNames: {
          "Scan-operator-abc123": "Tweet Source",
          "Filter-operator-xyz789": "Recent Filter",
        },
        upstreams: { "Filter-operator-xyz789": ["Scan-operator-abc123"] },
      });
      const hints = computeHintsForOperator("Filter-operator-xyz789", ctx);
      const overproduce = hints.find(h => h.ruleId === "UPSTREAM_OVERPRODUCTION");
      expect(overproduce).toBeDefined();
      expect(overproduce!.message).toContain("Tweet Source");
      expect(overproduce!.message).toContain("Recent Filter");
      // Negative: the message should NOT contain the raw internal ids.
      expect(overproduce!.message).not.toContain("Scan-operator-abc123");
      expect(overproduce!.message).not.toContain("Filter-operator-xyz789");
    });

    it("falls back to raw id in messages when no displayName is mapped", () => {
      const ctx = buildCtx({
        stats: {
          src: stat({ aggregatedOutputRowCount: 100_000 }),
          downstream: stat({ aggregatedInputRowCount: 1_000 }),
        },
        upstreams: { downstream: ["src"] },
        // no displayNames map
      });
      const hints = computeHintsForOperator("downstream", ctx);
      const overproduce = hints.find(h => h.ruleId === "UPSTREAM_OVERPRODUCTION");
      expect(overproduce!.message).toContain("src");
      expect(overproduce!.message).toContain("downstream");
    });
  });

  describe("RUNTIME_OUTLIER", () => {
    it("fires when an operator runs >3× the median", () => {
      const ctx = buildCtx({
        stats: {
          a: stat({ aggregatedDataProcessingTime: 100 }),
          b: stat({ aggregatedDataProcessingTime: 100 }),
          c: stat({ aggregatedDataProcessingTime: 100 }),
          bottleneck: stat({ aggregatedDataProcessingTime: 1000 }),
        },
      });
      expect(ruleIds("bottleneck", ctx)).toContain("RUNTIME_OUTLIER");
    });

    it("does not fire for operators within the median band", () => {
      const ctx = buildCtx({
        stats: {
          a: stat({ aggregatedDataProcessingTime: 100 }),
          b: stat({ aggregatedDataProcessingTime: 100 }),
          c: stat({ aggregatedDataProcessingTime: 200 }),
        },
      });
      expect(ruleIds("c", ctx)).not.toContain("RUNTIME_OUTLIER");
    });

    it("does not fire when there are fewer than 2 timed peers", () => {
      const ctx = buildCtx({
        stats: { only: stat({ aggregatedDataProcessingTime: 1_000_000 }) },
      });
      expect(ruleIds("only", ctx)).not.toContain("RUNTIME_OUTLIER");
    });

    it("does not fire when this operator has no measured runtime", () => {
      const ctx = buildCtx({
        stats: {
          slow: stat({ aggregatedDataProcessingTime: 0 }),
          a: stat({ aggregatedDataProcessingTime: 100 }),
          b: stat({ aggregatedDataProcessingTime: 100 }),
        },
      });
      expect(ruleIds("slow", ctx)).not.toContain("RUNTIME_OUTLIER");
    });
  });

  describe("LOW_PARALLELISM_HOT_OP", () => {
    it("fires when a hot operator runs with a single worker", () => {
      const ctx = buildCtx({
        stats: { hot: stat({ numWorkers: 1, aggregatedDataProcessingTime: 1000 }) },
        scores: { hot: 0.9 },
        hotThreshold: 0.8,
      });
      expect(ruleIds("hot", ctx)).toContain("LOW_PARALLELISM_HOT_OP");
    });

    it("does not fire when the operator is not hot", () => {
      const ctx = buildCtx({
        stats: { warm: stat({ numWorkers: 1 }) },
        scores: { warm: 0.4 },
        hotThreshold: 0.8,
      });
      expect(ruleIds("warm", ctx)).not.toContain("LOW_PARALLELISM_HOT_OP");
    });

    it("does not fire when worker count is >1, even for hot operators", () => {
      const ctx = buildCtx({
        stats: { hot: stat({ numWorkers: 4 }) },
        scores: { hot: 1.0 },
      });
      expect(ruleIds("hot", ctx)).not.toContain("LOW_PARALLELISM_HOT_OP");
    });
  });

  describe("IDLE_HEAVY", () => {
    it("fires when a running op is idle >70% of the time", () => {
      const ctx = buildCtx({
        stats: {
          waiting: stat({
            operatorState: OperatorState.Running,
            aggregatedDataProcessingTime: 100,
            aggregatedControlProcessingTime: 100,
            aggregatedIdleTime: 800,
          }),
        },
      });
      expect(ruleIds("waiting", ctx)).toContain("IDLE_HEAVY");
    });

    it("does not fire when the operator is not running", () => {
      const ctx = buildCtx({
        stats: {
          done: stat({
            operatorState: OperatorState.Completed,
            aggregatedIdleTime: 800,
            aggregatedDataProcessingTime: 100,
          }),
        },
      });
      expect(ruleIds("done", ctx)).not.toContain("IDLE_HEAVY");
    });

    it("does not fire when idle ratio is below threshold", () => {
      const ctx = buildCtx({
        stats: {
          busy: stat({
            operatorState: OperatorState.Running,
            aggregatedDataProcessingTime: 700,
            aggregatedControlProcessingTime: 100,
            aggregatedIdleTime: 200,
          }),
        },
      });
      expect(ruleIds("busy", ctx)).not.toContain("IDLE_HEAVY");
    });

    it("does not fire when all timing fields are missing/zero", () => {
      const ctx = buildCtx({
        stats: { fresh: stat({ operatorState: OperatorState.Running }) },
      });
      expect(ruleIds("fresh", ctx)).not.toContain("IDLE_HEAVY");
    });
  });

  describe("SCAN_FULL_TABLE_NO_FILTER", () => {
    it("fires when a large scan has no filter immediately downstream", () => {
      const ctx = buildCtx({
        stats: {
          scan: stat({ aggregatedOutputRowCount: 2_000_000 }),
          downstream: stat({}),
        },
        types: { scan: "CSVFileScan", downstream: "Projection" },
        downstreams: { scan: ["downstream"] },
      });
      expect(ruleIds("scan", ctx)).toContain("SCAN_FULL_TABLE_NO_FILTER");
    });

    it("does not fire when a Filter operator is directly downstream", () => {
      const ctx = buildCtx({
        stats: {
          scan: stat({ aggregatedOutputRowCount: 2_000_000 }),
          flt: stat({}),
        },
        types: { scan: "CSVFileScan", flt: "Filter" },
        downstreams: { scan: ["flt"] },
      });
      expect(ruleIds("scan", ctx)).not.toContain("SCAN_FULL_TABLE_NO_FILTER");
    });

    it("does not fire for small scans", () => {
      const ctx = buildCtx({
        stats: { scan: stat({ aggregatedOutputRowCount: 10 }) },
        types: { scan: "CSVFileScan" },
      });
      expect(ruleIds("scan", ctx)).not.toContain("SCAN_FULL_TABLE_NO_FILTER");
    });

    it("does not fire for non-scan operators", () => {
      const ctx = buildCtx({
        stats: { agg: stat({ aggregatedOutputRowCount: 2_000_000 }) },
        types: { agg: "Aggregate" },
      });
      expect(ruleIds("agg", ctx)).not.toContain("SCAN_FULL_TABLE_NO_FILTER");
    });
  });
});
