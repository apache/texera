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
import { HintContext } from "./profiler-hints";
import { computeSuggestions, edgeSuggestionId } from "./profiler-suggestions";

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

function buildCtx(input: BuildCtxInput = {}): HintContext {
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

describe("computeSuggestions — SCAN_FULL_TABLE_NO_FILTER", () => {
  it("creates a ghost between a large scan and its first non-filter downstream", () => {
    const ctx = buildCtx({
      stats: {
        scan: stat({ aggregatedOutputRowCount: 5_000_000 }),
        agg: stat({}),
      },
      types: { scan: "CSVScan", agg: "Aggregate" },
      downstreams: { scan: ["agg"] },
    });
    const out = computeSuggestions(ctx);
    expect(out).toHaveLength(1);
    expect(out[0].upstreamOpId).toBe("scan");
    expect(out[0].downstreamOpId).toBe("agg");
    expect(out[0].reasonRuleId).toBe("SCAN_FULL_TABLE_NO_FILTER");
    expect(out[0].id).toBe(edgeSuggestionId("scan", "agg"));
  });

  it("does NOT fire when the immediate downstream is already a Filter", () => {
    const ctx = buildCtx({
      stats: {
        scan: stat({ aggregatedOutputRowCount: 5_000_000 }),
        flt: stat({}),
      },
      types: { scan: "CSVScan", flt: "Filter" },
      downstreams: { scan: ["flt"] },
    });
    expect(computeSuggestions(ctx)).toHaveLength(0);
  });

  it("does NOT fire for small scans", () => {
    const ctx = buildCtx({
      stats: { scan: stat({ aggregatedOutputRowCount: 100 }) },
      types: { scan: "CSVScan" },
      downstreams: { scan: ["downstream"] },
    });
    expect(computeSuggestions(ctx)).toHaveLength(0);
  });

  it("picks the first non-filter downstream when multiple downstreams exist", () => {
    const ctx = buildCtx({
      stats: {
        scan: stat({ aggregatedOutputRowCount: 5_000_000 }),
        flt: stat({}),
        agg: stat({}),
      },
      types: { scan: "CSVScan", flt: "Filter", agg: "Aggregate" },
      downstreams: { scan: ["flt", "agg"] },
    });
    // First downstream is Filter — skip — pick Aggregate.
    const out = computeSuggestions(ctx);
    expect(out).toHaveLength(1);
    expect(out[0].downstreamOpId).toBe("agg");
  });
});

describe("computeSuggestions — UPSTREAM_OVERPRODUCTION", () => {
  it("creates a ghost on the edge from over-producer to keeps-little", () => {
    const ctx = buildCtx({
      stats: {
        src: stat({ aggregatedOutputRowCount: 100_000 }),
        downstream: stat({ aggregatedInputRowCount: 1_000 }),
      },
      upstreams: { downstream: ["src"] },
    });
    const out = computeSuggestions(ctx);
    expect(out).toHaveLength(1);
    expect(out[0].upstreamOpId).toBe("src");
    expect(out[0].downstreamOpId).toBe("downstream");
    expect(out[0].reasonRuleId).toBe("UPSTREAM_OVERPRODUCTION");
  });

  it("uses display names in the reason message", () => {
    const ctx = buildCtx({
      stats: {
        src: stat({ aggregatedOutputRowCount: 100_000 }),
        downstream: stat({ aggregatedInputRowCount: 1_000 }),
      },
      displayNames: { src: "Tweet Source", downstream: "Recent Filter" },
      upstreams: { downstream: ["src"] },
    });
    const out = computeSuggestions(ctx);
    expect(out[0].reasonMessage).toContain("Tweet Source");
    expect(out[0].reasonMessage).toContain("Recent Filter");
  });

  it("does not fire when the ratio is reasonable", () => {
    const ctx = buildCtx({
      stats: {
        src: stat({ aggregatedOutputRowCount: 1_000 }),
        downstream: stat({ aggregatedInputRowCount: 1_000 }),
      },
      upstreams: { downstream: ["src"] },
    });
    expect(computeSuggestions(ctx)).toHaveLength(0);
  });
});

describe("computeSuggestions — JOIN_HIGH_FANIN_LOW_FANOUT", () => {
  it("creates a ghost on the edge from the fattest input to the join", () => {
    const ctx = buildCtx({
      stats: {
        leftIn: stat({ aggregatedOutputRowCount: 10_000 }),
        rightIn: stat({ aggregatedOutputRowCount: 100 }),
        join: stat({ aggregatedInputRowCount: 10_100, aggregatedOutputRowCount: 50 }),
      },
      types: { join: "HashJoin", leftIn: "CSVScan", rightIn: "CSVScan" },
      upstreams: { join: ["leftIn", "rightIn"] },
    });
    const out = computeSuggestions(ctx);
    expect(out.find(s => s.reasonRuleId === "JOIN_HIGH_FANIN_LOW_FANOUT")).toBeDefined();
    const joinSug = out.find(s => s.reasonRuleId === "JOIN_HIGH_FANIN_LOW_FANOUT")!;
    // Should target the fattest input (leftIn with 10k > rightIn with 100)
    expect(joinSug.upstreamOpId).toBe("leftIn");
    expect(joinSug.downstreamOpId).toBe("join");
  });

  it("does not fire when join keeps >=5% of input", () => {
    const ctx = buildCtx({
      stats: {
        leftIn: stat({ aggregatedOutputRowCount: 10_000 }),
        join: stat({ aggregatedInputRowCount: 10_000, aggregatedOutputRowCount: 1_000 }),
      },
      types: { join: "HashJoin", leftIn: "CSVScan" },
      upstreams: { join: ["leftIn"] },
    });
    expect(computeSuggestions(ctx)).toHaveLength(0);
  });
});

describe("computeSuggestions — dedup + dismissed + ordering", () => {
  it("dedupes a single edge when multiple rules suggest it", () => {
    // Build a graph where the same edge (scan → downstream) is BOTH a large-scan
    // and an overproducer-consumer pair.
    const ctx = buildCtx({
      stats: {
        scan: stat({ aggregatedOutputRowCount: 5_000_000 }),
        downstream: stat({ aggregatedInputRowCount: 1_000 }),
      },
      types: { scan: "CSVScan", downstream: "Aggregate" },
      downstreams: { scan: ["downstream"] },
      upstreams: { downstream: ["scan"] },
    });
    const out = computeSuggestions(ctx);
    // Only one ghost per edge, even though two rules apply.
    expect(out).toHaveLength(1);
  });

  it("filters out dismissed suggestion ids", () => {
    const ctx = buildCtx({
      stats: {
        scan: stat({ aggregatedOutputRowCount: 5_000_000 }),
        agg: stat({}),
      },
      types: { scan: "CSVScan", agg: "Aggregate" },
      downstreams: { scan: ["agg"] },
    });
    const dismissed = new Set<string>([edgeSuggestionId("scan", "agg")]);
    expect(computeSuggestions(ctx, dismissed)).toHaveLength(0);
  });

  it("returns suggestions in deterministic id order", () => {
    const ctx = buildCtx({
      stats: {
        scanA: stat({ aggregatedOutputRowCount: 5_000_000 }),
        scanB: stat({ aggregatedOutputRowCount: 5_000_000 }),
        agg: stat({}),
      },
      types: { scanA: "CSVScan", scanB: "CSVScan", agg: "Aggregate" },
      downstreams: { scanA: ["agg"], scanB: ["agg"] },
    });
    const out = computeSuggestions(ctx);
    const ids = out.map(s => s.id);
    const sorted = [...ids].sort((a, b) => a.localeCompare(b));
    expect(ids).toEqual(sorted);
  });

  it("returns empty list for empty stats", () => {
    expect(computeSuggestions(buildCtx({}))).toHaveLength(0);
  });

  it("ignores operators with missing stats entries", () => {
    const ctx = buildCtx({
      stats: {
        scan: stat({ aggregatedOutputRowCount: 5_000_000 }),
        // 'agg' referenced in downstreams but has no stats — should be skipped gracefully.
      },
      types: { scan: "CSVScan" },
      downstreams: { scan: ["agg"] },
    });
    // SCAN_FULL_TABLE_NO_FILTER still fires; the downstream agg doesn't need a stats entry
    // to be the target of a ghost — it just needs to exist as an operator.
    const out = computeSuggestions(ctx);
    expect(out).toHaveLength(1);
    expect(out[0].downstreamOpId).toBe("agg");
  });
});
