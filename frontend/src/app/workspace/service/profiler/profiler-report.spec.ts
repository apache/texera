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
import { ProfilerEntry } from "./profiler.service";
import {
  buildReport,
  formatFilenameTimestamp,
  ReportInput,
  slugifyForFilename,
} from "./profiler-report";

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

function entry(score: number, s: OperatorStatistics = stat()): ProfilerEntry {
  return { score, state: s.operatorState, stats: s };
}

interface BuildInputOverrides {
  scores?: Record<string, ProfilerEntry>;
  types?: Record<string, string>;
  displayNames?: Record<string, string>;
  upstreams?: Record<string, string[]>;
  downstreams?: Record<string, string[]>;
  topN?: number;
  workflowName?: string;
  executionName?: string;
  view?: ReportInput["view"];
  hotThresholdPercentile?: number;
  generatedAt?: Date;
}

function buildInput(o: BuildInputOverrides = {}): ReportInput {
  return {
    workflowName: o.workflowName ?? "My Workflow",
    executionName: o.executionName,
    generatedAt: o.generatedAt ?? new Date("2026-05-14T12:00:00Z"),
    view: o.view ?? "runtime",
    hotThresholdPercentile: o.hotThresholdPercentile ?? 80,
    scores: o.scores ?? {},
    topN: o.topN,
    operatorType: id => o.types?.[id],
    displayName: id => o.displayNames?.[id] ?? id,
    upstreamOps: id => o.upstreams?.[id] ?? [],
    downstreamOps: id => o.downstreams?.[id] ?? [],
  };
}

describe("buildReport — header", () => {
  it("captures workflowName, view, threshold, operator count, generatedAt", () => {
    const report = buildReport(
      buildInput({
        workflowName: "TikTok Analysis",
        view: "throughput",
        hotThresholdPercentile: 90,
        scores: {
          a: entry(1.0),
          b: entry(0.5),
        },
        generatedAt: new Date("2026-05-14T17:30:00Z"),
      })
    );
    expect(report.json.header.workflowName).toBe("TikTok Analysis");
    expect(report.json.header.view).toBe("throughput");
    expect(report.json.header.hotThresholdPercentile).toBe(90);
    expect(report.json.header.operatorCount).toBe(2);
    expect(report.json.header.generatedAt).toBe("2026-05-14T17:30:00.000Z");
  });

  it("represents missing executionName as null in JSON", () => {
    const report = buildReport(buildInput({}));
    expect(report.json.header.executionName).toBeNull();
  });

  it("uses '(unnamed)' in markdown when executionName missing", () => {
    const report = buildReport(buildInput({}));
    expect(report.markdown).toContain("(unnamed)");
  });
});

describe("buildReport — top hot operators", () => {
  it("sorts descending by score and assigns ranks 1..N", () => {
    const report = buildReport(
      buildInput({
        scores: {
          slow: entry(0.4),
          medium: entry(0.6),
          hot: entry(0.9),
        },
      })
    );
    const ranks = report.json.topHotOperators.map(o => o.operatorId);
    expect(ranks).toEqual(["hot", "medium", "slow"]);
    expect(report.json.topHotOperators[0].rank).toBe(1);
    expect(report.json.topHotOperators[2].rank).toBe(3);
  });

  it("breaks score ties by displayName for deterministic output", () => {
    const report = buildReport(
      buildInput({
        scores: {
          alpha: entry(0.5),
          beta: entry(0.5),
          gamma: entry(0.5),
        },
        displayNames: { alpha: "Zoo", beta: "Apple", gamma: "Mango" },
      })
    );
    const order = report.json.topHotOperators.map(o => o.displayName);
    expect(order).toEqual(["Apple", "Mango", "Zoo"]);
  });

  it("caps the top section at the requested topN (default 5)", () => {
    const scores: Record<string, ProfilerEntry> = {};
    for (let i = 0; i < 10; i++) {
      scores[`op-${i}`] = entry(i / 10);
    }
    const defaultReport = buildReport(buildInput({ scores }));
    expect(defaultReport.json.topHotOperators).toHaveLength(5);

    const top3 = buildReport(buildInput({ scores, topN: 3 }));
    expect(top3.json.topHotOperators).toHaveLength(3);
  });

  it("emits topN=0 as empty top section without crashing", () => {
    const report = buildReport(
      buildInput({ scores: { a: entry(1.0) }, topN: 0 })
    );
    expect(report.json.topHotOperators).toHaveLength(0);
    expect(report.markdown).toContain("Top 0 hottest operators");
  });

  it("converts runtime ns to ms and computes throughput", () => {
    // 2 second runtime, 1000 output rows -> 500 rows/s, 2000 ms
    const report = buildReport(
      buildInput({
        scores: {
          a: entry(
            1.0,
            stat({ aggregatedDataProcessingTime: 2_000_000_000, aggregatedOutputRowCount: 1_000 })
          ),
        },
      })
    );
    const op = report.json.topHotOperators[0];
    expect(op.runtimeMs).toBe(2_000);
    expect(op.throughputRowsPerSec).toBe(500);
  });

  it("leaves runtimeMs / throughput as null when unmeasurable", () => {
    const report = buildReport(
      buildInput({ scores: { a: entry(0, stat({ aggregatedDataProcessingTime: 0 })) } })
    );
    const op = report.json.topHotOperators[0];
    expect(op.runtimeMs).toBeNull();
    expect(op.throughputRowsPerSec).toBeNull();
  });

  it("computes idle ratio when timing fields are present", () => {
    const report = buildReport(
      buildInput({
        scores: {
          a: entry(
            0.5,
            stat({
              aggregatedDataProcessingTime: 100,
              aggregatedControlProcessingTime: 100,
              aggregatedIdleTime: 300,
              operatorState: OperatorState.Running,
            })
          ),
        },
      })
    );
    expect(report.json.topHotOperators[0].idleRatio).toBeCloseTo(0.6, 5);
  });

  it("leaves idleRatio as null when no timing data is available", () => {
    const report = buildReport(buildInput({ scores: { a: entry(0) } }));
    expect(report.json.topHotOperators[0].idleRatio).toBeNull();
  });
});

describe("buildReport — hints", () => {
  it("includes only operators that produced hints", () => {
    // Scan→Filter where scan emits 100k and filter keeps 1k → UPSTREAM_OVERPRODUCTION on the filter.
    const report = buildReport(
      buildInput({
        scores: {
          scan: entry(0.3, stat({ aggregatedOutputRowCount: 100_000 })),
          filter: entry(0.5, stat({ aggregatedInputRowCount: 1_000 })),
        },
        types: { scan: "CSVScan", filter: "Filter" },
        displayNames: { scan: "Tweet Source", filter: "Recent Filter" },
        upstreams: { filter: ["scan"] },
      })
    );
    const ops = report.json.hintsByOperator.map(h => h.operatorId);
    expect(ops).toContain("filter");
    expect(ops).not.toContain("scan");
  });

  it("uses displayName (not operatorID) when rendering hint messages", () => {
    const report = buildReport(
      buildInput({
        scores: {
          "Scan-op-abc": entry(0.3, stat({ aggregatedOutputRowCount: 100_000 })),
          "Filter-op-xyz": entry(0.5, stat({ aggregatedInputRowCount: 1_000 })),
        },
        displayNames: { "Scan-op-abc": "Tweet Source", "Filter-op-xyz": "Recent Filter" },
        upstreams: { "Filter-op-xyz": ["Scan-op-abc"] },
      })
    );
    const filterHints = report.json.hintsByOperator.find(h => h.operatorId === "Filter-op-xyz");
    expect(filterHints?.hints[0].message).toContain("Tweet Source");
    expect(filterHints?.hints[0].message).not.toContain("Scan-op-abc");
  });

  it("emits an empty hintsByOperator + helpful markdown when nothing fires", () => {
    const report = buildReport(
      buildInput({ scores: { a: entry(0.1, stat({ aggregatedDataProcessingTime: 1 })) } })
    );
    expect(report.json.hintsByOperator).toHaveLength(0);
    expect(report.markdown).toContain("No optimization hints fired");
  });
});

describe("buildReport — raw appendix", () => {
  it("contains every operator in score-sorted order", () => {
    const report = buildReport(
      buildInput({
        scores: {
          a: entry(0.2),
          b: entry(0.8),
          c: entry(0.5),
        },
      })
    );
    expect(report.json.operators.map(o => o.operatorId)).toEqual(["b", "c", "a"]);
  });

  it("includes operators not in the topN block", () => {
    const scores: Record<string, ProfilerEntry> = {};
    for (let i = 0; i < 8; i++) scores[`op-${i}`] = entry(i / 8);
    const report = buildReport(buildInput({ scores, topN: 3 }));
    expect(report.json.topHotOperators).toHaveLength(3);
    expect(report.json.operators).toHaveLength(8);
  });
});

describe("buildReport — markdown integrity", () => {
  it("includes the expected sections in order", () => {
    const report = buildReport(
      buildInput({
        scores: { a: entry(1.0, stat({ aggregatedDataProcessingTime: 1 })) },
        workflowName: "Header Check",
      })
    );
    const headerIdx = report.markdown.indexOf("# Profiler report — Header Check");
    const topIdx = report.markdown.indexOf("## Top 1 hottest operator");
    const hintsIdx = report.markdown.indexOf("## Optimization hints");
    const rawIdx = report.markdown.indexOf("## All operators (raw appendix)");
    expect(headerIdx).toBeGreaterThanOrEqual(0);
    expect(topIdx).toBeGreaterThan(headerIdx);
    expect(hintsIdx).toBeGreaterThan(topIdx);
    expect(rawIdx).toBeGreaterThan(hintsIdx);
  });

  it("escapes pipe and newline in operator names so the table is well-formed", () => {
    const report = buildReport(
      buildInput({
        scores: { weird: entry(1.0) },
        displayNames: { weird: "evil|name\nwith\rstuff" },
      })
    );
    // pipe must be escaped; raw newline must be replaced with a space.
    expect(report.markdown).toContain("evil\\|name with stuff");
    expect(report.markdown).not.toMatch(/evil\|name\nwith/);
  });

  it("renders all six numeric columns of the table header", () => {
    const report = buildReport(buildInput({ scores: { a: entry(1.0) } }));
    expect(report.markdown).toContain(
      "| # | Operator | Type | Score | Runtime (ms) | Throughput (rows/s) | In rows | Out rows | Workers | Idle ratio |"
    );
  });

  it("uses '—' placeholder when a metric is null", () => {
    const report = buildReport(
      buildInput({ scores: { a: entry(0, stat({ aggregatedDataProcessingTime: 0 })) } })
    );
    // The table row should contain at least one en-dash for runtime/throughput/idle null cases.
    expect(report.markdown).toMatch(/\|\s*—\s*\|/);
  });
});

describe("filename helpers", () => {
  it("slugifies workflow names into safe filename chunks", () => {
    expect(slugifyForFilename("My TikTok Analysis!")).toBe("my-tiktok-analysis");
    expect(slugifyForFilename("  spaces   ")).toBe("spaces");
    expect(slugifyForFilename("UPPER-CASE")).toBe("upper-case");
    expect(slugifyForFilename("!!!")).toBe("workflow"); // fallback when nothing usable
    expect(slugifyForFilename("中文名前")).toBe("workflow");
  });

  it("formats date as colon-free ISO chunk for filenames", () => {
    expect(formatFilenameTimestamp(new Date("2026-05-14T17:30:42.123Z"))).toBe(
      "2026-05-14T17-30-42"
    );
  });
});

describe("buildReport — empty state", () => {
  it("does not throw with no scores and produces a coherent skeleton", () => {
    const report = buildReport(buildInput({}));
    expect(report.json.topHotOperators).toEqual([]);
    expect(report.json.hintsByOperator).toEqual([]);
    expect(report.json.operators).toEqual([]);
    expect(report.markdown).toContain("No operators have stats yet");
    expect(report.markdown).toContain("No optimization hints fired");
  });
});
