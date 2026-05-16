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
import { ProfilerEntry, ProfilerState } from "./profiler.service";
import { buildProfilerSnapshot, BuildSnapshotInput } from "./profiler-snapshot";

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
  enabled?: boolean;
  view?: ProfilerState["view"];
  hotThresholdPercentile?: number;
  scores?: Record<string, ProfilerEntry>;
  baseline?: ProfilerState["baseline"];
  types?: Record<string, string>;
  displayNames?: Record<string, string>;
  upstreams?: Record<string, string[]>;
  downstreams?: Record<string, string[]>;
  now?: () => Date;
}

function makeInput(o: BuildInputOverrides = {}): BuildSnapshotInput {
  const state: ProfilerState = {
    enabled: o.enabled ?? true,
    view: o.view ?? "runtime",
    hotThresholdPercentile: o.hotThresholdPercentile ?? 80,
    scores: o.scores ?? {},
    baseline: o.baseline,
  };
  return {
    state,
    operatorType: id => o.types?.[id],
    displayName: id => o.displayNames?.[id] ?? id,
    upstreamOps: id => o.upstreams?.[id] ?? [],
    downstreamOps: id => o.downstreams?.[id] ?? [],
    now: o.now,
  };
}

describe("buildProfilerSnapshot", () => {
  it("returns undefined when profiling is disabled", () => {
    expect(buildProfilerSnapshot(makeInput({ enabled: false }))).toBeUndefined();
  });

  it("returns a snapshot with header reflecting the current state", () => {
    const snapshot = buildProfilerSnapshot(
      makeInput({
        enabled: true,
        view: "throughput",
        hotThresholdPercentile: 90,
        scores: { a: entry(0.5), b: entry(0.2) },
        now: () => new Date("2026-05-15T17:30:00Z"),
      })
    );
    expect(snapshot).toBeDefined();
    expect(snapshot!.header.enabled).toBe(true);
    expect(snapshot!.header.view).toBe("throughput");
    expect(snapshot!.header.hotThresholdPercentile).toBe(90);
    expect(snapshot!.header.operatorCount).toBe(2);
    expect(snapshot!.header.generatedAt).toBe("2026-05-15T17:30:00.000Z");
  });

  it("sorts operators by score descending with displayName as tiebreaker", () => {
    const snapshot = buildProfilerSnapshot(
      makeInput({
        scores: {
          a: entry(0.5),
          b: entry(0.5),
          c: entry(0.9),
        },
        displayNames: { a: "Zoo", b: "Apple", c: "Bird" },
      })
    );
    expect(snapshot!.operators.map(o => o.displayName)).toEqual(["Bird", "Apple", "Zoo"]);
  });

  it("derives runtime, throughput, idle ratio from raw nanosecond stats", () => {
    const snapshot = buildProfilerSnapshot(
      makeInput({
        scores: {
          a: entry(
            1.0,
            stat({
              aggregatedDataProcessingTime: 2_000_000_000,
              aggregatedOutputRowCount: 1_000,
              aggregatedIdleTime: 1_000_000_000,
              aggregatedControlProcessingTime: 0,
            })
          ),
        },
      })
    );
    const op = snapshot!.operators[0];
    expect(op.runtimeMs).toBe(2_000);
    expect(op.throughputRowsPerSec).toBe(500);
    // idle / (data + ctrl + idle) = 1e9 / 3e9 = 0.333...
    expect(op.idleRatio).toBeCloseTo(0.333, 2);
  });

  it("only includes operators that fired hints in hintsByOperator", () => {
    const snapshot = buildProfilerSnapshot(
      makeInput({
        scores: {
          scan: entry(0.3, stat({ aggregatedOutputRowCount: 100_000 })),
          filter: entry(0.5, stat({ aggregatedInputRowCount: 1_000 })),
        },
        types: { scan: "CSVScan", filter: "Filter" },
        upstreams: { filter: ["scan"] },
        downstreams: { scan: ["filter"] },
      })
    );
    const opsWithHints = snapshot!.hintsByOperator.map(h => h.operatorId);
    expect(opsWithHints).toContain("filter"); // UPSTREAM_OVERPRODUCTION fires
    expect(opsWithHints).not.toContain("scan");
  });

  it("omits baseline section when no baseline is loaded", () => {
    const snapshot = buildProfilerSnapshot(
      makeInput({ scores: { a: entry(1.0) } })
    );
    expect(snapshot!.baseline).toBeUndefined();
  });

  it("includes baseline section with deltas when a baseline is loaded", () => {
    const baseline = {
      header: {
        workflowName: "Prev",
        executionName: "run-1",
        generatedAt: "2026-05-14T12:00:00Z",
        view: "runtime",
        hotThresholdPercentile: 80,
        operatorCount: 1,
      },
      operators: [
        {
          operatorId: "a",
          displayName: "Python UDF",
          operatorType: "PythonUDFV2",
          score: 0.5,
          runtimeMs: 2000,
          throughputRowsPerSec: 500,
          inputRows: 100,
          outputRows: 100,
          inputSize: null,
          outputSize: null,
          workers: 1,
          idleRatio: null,
        },
      ],
    };
    const snapshot = buildProfilerSnapshot(
      makeInput({
        scores: {
          a: entry(
            1.0,
            stat({ aggregatedDataProcessingTime: 1_000_000_000, aggregatedOutputRowCount: 100 })
          ),
        },
        baseline,
      })
    );
    expect(snapshot!.baseline).toBeDefined();
    expect(snapshot!.baseline!.header.workflowName).toBe("Prev");
    expect(snapshot!.baseline!.deltas).toHaveLength(1);
    const delta = snapshot!.baseline!.deltas[0];
    expect(delta.operatorId).toBe("a");
    expect(delta.matchStatus).toBe("matched");
    // current runtime 1000 ms - baseline 2000 ms = -1000 ms (improved)
    expect(delta.runtimeMsDelta).toBe(-1000);
    expect(delta.direction).toBe("improved");
  });

  it("produces JSON-serializable output (no Date objects, no functions)", () => {
    const snapshot = buildProfilerSnapshot(
      makeInput({
        scores: { a: entry(1.0, stat({ aggregatedDataProcessingTime: 100 })) },
        now: () => new Date("2026-05-15T12:00:00Z"),
      })
    );
    // Should round-trip cleanly through JSON.stringify / JSON.parse.
    const roundTripped = JSON.parse(JSON.stringify(snapshot));
    expect(roundTripped).toEqual(snapshot);
  });

  it("returns an empty operators array when there are no scores", () => {
    const snapshot = buildProfilerSnapshot(makeInput({ scores: {} }));
    expect(snapshot!.operators).toEqual([]);
    expect(snapshot!.hintsByOperator).toEqual([]);
    expect(snapshot!.header.operatorCount).toBe(0);
  });
});
