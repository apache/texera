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

/**
 * Pure builder for the JSON snapshot that the frontend ships to the texera-agent
 * on every chat message. The agent uses this as the input to its read-only profiler
 * tools (Phase 1 of `profiler-agent-tool-plan.md`) — `getProfilerSummary`,
 * `listHotOperators`, `getOperatorMetrics`, `getOptimizationHints`, `compareToBaseline`.
 *
 * Design notes:
 *   1. The agent-service never re-computes profiler math. We pre-compute scores,
 *      hints, and (when a baseline is loaded) deltas here so the tools just slice /
 *      filter / sort.
 *   2. The snapshot is JSON-serializable end-to-end (no Date objects, no class
 *      instances, no functions) so it can be sent over the existing WebSocket and
 *      parsed cleanly on the Bun side.
 *   3. Dependency-free pure function — no Angular, no RxJS. Caller is responsible
 *      for collecting the inputs from ProfilerService / WorkflowActionService.
 */

import type { ProfilerEntry, ProfilerState, ProfilerView } from "./profiler.service";
import type { Hint, HintContext } from "./profiler-hints";
import { computeHintsForOperator } from "./profiler-hints";
import type { BaselineReport, ComparableOperator, OperatorDelta } from "./profiler-delta";
import {
  computeOperatorDelta,
  indexBaseline,
  statsToComparable,
} from "./profiler-delta";

export interface ProfilerSnapshotHeader {
  readonly enabled: boolean;
  readonly view: ProfilerView;
  readonly hotThresholdPercentile: number;
  readonly operatorCount: number;
  readonly generatedAt: string;
}

export interface ProfilerSnapshotHintEntry {
  readonly operatorId: string;
  readonly displayName: string;
  readonly hints: readonly Hint[];
}

export interface ProfilerSnapshotBaselineHeader {
  readonly workflowName: string;
  readonly executionName: string | null;
  readonly generatedAt: string;
}

/**
 * The JSON payload shipped to agent-service. Field names are agent-tool oriented
 * (e.g. `operators` not `topHotOperators`) so the tools' read sites stay terse.
 */
export interface ProfilerSnapshot {
  readonly header: ProfilerSnapshotHeader;
  /** All operators that have profiler stats, sorted by heat score descending. */
  readonly operators: readonly ComparableOperator[];
  /** Operators that produced at least one optimization hint. */
  readonly hintsByOperator: readonly ProfilerSnapshotHintEntry[];
  /** When a baseline is loaded, deltas vs that baseline for matched operators. */
  readonly baseline?: {
    readonly header: ProfilerSnapshotBaselineHeader;
    readonly deltas: readonly OperatorDelta[];
  };
}

export interface BuildSnapshotInput {
  readonly state: ProfilerState;
  /** Same graph adapter shape the side panel uses for `HintContext`. */
  readonly operatorType: (opId: string) => string | undefined;
  readonly displayName: (opId: string) => string;
  readonly upstreamOps: (opId: string) => readonly string[];
  readonly downstreamOps: (opId: string) => readonly string[];
  /** Optional clock injection — tests pass a fixed Date for deterministic output. */
  readonly now?: () => Date;
}

/**
 * Returns `undefined` when profiling is disabled — callers should send no snapshot
 * at all in that case so the agent knows profiler data is unavailable. Otherwise
 * returns a fully-resolved ProfilerSnapshot ready to JSON.stringify.
 */
export function buildProfilerSnapshot(input: BuildSnapshotInput): ProfilerSnapshot | undefined {
  const { state } = input;
  if (!state.enabled) return undefined;

  const ctx: HintContext = {
    stats: collectStats(state),
    scores: collectScores(state),
    hotThreshold: state.hotThresholdPercentile / 100,
    operatorType: input.operatorType,
    displayName: input.displayName,
    upstreamOps: input.upstreamOps,
    downstreamOps: input.downstreamOps,
  };

  // Sort operators by score desc (tie-break by displayName for determinism).
  const sortedIds = Object.keys(state.scores).sort((a, b) => {
    const sa = state.scores[a].score;
    const sb = state.scores[b].score;
    if (sb !== sa) return sb - sa;
    return input.displayName(a).localeCompare(input.displayName(b));
  });

  const operators: ComparableOperator[] = sortedIds.map(id =>
    statsToComparable({
      operatorId: id,
      displayName: input.displayName(id),
      operatorType: input.operatorType(id),
      score: state.scores[id].score,
      stats: state.scores[id].stats,
    })
  );

  const hintsByOperator: ProfilerSnapshotHintEntry[] = [];
  for (const id of sortedIds) {
    const hints = computeHintsForOperator(id, ctx);
    if (hints.length === 0) continue;
    hintsByOperator.push({
      operatorId: id,
      displayName: input.displayName(id),
      hints,
    });
  }

  const baseline = state.baseline ? buildBaselineSection(state.baseline, operators) : undefined;

  const header: ProfilerSnapshotHeader = {
    enabled: true,
    view: state.view,
    hotThresholdPercentile: state.hotThresholdPercentile,
    operatorCount: operators.length,
    generatedAt: (input.now?.() ?? new Date()).toISOString(),
  };

  return baseline ? { header, operators, hintsByOperator, baseline } : { header, operators, hintsByOperator };
}

// ---- internals ----

function collectStats(state: ProfilerState): HintContext["stats"] {
  const stats: Record<string, ProfilerEntry["stats"]> = {};
  for (const id of Object.keys(state.scores)) {
    stats[id] = state.scores[id].stats;
  }
  return stats;
}

function collectScores(state: ProfilerState): HintContext["scores"] {
  const scores: Record<string, number> = {};
  for (const id of Object.keys(state.scores)) {
    scores[id] = state.scores[id].score;
  }
  return scores;
}

function buildBaselineSection(
  baseline: BaselineReport,
  currentOps: readonly ComparableOperator[]
): ProfilerSnapshot["baseline"] {
  const baselineIndex = indexBaseline(baseline);
  const currentIndex: Record<string, ComparableOperator> = {};
  for (const op of currentOps) currentIndex[op.operatorId] = op;

  const allIds = new Set<string>([...Object.keys(currentIndex), ...Object.keys(baselineIndex)]);
  const deltas: OperatorDelta[] = [];
  for (const id of allIds) {
    deltas.push(computeOperatorDelta(id, currentIndex[id], baselineIndex[id]));
  }

  return {
    header: {
      workflowName: baseline.header.workflowName,
      executionName: baseline.header.executionName,
      generatedAt: baseline.header.generatedAt,
    },
    deltas,
  };
}
