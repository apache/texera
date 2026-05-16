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

import { tool } from "ai";
import { z } from "zod";
import { createErrorResult, createToolResult } from "./tools-utility";

/**
 * Phase 1 of the profiler-agent-tool plan: five read-only tools that let the agent
 * answer questions like "why is my workflow slow?" by inspecting the per-message
 * profiler snapshot the frontend ships alongside each chat message.
 *
 * All snapshot math (scores, hints, baseline deltas) is pre-computed on the
 * frontend (`profiler-snapshot.ts`). These tools just slice / filter / sort.
 *
 * The snapshot is passed in via a getter callback rather than a direct value so
 * the tool instances created once at agent boot always read the *current*
 * snapshot — `TexeraAgent` updates the underlying field per-message in
 * `sendMessage`.
 */

export const TOOL_NAME_GET_PROFILER_SUMMARY = "getProfilerSummary";
export const TOOL_NAME_LIST_HOT_OPERATORS = "listHotOperators";
export const TOOL_NAME_GET_OPERATOR_METRICS = "getOperatorMetrics";
export const TOOL_NAME_GET_OPTIMIZATION_HINTS = "getOptimizationHints";
export const TOOL_NAME_COMPARE_TO_BASELINE = "compareToBaseline";

const NO_DATA_MSG =
  "No profiler data available. Ask the user to turn on the Profiler heatmap (gauge icon in the run-bar) and re-run the workflow, then try again.";

/**
 * Snapshot shape — defensively-typed mirror of the frontend's `ProfilerSnapshot`.
 * Fields that may be missing are `unknown` here so we can validate at the read site.
 */
interface ParsedSnapshot {
  header: {
    enabled: boolean;
    view: string;
    hotThresholdPercentile: number;
    operatorCount: number;
    generatedAt: string;
  };
  operators: ParsedOperator[];
  hintsByOperator: ParsedHintEntry[];
  baseline?: {
    header: {
      workflowName: string;
      executionName: string | null;
      generatedAt: string;
    };
    deltas: ParsedDelta[];
  };
}

interface ParsedOperator {
  operatorId: string;
  displayName: string;
  operatorType: string | null;
  score: number;
  runtimeMs: number | null;
  throughputRowsPerSec: number | null;
  inputRows: number;
  outputRows: number;
  inputSize: number | null;
  outputSize: number | null;
  workers: number | null;
  idleRatio: number | null;
}

interface ParsedHintEntry {
  operatorId: string;
  displayName: string;
  hints: { ruleId: string; severity: string; message: string }[];
}

interface ParsedDelta {
  operatorId: string;
  displayName: string;
  matchStatus: string;
  direction: string;
  runtimeMsDelta: number | null;
  throughputRowsPerSecDelta: number | null;
  outputRowsDelta: number | null;
  inputRowsDelta: number | null;
  scoreDelta: number | null;
}

/**
 * Defensive parse of the raw snapshot blob. Returns `undefined` for any shape
 * we don't recognize — caller surfaces NO_DATA_MSG.
 */
export function parseSnapshot(raw: unknown): ParsedSnapshot | undefined {
  if (!raw || typeof raw !== "object") return undefined;
  const obj = raw as Record<string, unknown>;
  const header = obj.header as Record<string, unknown> | undefined;
  const operators = obj.operators;
  if (!header || !Array.isArray(operators)) return undefined;
  return raw as ParsedSnapshot;
}

export function createGetProfilerSummaryTool(getSnapshot: () => unknown) {
  return tool({
    description:
      "Returns a one-shot overview of the profiler state for the current workflow run: " +
      "current view (runtime/throughput/io-imbalance/delta), hot-threshold percentile, " +
      "operator count, the single hottest operator (id + display name + score), " +
      "the total number of fired optimization hints, and whether a baseline is loaded for comparison. " +
      "Call this first to know whether profiler data is even available before drilling into specifics.",
    inputSchema: z.object({}),
    execute: async () => {
      const snap = parseSnapshot(getSnapshot());
      if (!snap) return createToolResult(NO_DATA_MSG);
      const hottest = snap.operators[0];
      const totalRuntimeMs = snap.operators.reduce(
        (sum, op) => sum + (op.runtimeMs ?? 0),
        0
      );
      const totalHints = snap.hintsByOperator.reduce((sum, h) => sum + h.hints.length, 0);
      return createToolResult(
        JSON.stringify({
          enabled: snap.header.enabled,
          view: snap.header.view,
          hotThresholdPercentile: snap.header.hotThresholdPercentile,
          operatorCount: snap.header.operatorCount,
          totalRuntimeMs,
          hintsCount: totalHints,
          baselineLoaded: !!snap.baseline,
          baselineWorkflow: snap.baseline?.header.workflowName ?? null,
          hottestOperator: hottest
            ? {
                operatorId: hottest.operatorId,
                displayName: hottest.displayName,
                operatorType: hottest.operatorType,
                score: hottest.score,
                runtimeMs: hottest.runtimeMs,
              }
            : null,
          generatedAt: snap.header.generatedAt,
        })
      );
    },
  });
}

export function createListHotOperatorsTool(getSnapshot: () => unknown) {
  return tool({
    description:
      "Returns the top-N hottest operators (sorted by heat score descending). " +
      "Default N = 5. Each entry includes full per-operator metrics: " +
      "score, runtimeMs, throughputRowsPerSec, inputRows, outputRows, inputSize, outputSize, " +
      "workers, idleRatio. Use this when the user asks 'what's slow' or 'which operators are the bottleneck'.",
    inputSchema: z.object({
      limit: z
        .number()
        .int()
        .min(1)
        .max(50)
        .optional()
        .describe("Max operators to return (default 5, max 50)."),
    }),
    execute: async args => {
      const snap = parseSnapshot(getSnapshot());
      if (!snap) return createToolResult(NO_DATA_MSG);
      const n = args.limit ?? 5;
      const top = snap.operators.slice(0, n);
      return createToolResult(JSON.stringify(top));
    },
  });
}

export function createGetOperatorMetricsTool(getSnapshot: () => unknown) {
  return tool({
    description:
      "Returns the full per-operator metrics for a single operator id. " +
      "Use this when the user asks about a specific operator by id or display name shown in a prior tool result. " +
      "Returns an error message if the operator is not in the snapshot.",
    inputSchema: z.object({
      operatorId: z.string().describe("The exact operatorId (not the display name)."),
    }),
    execute: async args => {
      const snap = parseSnapshot(getSnapshot());
      if (!snap) return createToolResult(NO_DATA_MSG);
      const op = snap.operators.find(o => o.operatorId === args.operatorId);
      if (!op) {
        return createErrorResult(
          `Operator '${args.operatorId}' is not in the profiler snapshot. Use listHotOperators to see available operator ids.`
        );
      }
      return createToolResult(JSON.stringify(op));
    },
  });
}

export function createGetOptimizationHintsTool(getSnapshot: () => unknown) {
  return tool({
    description:
      "Returns the optimization hints fired by the profiler rule engine. " +
      "When 'operatorId' is provided, returns only hints for that operator; otherwise returns all hints across the workflow. " +
      "Each hint has a ruleId (SCAN_FULL_TABLE_NO_FILTER, UPSTREAM_OVERPRODUCTION, " +
      "JOIN_HIGH_FANIN_LOW_FANOUT, RUNTIME_OUTLIER, IDLE_HEAVY, LOW_PARALLELISM_HOT_OP), " +
      "a severity (warning/info), and a human-readable message. " +
      "Use this to explain *why* an operator is hot and what the engine recommends.",
    inputSchema: z.object({
      operatorId: z
        .string()
        .optional()
        .describe("If set, return only hints for this operator. Otherwise return all hints."),
    }),
    execute: async args => {
      const snap = parseSnapshot(getSnapshot());
      if (!snap) return createToolResult(NO_DATA_MSG);
      const filtered = args.operatorId
        ? snap.hintsByOperator.filter(h => h.operatorId === args.operatorId)
        : snap.hintsByOperator;
      if (filtered.length === 0) {
        return createToolResult(
          args.operatorId
            ? `No optimization hints fired for operator '${args.operatorId}'.`
            : "No optimization hints fired across the workflow."
        );
      }
      return createToolResult(JSON.stringify(filtered));
    },
  });
}

export function createCompareToBaselineTool(getSnapshot: () => unknown) {
  return tool({
    description:
      "Returns per-operator deltas (current run vs the user's uploaded baseline run). " +
      "Each delta includes matchStatus (matched/new-in-current/removed-since-baseline), " +
      "direction (improved/regressed/unchanged/n/a), and signed numeric deltas for " +
      "runtimeMs, throughputRowsPerSec, outputRows, inputRows, scoreDelta. " +
      "Returns a no-data message if the user hasn't uploaded a baseline. " +
      "When 'operatorId' is set, returns only that operator's delta; otherwise returns all.",
    inputSchema: z.object({
      operatorId: z
        .string()
        .optional()
        .describe("If set, return only this operator's delta. Otherwise return all deltas."),
    }),
    execute: async args => {
      const snap = parseSnapshot(getSnapshot());
      if (!snap) return createToolResult(NO_DATA_MSG);
      if (!snap.baseline) {
        return createToolResult(
          "No baseline loaded. Ask the user to upload a previously-downloaded JSON profiler report via the profiler popover's 'Compare to previous run' section to enable run-vs-run comparison."
        );
      }
      const deltas = args.operatorId
        ? snap.baseline.deltas.filter(d => d.operatorId === args.operatorId)
        : snap.baseline.deltas;
      if (deltas.length === 0 && args.operatorId) {
        return createErrorResult(
          `Operator '${args.operatorId}' is not in the baseline or current snapshot.`
        );
      }
      return createToolResult(
        JSON.stringify({
          baselineWorkflow: snap.baseline.header.workflowName,
          baselineExecution: snap.baseline.header.executionName,
          baselineGeneratedAt: snap.baseline.header.generatedAt,
          deltas,
        })
      );
    },
  });
}

/**
 * Convenience factory — builds all five profiler tools given a single getter that
 * always returns the current snapshot. Mirrors the create-X-tool pattern used by
 * the workflow CRUD tools.
 */
export function createProfilerTools(getSnapshot: () => unknown): Record<string, any> {
  return {
    [TOOL_NAME_GET_PROFILER_SUMMARY]: createGetProfilerSummaryTool(getSnapshot),
    [TOOL_NAME_LIST_HOT_OPERATORS]: createListHotOperatorsTool(getSnapshot),
    [TOOL_NAME_GET_OPERATOR_METRICS]: createGetOperatorMetricsTool(getSnapshot),
    [TOOL_NAME_GET_OPTIMIZATION_HINTS]: createGetOptimizationHintsTool(getSnapshot),
    [TOOL_NAME_COMPARE_TO_BASELINE]: createCompareToBaselineTool(getSnapshot),
  };
}
