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
 * Pure builder that converts a ProfilerService state snapshot + workflow-graph
 * context into a downloadable run report in two formats: Markdown (human-readable)
 * and JSON (machine-readable).
 *
 * Kept dependency-free (no Angular, no RxJS, no DOM) so the entire builder is
 * unit-testable with synthetic inputs and so the JSON shape is documented in code.
 */

import { OperatorStatistics } from "../../types/execute-workflow.interface";
import { ProfilerEntry, ProfilerView } from "./profiler.service";
import { computeHintsForOperator, Hint, HintContext } from "./profiler-hints";
import { formatViewLabel } from "./profiler-hover";

export interface ReportInput {
  readonly workflowName: string;
  readonly executionName: string | undefined;
  readonly generatedAt: Date;
  readonly view: ProfilerView;
  readonly hotThresholdPercentile: number;
  readonly scores: Readonly<Record<string, ProfilerEntry>>;
  readonly operatorType: (opId: string) => string | undefined;
  readonly displayName: (opId: string) => string;
  readonly upstreamOps: (opId: string) => readonly string[];
  readonly downstreamOps: (opId: string) => readonly string[];
  /** Number of hot operators to show in the headline section. Defaults to 5. */
  readonly topN?: number;
}

export interface ReportHeader {
  readonly workflowName: string;
  readonly executionName: string | null;
  readonly generatedAt: string; // ISO 8601
  readonly view: ProfilerView;
  readonly hotThresholdPercentile: number;
  readonly operatorCount: number;
}

export interface ReportTopOperator {
  readonly rank: number;
  readonly operatorId: string;
  readonly displayName: string;
  readonly operatorType: string | null;
  readonly score: number;
  readonly runtimeMs: number | null;
  readonly throughputRowsPerSec: number | null;
  readonly inputRows: number;
  readonly outputRows: number;
  readonly inputSize: number | null;
  readonly outputSize: number | null;
  readonly workers: number | null;
  readonly idleRatio: number | null;
}

export interface ReportHintEntry {
  readonly operatorId: string;
  readonly displayName: string;
  readonly hints: readonly Hint[];
}

export interface ReportJson {
  readonly header: ReportHeader;
  readonly topHotOperators: readonly ReportTopOperator[];
  readonly hintsByOperator: readonly ReportHintEntry[];
  readonly operators: readonly ReportTopOperator[]; // full appendix, same shape minus rank semantics
}

export interface Report {
  readonly markdown: string;
  readonly json: ReportJson;
}

const DEFAULT_TOP_N = 5;

/**
 * Build a complete profiler report. Pure: same inputs always yield the same output
 * (modulo the `generatedAt` timestamp the caller provides).
 */
export function buildReport(input: ReportInput): Report {
  const opIds = Object.keys(input.scores);

  // Sort operators by score descending, break ties by displayName for deterministic output.
  const sortedIds = [...opIds].sort((a, b) => {
    const sa = input.scores[a].score;
    const sb = input.scores[b].score;
    if (sb !== sa) return sb - sa;
    return input.displayName(a).localeCompare(input.displayName(b));
  });

  const allOperators: ReportTopOperator[] = sortedIds.map((opId, idx) =>
    toReportOperator(opId, idx + 1, input)
  );

  const topN = input.topN ?? DEFAULT_TOP_N;
  const topHotOperators = allOperators.slice(0, Math.max(0, topN));

  // Build hint context once, reuse across operators.
  const stats: Record<string, OperatorStatistics> = {};
  const scoreMap: Record<string, number> = {};
  for (const id of opIds) {
    stats[id] = input.scores[id].stats;
    scoreMap[id] = input.scores[id].score;
  }
  const hintCtx: HintContext = {
    stats,
    scores: scoreMap,
    hotThreshold: input.hotThresholdPercentile / 100,
    operatorType: input.operatorType,
    displayName: input.displayName,
    upstreamOps: input.upstreamOps,
    downstreamOps: input.downstreamOps,
  };

  const hintsByOperator: ReportHintEntry[] = [];
  for (const opId of sortedIds) {
    const hints = computeHintsForOperator(opId, hintCtx);
    if (hints.length === 0) continue;
    hintsByOperator.push({
      operatorId: opId,
      displayName: input.displayName(opId),
      hints,
    });
  }

  const header: ReportHeader = {
    workflowName: input.workflowName,
    executionName: input.executionName ?? null,
    generatedAt: input.generatedAt.toISOString(),
    view: input.view,
    hotThresholdPercentile: input.hotThresholdPercentile,
    operatorCount: opIds.length,
  };

  const json: ReportJson = {
    header,
    topHotOperators,
    hintsByOperator,
    operators: allOperators,
  };

  const markdown = renderMarkdown(header, topHotOperators, hintsByOperator, allOperators);

  return { markdown, json };
}

/**
 * Produce a filesystem-safe slug for use in the downloaded filename.
 * Example: `"My TikTok Analysis!"` → `"my-tiktok-analysis"`.
 */
export function slugifyForFilename(name: string): string {
  const cleaned = name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return cleaned.length > 0 ? cleaned : "workflow";
}

/**
 * Format the ISO timestamp portion of the filename — `2026-05-14T17-30-00`
 * (colons replaced so the name is safe on every OS).
 */
export function formatFilenameTimestamp(date: Date): string {
  return date.toISOString().replace(/[:]/g, "-").replace(/\..+$/, "");
}

// ---------------- internals ----------------

function toReportOperator(opId: string, rank: number, input: ReportInput): ReportTopOperator {
  const entry = input.scores[opId];
  const s = entry.stats;
  const runtimeNs = s.aggregatedDataProcessingTime;
  const runtimeMs = runtimeNs && runtimeNs > 0 ? runtimeNs / 1_000_000 : null;
  const outRows = s.aggregatedOutputRowCount ?? 0;
  const throughputRowsPerSec =
    runtimeNs && runtimeNs > 0 && outRows > 0 ? outRows / (runtimeNs / 1_000_000_000) : null;

  const dataNs = s.aggregatedDataProcessingTime ?? 0;
  const ctrlNs = s.aggregatedControlProcessingTime ?? 0;
  const idleNs = s.aggregatedIdleTime ?? 0;
  const totalNs = dataNs + ctrlNs + idleNs;
  const idleRatio = totalNs > 0 ? idleNs / totalNs : null;

  return {
    rank,
    operatorId: opId,
    displayName: input.displayName(opId),
    operatorType: input.operatorType(opId) ?? null,
    score: entry.score,
    runtimeMs,
    throughputRowsPerSec,
    inputRows: s.aggregatedInputRowCount ?? 0,
    outputRows: outRows,
    inputSize: s.aggregatedInputSize ?? null,
    outputSize: s.aggregatedOutputSize ?? null,
    workers: s.numWorkers ?? null,
    idleRatio,
  };
}

function renderMarkdown(
  header: ReportHeader,
  top: readonly ReportTopOperator[],
  hintsByOperator: readonly ReportHintEntry[],
  all: readonly ReportTopOperator[]
): string {
  const lines: string[] = [];
  lines.push(`# Profiler report — ${escapeMd(header.workflowName)}`);
  lines.push("");
  lines.push(`- **Execution:** ${escapeMd(header.executionName ?? "(unnamed)")}`);
  lines.push(`- **Generated at:** ${header.generatedAt}`);
  lines.push(`- **View:** ${formatViewLabel(header.view)}`);
  lines.push(`- **Hot threshold:** ${header.hotThresholdPercentile}th percentile`);
  lines.push(`- **Operators with stats:** ${header.operatorCount}`);
  lines.push("");

  lines.push(`## Top ${top.length} hottest operator${top.length === 1 ? "" : "s"}`);
  lines.push("");
  if (top.length === 0) {
    lines.push("_No operators have stats yet._");
  } else {
    lines.push(operatorTableHeader());
    for (const op of top) {
      lines.push(operatorTableRow(op));
    }
  }
  lines.push("");

  lines.push("## Optimization hints");
  lines.push("");
  if (hintsByOperator.length === 0) {
    lines.push("_No optimization hints fired across the workflow._");
  } else {
    for (const entry of hintsByOperator) {
      lines.push(`### ${escapeMd(entry.displayName)}`);
      lines.push("");
      for (const hint of entry.hints) {
        lines.push(`- **${hint.ruleId}** (${hint.severity}): ${hint.message}`);
      }
      lines.push("");
    }
  }

  lines.push("## All operators (raw appendix)");
  lines.push("");
  if (all.length === 0) {
    lines.push("_No operators have stats yet._");
  } else {
    lines.push(operatorTableHeader());
    for (const op of all) {
      lines.push(operatorTableRow(op));
    }
  }
  lines.push("");

  return lines.join("\n");
}

function operatorTableHeader(): string {
  return [
    "| # | Operator | Type | Score | Runtime (ms) | Throughput (rows/s) | In rows | Out rows | Workers | Idle ratio |",
    "|---|---|---|---|---|---|---|---|---|---|",
  ].join("\n");
}

function operatorTableRow(op: ReportTopOperator): string {
  return `| ${op.rank} | ${escapeMd(op.displayName)} | ${op.operatorType ?? "—"} | ${op.score.toFixed(2)} | ${formatNumOrDash(op.runtimeMs, 1)} | ${formatNumOrDash(op.throughputRowsPerSec, 0)} | ${op.inputRows.toLocaleString()} | ${op.outputRows.toLocaleString()} | ${op.workers ?? "—"} | ${formatNumOrDash(op.idleRatio, 2)} |`;
}

function formatNumOrDash(n: number | null, fractionDigits: number): string {
  if (n === null || !Number.isFinite(n)) return "—";
  return n.toLocaleString(undefined, {
    minimumFractionDigits: fractionDigits,
    maximumFractionDigits: fractionDigits,
  });
}

/**
 * Escape pipe and any kind of newline (LF / CR / CRLF) so a free-text operator name
 * doesn't break a markdown table row.
 */
function escapeMd(text: string): string {
  return text.replace(/\|/g, "\\|").replace(/[\r\n]+/g, " ");
}
