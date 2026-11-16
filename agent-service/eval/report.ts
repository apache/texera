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

import type { Verdict } from "./checks";
import type { ModelCall } from "./llm-recorder";

/** One run of one case. */
export interface RunRecord {
  caseId: string;
  run: number;
  verdict: Verdict;
  /** The model calls the agent made during the run. */
  calls: ModelCall[];
  seconds: number;
}

/** A case's runs, with the costs averaged per run. */
export interface CaseSummary {
  caseId: string;
  passed: number;
  runs: number;
  modelCalls: number;
  inputTokens: number;
  outputTokens: number;
  /** The average size of one request, in KB (1,000 bytes). */
  kbPerCall: number;
  /** The largest request of any run, in KB. */
  maxKbPerCall: number;
}

function summarizeRuns(caseId: string, runs: RunRecord[]): CaseSummary {
  const calls = runs.flatMap(r => r.calls);
  const perRun = (n: number) => (runs.length === 0 ? 0 : n / runs.length);
  const sum = (pick: (c: ModelCall) => number) => calls.reduce((n, c) => n + pick(c), 0);
  return {
    caseId,
    passed: runs.filter(r => r.verdict.pass).length,
    runs: runs.length,
    modelCalls: perRun(calls.length),
    inputTokens: perRun(sum(c => c.inputTokens)),
    outputTokens: perRun(sum(c => c.outputTokens)),
    kbPerCall: calls.length === 0 ? 0 : sum(c => c.requestBytes) / calls.length / 1000,
    maxKbPerCall: calls.reduce((max, c) => Math.max(max, c.requestBytes / 1000), 0),
  };
}

/** One summary per case, in the order the cases first ran, and one for all runs together. */
export function summarize(records: RunRecord[]): { cases: CaseSummary[]; total: CaseSummary } {
  const ids = [...new Set(records.map(r => r.caseId))];
  return {
    cases: ids.map(id =>
      summarizeRuns(
        id,
        records.filter(r => r.caseId === id)
      )
    ),
    total: summarizeRuns("total", records),
  };
}

export function formatReport(summary: { cases: CaseSummary[]; total: CaseSummary }): string {
  const header = ["Case", "Passed", "Model calls", "Input tokens", "Output tokens", "KB per call", "Max KB"];
  const row = (s: CaseSummary) => [
    s.caseId,
    `${s.passed}/${s.runs}`,
    s.modelCalls.toFixed(1),
    Math.round(s.inputTokens).toLocaleString("en-US"),
    Math.round(s.outputTokens).toLocaleString("en-US"),
    s.kbPerCall.toFixed(1),
    s.maxKbPerCall.toFixed(1),
  ];
  const rows = [header, ...summary.cases.map(row), row(summary.total)];
  const widths = header.map((_, i) => Math.max(...rows.map(r => r[i].length)));
  const line = (cells: string[]) =>
    cells.map((cell, i) => (i === 0 ? cell.padEnd(widths[i]) : cell.padStart(widths[i]))).join("  ");
  const rule = widths.map(w => "-".repeat(w)).join("  ");
  const { passed, runs } = summary.total;
  const score = runs === 0 ? 0 : Math.round((100 * passed) / runs);
  return [
    line(header),
    rule,
    ...summary.cases.map(s => line(row(s))),
    rule,
    line(row(summary.total)),
    "",
    `Score: ${passed} of ${runs} runs passed (${score}%). Costs are per run, averaged.`,
  ].join("\n");
}
