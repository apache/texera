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

import { describe, expect, test } from "bun:test";
import { fail, pass } from "./checks";
import { formatReport, summarize, type RunRecord } from "./report";

const call = (requestBytes: number, inputTokens: number, outputTokens: number) => ({
  requestBytes,
  status: 200,
  inputTokens,
  outputTokens,
});

const records: RunRecord[] = [
  { caseId: "a", run: 1, verdict: pass("ok"), calls: [call(10_000, 100, 10), call(20_000, 200, 20)], seconds: 3 },
  { caseId: "a", run: 2, verdict: fail("no"), calls: [call(30_000, 300, 30)], seconds: 2 },
  { caseId: "b", run: 1, verdict: pass("ok"), calls: [], seconds: 1 },
];

describe("summarize", () => {
  test("counts passes and averages the costs per run, by case and in total", () => {
    const { cases, total } = summarize(records);
    expect(cases.map(c => c.caseId)).toEqual(["a", "b"]);
    expect(cases[0]).toEqual({
      caseId: "a",
      passed: 1,
      runs: 2,
      modelCalls: 1.5,
      inputTokens: 300,
      outputTokens: 30,
      kbPerCall: 20,
      maxKbPerCall: 30,
    });
    // A run without model calls costs nothing and divides nothing by zero.
    expect(cases[1]).toEqual({
      caseId: "b",
      passed: 1,
      runs: 1,
      modelCalls: 0,
      inputTokens: 0,
      outputTokens: 0,
      kbPerCall: 0,
      maxKbPerCall: 0,
    });
    expect(total).toMatchObject({ passed: 2, runs: 3, modelCalls: 1, inputTokens: 200, kbPerCall: 20 });
  });

  test("has nothing to say about no runs", () => {
    expect(summarize([])).toEqual({
      cases: [],
      total: {
        caseId: "total",
        passed: 0,
        runs: 0,
        modelCalls: 0,
        inputTokens: 0,
        outputTokens: 0,
        kbPerCall: 0,
        maxKbPerCall: 0,
      },
    });
  });
});

describe("formatReport", () => {
  test("prints one aligned row per case, the total, and the score", () => {
    const lines = formatReport(summarize(records)).split("\n");
    expect(lines[0]).toMatch(/^Case\s+Passed\s+Model calls\s+Input tokens\s+Output tokens\s+KB per call\s+Max KB$/);
    expect(lines[2]).toMatch(/^a\s+1\/2\s+1\.5\s+300\s+30\s+20\.0\s+30\.0$/);
    expect(lines[5]).toMatch(/^total\s+2\/3\s+1\.0\s+200\s+20\s+20\.0\s+30\.0$/);
    expect(lines.at(-1)).toBe("Score: 2 of 3 runs passed (67%). Costs are per run, averaged.");
    expect(
      new Set(
        lines
          .slice(0, 6)
          .filter(l => l.length > 0)
          .map(l => l.length)
      ).size
    ).toBe(1);
  });

  test("scores no runs as zero", () => {
    expect(formatReport(summarize([])).split("\n").at(-1)).toBe(
      "Score: 0 of 0 runs passed (0%). Costs are per run, averaged."
    );
  });
});
