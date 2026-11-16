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
import {
  LAST_RUN_MESSAGE_MAX_CHARS,
  LAST_RUN_MAX_ENTRIES,
  LAST_RUN_MAX_OPERATOR_STATS,
  LAST_RUN_STATE_MAX_CHARS,
  parseLastRun,
} from "./last-run";

const valid = {
  state: "Failed" as const,
  operatorErrors: [{ operatorId: "op1", message: "ValueError: bad row" }],
  workflowErrors: ["worker lost"],
};

describe("parseLastRun", () => {
  test("returns undefined for an absent field", () => {
    expect(parseLastRun(undefined)).toBeUndefined();
    expect(parseLastRun(null)).toBeUndefined();
  });

  test("accepts a well-formed report", () => {
    expect(parseLastRun(valid)).toEqual(valid);
  });

  test("accepts a clean run with empty error lists", () => {
    const clean = { state: "Completed" as const, operatorErrors: [], workflowErrors: [] };
    expect(parseLastRun(clean)).toEqual(clean);
  });

  test("accepts each state a finished run ends in", () => {
    for (const state of ["Completed", "Failed", "Killed", "Terminated"] as const) {
      expect(parseLastRun({ ...valid, state })).toEqual({ ...valid, state });
    }
  });

  test("accepts each state of a run that is still in flight", () => {
    for (const state of ["Running", "Pausing", "Paused", "Resuming", "Recovering"] as const) {
      expect(parseLastRun({ ...valid, state })).toEqual({ ...valid, state });
    }
  });

  test("rejects a run that has not started, and an unknown or missing state", () => {
    for (const state of ["Uninitialized", "Initializing", "Done", "running", "", 3, null]) {
      expect(parseLastRun({ ...valid, state })).toBeUndefined();
    }
    const { state: _dropped, ...withoutState } = valid;
    expect(parseLastRun(withoutState)).toBeUndefined();
  });

  test("rejects a missing or non-array error list", () => {
    expect(parseLastRun({ state: "Failed", operatorErrors: [] })).toBeUndefined();
    expect(parseLastRun({ ...valid, workflowErrors: "worker lost" })).toBeUndefined();
  });

  test("rejects an operator entry without a string operatorId or message", () => {
    expect(parseLastRun({ ...valid, operatorErrors: [{ operatorId: 1, message: "x" }] })).toBeUndefined();
    expect(parseLastRun({ ...valid, operatorErrors: [{ operatorId: "op1" }] })).toBeUndefined();
  });

  test("rejects a message over the size limit", () => {
    const long = "x".repeat(LAST_RUN_MESSAGE_MAX_CHARS + 1);
    expect(parseLastRun({ ...valid, operatorErrors: [{ operatorId: "op1", message: long }] })).toBeUndefined();
    expect(parseLastRun({ ...valid, workflowErrors: [long] })).toBeUndefined();
    const atLimit = "x".repeat(LAST_RUN_MESSAGE_MAX_CHARS);
    expect(parseLastRun({ ...valid, workflowErrors: [atLimit] })).toBeDefined();
  });

  test("rejects more entries than the cap", () => {
    const tooMany = Array.from({ length: LAST_RUN_MAX_ENTRIES + 1 }, (_, i) => ({
      operatorId: `op${i}`,
      message: "e",
    }));
    expect(parseLastRun({ ...valid, operatorErrors: tooMany })).toBeUndefined();
    expect(parseLastRun({ ...valid, workflowErrors: tooMany.map(e => e.message) })).toBeUndefined();
  });

  test("rejects a non-object value", () => {
    expect(parseLastRun("Failed")).toBeUndefined();
    expect(parseLastRun(42)).toBeUndefined();
  });

  test("rejects an operator error entry with an empty operator id", () => {
    expect(parseLastRun({ ...valid, operatorErrors: [{ operatorId: "", message: "x" }] })).toBeUndefined();
  });

  test("accepts an operator id of 200 characters and rejects one of 201", () => {
    const error = (operatorId: string) => ({ ...valid, operatorErrors: [{ operatorId, message: "x" }] });
    const stats = (operatorId: string) => ({
      ...valid,
      operatorStats: [{ operatorId, state: "Completed", inputRows: 1, outputRows: 1 }],
    });

    expect(parseLastRun(error("o".repeat(200)))).toBeDefined();
    expect(parseLastRun(error("o".repeat(201)))).toBeUndefined();
    expect(parseLastRun(stats("o".repeat(200)))).toBeDefined();
    expect(parseLastRun(stats("o".repeat(201)))).toBeUndefined();
  });
});

// The numbers below are written out rather than imported: they are the limits the frontend
// builds its report against, so a changed constant on this side has to fail here.
describe("parseLastRun — limits the frontend relies on", () => {
  const errors = (n: number, length: number) =>
    Array.from({ length: n }, (_, i) => ({ operatorId: `op${i}`, message: "m".repeat(length) }));
  const stats = (n: number) =>
    Array.from({ length: n }, (_, i) => ({ operatorId: `op${i}`, state: "Completed", inputRows: 1, outputRows: 1 }));

  test("accepts the largest report the frontend sends", () => {
    // 10 entries per error list, messages cut to 1500 + 5 + 500 characters, 50 stats entries.
    const largest = {
      state: "Failed",
      operatorErrors: errors(10, 2005),
      workflowErrors: errors(10, 2005).map(e => e.message),
      operatorStats: stats(50),
    };

    expect(parseLastRun(largest)).toEqual(largest as any);
  });

  test("accepts ten entries in each error list and rejects eleven", () => {
    expect(parseLastRun({ ...valid, operatorErrors: errors(10, 1) })).toBeDefined();
    expect(parseLastRun({ ...valid, operatorErrors: errors(11, 1) })).toBeUndefined();
    expect(parseLastRun({ ...valid, workflowErrors: errors(10, 1).map(e => e.message) })).toBeDefined();
    expect(parseLastRun({ ...valid, workflowErrors: errors(11, 1).map(e => e.message) })).toBeUndefined();
  });

  test("accepts a message of 4000 characters and rejects one of 4001", () => {
    expect(parseLastRun({ ...valid, workflowErrors: ["m".repeat(4000)] })).toBeDefined();
    expect(parseLastRun({ ...valid, workflowErrors: ["m".repeat(4001)] })).toBeUndefined();
    expect(parseLastRun({ ...valid, operatorErrors: errors(1, 4000) })).toBeDefined();
    expect(parseLastRun({ ...valid, operatorErrors: errors(1, 4001) })).toBeUndefined();
  });

  test("accepts fifty stats entries and rejects fifty-one", () => {
    expect(parseLastRun({ ...valid, operatorStats: stats(50) })).toBeDefined();
    expect(parseLastRun({ ...valid, operatorStats: stats(51) })).toBeUndefined();
  });
});

describe("parseLastRun — operator stats", () => {
  const stats = { operatorId: "op1", state: "Completed", inputRows: 5000, outputRows: 0 };

  test("accepts a report without operator stats", () => {
    expect(parseLastRun(valid)).toEqual(valid);
    expect(parseLastRun(valid)?.operatorStats).toBeUndefined();
  });

  test("accepts well-formed operator stats, with and without the cache flag", () => {
    const report = { ...valid, operatorStats: [stats, { ...stats, operatorId: "op2", reusedFromCache: true }] };
    expect(parseLastRun(report)).toEqual(report);
  });

  test("accepts an empty stats list and zero counts", () => {
    expect(parseLastRun({ ...valid, operatorStats: [] })).toBeDefined();
    expect(parseLastRun({ ...valid, operatorStats: [{ ...stats, inputRows: 0, outputRows: 0 }] })).toBeDefined();
  });

  test("rejects negative or fractional row counts", () => {
    expect(parseLastRun({ ...valid, operatorStats: [{ ...stats, inputRows: -1 }] })).toBeUndefined();
    expect(parseLastRun({ ...valid, operatorStats: [{ ...stats, outputRows: 1.5 }] })).toBeUndefined();
    expect(parseLastRun({ ...valid, operatorStats: [{ ...stats, outputRows: "3" }] })).toBeUndefined();
  });

  test("rejects a stats entry with a missing field, an empty operator id or a non-boolean cache flag", () => {
    const { outputRows: _dropped, ...withoutOutput } = stats;
    expect(parseLastRun({ ...valid, operatorStats: [withoutOutput] })).toBeUndefined();
    expect(parseLastRun({ ...valid, operatorStats: [{ ...stats, operatorId: "" }] })).toBeUndefined();
    expect(parseLastRun({ ...valid, operatorStats: [{ ...stats, reusedFromCache: "yes" }] })).toBeUndefined();
  });

  test("rejects a state over the size limit and accepts one at the limit", () => {
    const atLimit = "s".repeat(LAST_RUN_STATE_MAX_CHARS);
    expect(parseLastRun({ ...valid, operatorStats: [{ ...stats, state: atLimit }] })).toBeDefined();
    expect(parseLastRun({ ...valid, operatorStats: [{ ...stats, state: atLimit + "s" }] })).toBeUndefined();
  });

  test("rejects more stats entries than the cap and accepts exactly the cap", () => {
    const entries = (n: number) => Array.from({ length: n }, (_, i) => ({ ...stats, operatorId: `op${i}` }));
    expect(parseLastRun({ ...valid, operatorStats: entries(LAST_RUN_MAX_OPERATOR_STATS) })).toBeDefined();
    expect(parseLastRun({ ...valid, operatorStats: entries(LAST_RUN_MAX_OPERATOR_STATS + 1) })).toBeUndefined();
  });

  test("rejects a non-array stats field", () => {
    expect(parseLastRun({ ...valid, operatorStats: stats })).toBeUndefined();
  });
});
