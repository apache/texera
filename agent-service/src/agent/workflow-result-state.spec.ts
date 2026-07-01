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
import { WorkflowResultState } from "./workflow-result-state";
import type { OperatorInfo } from "../types/execution";

function makeInfo(outputTuples: number): OperatorInfo {
  return {
    state: "Completed",
    inputTuples: 0,
    outputTuples,
    resultMode: "table",
  };
}

describe("WorkflowResultState - ancestor walk", () => {
  test("returns the most recent ancestor entry", () => {
    let path: string[] = [];
    const state = new WorkflowResultState(() => path);

    state.set("op1", "step-A", makeInfo(1));
    state.set("op1", "step-B", makeInfo(2));
    state.set("op1", "step-C", makeInfo(3));

    path = ["step-A", "step-B", "step-C"];
    expect(state.get("op1")?.operatorInfo.outputTuples).toBe(3);

    // Rewind to step-B; step-C is no longer an ancestor.
    path = ["step-A", "step-B"];
    expect(state.get("op1")?.operatorInfo.outputTuples).toBe(2);

    // Rewind further.
    path = ["step-A"];
    expect(state.get("op1")?.operatorInfo.outputTuples).toBe(1);
  });

  test("returns undefined when no ancestor has a result", () => {
    const state = new WorkflowResultState(() => ["step-X"]);
    state.set("op1", "step-A", makeInfo(1));
    expect(state.get("op1")).toBeUndefined();
  });

  test("returns undefined for unknown operator", () => {
    const state = new WorkflowResultState(() => ["step-A"]);
    expect(state.get("missing")).toBeUndefined();
  });

  test("getAllVisible returns one entry per operator on the current branch", () => {
    let path: string[] = [];
    const state = new WorkflowResultState(() => path);

    // op1 has results on step-A and step-C; the branch only goes through A and B.
    state.set("op1", "step-A", makeInfo(1));
    state.set("op1", "step-C", makeInfo(99));
    state.set("op2", "step-B", makeInfo(7));

    path = ["step-A", "step-B"];
    const visible = state.getAllVisible();
    expect(visible.size).toBe(2);
    expect(visible.get("op1")?.operatorInfo.outputTuples).toBe(1);
    expect(visible.get("op2")?.operatorInfo.outputTuples).toBe(7);
  });

  test("clear drops all stored results", () => {
    const state = new WorkflowResultState(() => ["step-A"]);
    state.set("op1", "step-A", makeInfo(1));
    state.clear();
    expect(state.get("op1")).toBeUndefined();
    expect(state.getAllVisible().size).toBe(0);
  });

  test("set on the same step overwrites", () => {
    const state = new WorkflowResultState(() => ["step-A"]);
    state.set("op1", "step-A", makeInfo(1));
    state.set("op1", "step-A", makeInfo(42));
    expect(state.get("op1")?.operatorInfo.outputTuples).toBe(42);
  });
});
