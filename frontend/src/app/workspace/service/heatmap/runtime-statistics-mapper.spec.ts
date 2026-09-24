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

import { operatorStateFromStatusCode, toOperatorRuntimeStatusMap } from "./runtime-statistics-mapper";
import { OperatorState } from "../../types/execute-workflow.interface";
import { WorkflowRuntimeStatistics } from "../../../dashboard/type/workflow-runtime-statistics";

function makeRow(overrides: Partial<WorkflowRuntimeStatistics>): WorkflowRuntimeStatistics {
  return {
    operatorId: "op1",
    timestamp: 1_000,
    inputTupleCount: 0,
    inputTupleSize: 0,
    outputTupleCount: 0,
    outputTupleSize: 0,
    totalDataProcessingTime: 0,
    totalControlProcessingTime: 0,
    totalIdleTime: 0,
    numberOfWorkers: 1,
    status: 3,
    ...overrides,
  };
}

describe("operatorStateFromStatusCode", () => {
  it("maps the persisted codes the engine writes (Utils.maptoStatusCode)", () => {
    expect(operatorStateFromStatusCode(0)).toBe(OperatorState.Uninitialized);
    expect(operatorStateFromStatusCode(1)).toBe(OperatorState.Running);
    expect(operatorStateFromStatusCode(2)).toBe(OperatorState.Paused);
    expect(operatorStateFromStatusCode(3)).toBe(OperatorState.Completed);
  });

  it("falls back to Uninitialized for codes OperatorState cannot express", () => {
    // 4 = Failed, 5 = Killed, -1 = other: no OperatorState member exists for
    // them, so they render as the neutral default rather than a wrong state.
    expect(operatorStateFromStatusCode(4)).toBe(OperatorState.Uninitialized);
    expect(operatorStateFromStatusCode(5)).toBe(OperatorState.Uninitialized);
    expect(operatorStateFromStatusCode(-1)).toBe(OperatorState.Uninitialized);
    expect(operatorStateFromStatusCode(999)).toBe(OperatorState.Uninitialized);
  });
});

describe("toOperatorRuntimeStatusMap", () => {
  it("maps every field of a row onto the wire shape", () => {
    const result = toOperatorRuntimeStatusMap([
      makeRow({
        operatorId: "op1",
        inputTupleCount: 1_000,
        inputTupleSize: 8_000,
        outputTupleCount: 250,
        outputTupleSize: 2_000,
        totalDataProcessingTime: 5_000_000,
        totalControlProcessingTime: 1_000_000,
        totalIdleTime: 700_000,
        numberOfWorkers: 2,
        status: 1,
      }),
    ]);

    expect(result).toEqual({
      op1: {
        operatorState: OperatorState.Running,
        aggregatedInputRowCount: 1_000,
        aggregatedInputSize: 8_000,
        aggregatedOutputRowCount: 250,
        aggregatedOutputSize: 2_000,
        numWorkers: 2,
        aggregatedDataProcessingTime: 5_000_000,
        aggregatedControlProcessingTime: 1_000_000,
        aggregatedIdleTime: 700_000,
      },
    });
  });

  it("omits the port maps rather than emitting empty ones, so port labels survive a restore", () => {
    // An empty map reads as "every port measured zero" and makes JointUIService write 0 over
    // the port display names; absent means "no per-port information" and leaves them alone.
    const restored = toOperatorRuntimeStatusMap([makeRow({ operatorId: "op1" })]).op1;
    expect(restored).not.toHaveProperty("inputPortMetrics");
    expect(restored).not.toHaveProperty("outputPortMetrics");
  });

  it("keeps only the latest row per operator, regardless of input order", () => {
    const result = toOperatorRuntimeStatusMap([
      makeRow({ operatorId: "op1", timestamp: 3_000, outputTupleCount: 30, status: 3 }),
      makeRow({ operatorId: "op1", timestamp: 1_000, outputTupleCount: 10, status: 1 }),
      makeRow({ operatorId: "op2", timestamp: 2_000, outputTupleCount: 99 }),
      makeRow({ operatorId: "op1", timestamp: 2_000, outputTupleCount: 20, status: 1 }),
    ]);

    expect(Object.keys(result).sort()).toEqual(["op1", "op2"]);
    expect(result["op1"].aggregatedOutputRowCount).toBe(30);
    expect(result["op1"].operatorState).toBe(OperatorState.Completed);
    expect(result["op2"].aggregatedOutputRowCount).toBe(99);
  });

  it("lets the later row win on equal timestamps (snapshots arrive in write order)", () => {
    const result = toOperatorRuntimeStatusMap([
      makeRow({ operatorId: "op1", timestamp: 1_000, outputTupleCount: 1 }),
      makeRow({ operatorId: "op1", timestamp: 1_000, outputTupleCount: 2 }),
    ]);
    expect(result["op1"].aggregatedOutputRowCount).toBe(2);
  });

  it("keys the result by operator id, including unicode ids", () => {
    const id = "算子-✓-1";
    const result = toOperatorRuntimeStatusMap([makeRow({ operatorId: id })]);
    expect(Object.keys(result)).toEqual([id]);
  });

  it("returns an empty map for empty input", () => {
    expect(toOperatorRuntimeStatusMap([])).toEqual({});
  });
});
