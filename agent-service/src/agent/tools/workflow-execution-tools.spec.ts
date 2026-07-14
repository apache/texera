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

import { afterEach, beforeEach, describe, expect, mock, spyOn, test } from "bun:test";
import { executeOperatorAndFormat, type ExecutionConfig } from "./workflow-execution-tools";
import { WorkflowState } from "../workflow-state";
import { WorkflowSystemMetadata } from "../util/workflow-system-metadata";
import type { OperatorPredicate, PortDescription } from "../../types/workflow";
import type { OperatorInfo, SyncExecutionResult } from "../../types/execution";

function makeOperator(id: string, inputPorts: PortDescription[] = []): OperatorPredicate {
  return {
    operatorID: id,
    operatorType: "TestOp",
    operatorVersion: "1.0",
    operatorProperties: {},
    inputPorts,
    outputPorts: [],
    showAdvanced: false,
  };
}

function stateWith(...operators: OperatorPredicate[]): WorkflowState {
  const state = new WorkflowState();
  for (const op of operators) state.addOperator(op);
  return state;
}

function cfg(overrides: Partial<ExecutionConfig> = {}): ExecutionConfig {
  return { userToken: "tok", workflowId: 1, ...overrides };
}

// A fetch double resolving to an ok response whose body is the given result.
function resolveFetch(spy: ReturnType<typeof spyOn>, result: SyncExecutionResult): void {
  spy.mockResolvedValue({ ok: true, json: async () => result } as unknown as Response);
}

let fetchSpy: ReturnType<typeof spyOn>;
let validateSpy: ReturnType<typeof spyOn>;

beforeEach(() => {
  // Default: any unexpected network call fails loudly instead of hitting localhost.
  fetchSpy = spyOn(globalThis, "fetch").mockRejectedValue(new Error("unexpected fetch"));
  // Isolate connection validation from schema validation (TestOp is an unknown type).
  validateSpy = spyOn(WorkflowSystemMetadata.getInstance(), "validateOperatorProperties").mockReturnValue({
    isValid: true,
  });
});

afterEach(() => {
  fetchSpy.mockRestore();
  validateSpy.mockRestore();
});

describe("executeOperatorAndFormat — guards & validation", () => {
  test("reports 'no operators' when the workflow is empty", async () => {
    const result = await executeOperatorAndFormat(new WorkflowState(), cfg(), "op1");
    expect(result).toBe("[ERROR] Cannot execute: workflow has no operators.");
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  test("reports 'no operators' when the target operator is absent from the workflow", async () => {
    // The sub-DAG built for a non-existent target contains no operators.
    const state = stateWith(makeOperator("other"));
    const result = await executeOperatorAndFormat(state, cfg(), "op1");
    expect(result).toBe("[ERROR] Cannot execute: workflow has no operators.");
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  test("blocks on the target operator's own validation error without executing", async () => {
    // One unlinked input port -> connection validation fails for op1.
    const state = stateWith(makeOperator("op1", [{ portID: "input-0" }]));
    const result = await executeOperatorAndFormat(state, cfg(), "op1");
    expect(result).toBe("[ERROR] Operator op1:\n  - inputs: input-0 requires at least 1 input, has 0.");
    expect(fetchSpy).not.toHaveBeenCalled();
  });
});

describe("executeOperatorAndFormat — execution-level failures", () => {
  test("formats per-operator errors when the run state is Failed and notifies onResult", async () => {
    const state = stateWith(makeOperator("op1"));
    resolveFetch(fetchSpy, {
      success: false,
      state: "Failed",
      operators: {
        op1: { state: "Failed", inputTuples: 0, outputTuples: 0, resultMode: "table", error: "runtime boom" },
      },
      errors: [],
    });
    const onResult = mock((_id: string, _info: OperatorInfo) => {});

    const result = await executeOperatorAndFormat(state, cfg(), "op1", { onResult });

    expect(result).toContain("[ERROR]");
    expect(result).toContain("Execution error:");
    expect(result).toContain("op1: runtime boom");
    expect(onResult).toHaveBeenCalledTimes(1);
    expect(onResult.mock.calls[0][0]).toBe("op1");
    expect(onResult.mock.calls[0][1].state).toBe("Failed");
    expect(onResult.mock.calls[0][1].error).toContain("runtime boom");
  });

  test("formats compilation errors when the run state is CompilationFailed", async () => {
    const state = stateWith(makeOperator("op1"));
    resolveFetch(fetchSpy, {
      success: false,
      state: "CompilationFailed",
      operators: {},
      compilationErrors: { op1: "type mismatch" },
    });

    const result = await executeOperatorAndFormat(state, cfg(), "op1");

    expect(result).toContain("Compilation error:");
    expect(result).toContain("op1: type mismatch");
  });

  test("reports a timeout message when the run state is Killed", async () => {
    const state = stateWith(makeOperator("op1"));
    resolveFetch(fetchSpy, { success: false, state: "Killed", operators: {}, errors: ["ignored"] });

    const result = await executeOperatorAndFormat(state, cfg(), "op1");

    expect(result).toContain("Workflow execution was killed (timeout).");
    expect(result).not.toContain("ignored");
  });

  test("surfaces a network error as a general error", async () => {
    const state = stateWith(makeOperator("op1"));
    fetchSpy.mockRejectedValue(new Error("network down"));

    const result = await executeOperatorAndFormat(state, cfg(), "op1");

    expect(result).toContain("[ERROR]");
    expect(result).toContain("network down");
  });

  test("surfaces a non-ok HTTP response as a general error", async () => {
    const state = stateWith(makeOperator("op1"));
    fetchSpy.mockResolvedValue({
      ok: false,
      status: 500,
      statusText: "Internal Server Error",
      text: async () => "upstream boom",
    } as unknown as Response);

    const result = await executeOperatorAndFormat(state, cfg(), "op1");

    expect(result).toContain("Execution request failed: 500 Internal Server Error - upstream boom");
  });
});

describe("executeOperatorAndFormat — operator result handling", () => {
  test("errors when the run succeeds but the target operator has no result entry", async () => {
    const state = stateWith(makeOperator("op1"));
    resolveFetch(fetchSpy, { success: true, state: "Completed", operators: {} });

    const result = await executeOperatorAndFormat(state, cfg(), "op1");

    expect(result).toContain("[ERROR]");
    expect(result).toContain("No result found for operator: op1");
  });

  test("errors and notifies onResult when the target operator carries an error", async () => {
    const state = stateWith(makeOperator("op1"));
    const opInfo: OperatorInfo = {
      state: "Completed",
      inputTuples: 0,
      outputTuples: 0,
      resultMode: "table",
      error: "kaboom",
    };
    resolveFetch(fetchSpy, { success: true, state: "Completed", operators: { op1: opInfo } });
    const onResult = mock((_id: string, _info: OperatorInfo) => {});

    const result = await executeOperatorAndFormat(state, cfg(), "op1", { onResult });

    expect(result).toContain("Execution error:");
    expect(result).toContain("op1: kaboom");
    expect(onResult).toHaveBeenCalledTimes(1);
    expect(onResult.mock.calls[0][1].error).toBe("kaboom");
  });

  test("returns a placeholder when the operator has no result array", async () => {
    const state = stateWith(makeOperator("op1"));
    resolveFetch(fetchSpy, {
      success: true,
      state: "Completed",
      operators: { op1: { state: "Completed", inputTuples: 0, outputTuples: 0, resultMode: "table" } },
    });

    const result = await executeOperatorAndFormat(state, cfg(), "op1");

    expect(result).toBe("(no result data)");
  });

  test("returns a placeholder when the operator result is present but not an array", async () => {
    const state = stateWith(makeOperator("op1"));
    resolveFetch(fetchSpy, {
      success: true,
      state: "Completed",
      operators: {
        op1: {
          state: "Completed",
          inputTuples: 0,
          outputTuples: 0,
          resultMode: "table",
          result: { rows: [] } as unknown as Record<string, any>[],
        },
      },
    });

    const result = await executeOperatorAndFormat(state, cfg(), "op1");

    expect(result).toBe("(no result data)");
  });

  test("formats a successful tabular result with shape line and notifies onResult per operator", async () => {
    const state = stateWith(makeOperator("op1"));
    const opInfo: OperatorInfo = {
      state: "Completed",
      inputTuples: 0,
      outputTuples: 2,
      resultMode: "table",
      totalRowCount: 2,
      result: [
        { a: 1, b: 2 },
        { a: 3, b: 4 },
      ],
    };
    resolveFetch(fetchSpy, { success: true, state: "Completed", operators: { op1: opInfo } });
    const onResult = mock((_id: string, _info: OperatorInfo) => {});

    const result = await executeOperatorAndFormat(state, cfg(), "op1", { onResult });

    expect(result).toContain("Executed operator op1");
    expect(result).toContain("Output table shape: (2, 2)");
    expect(result).toContain("\ta\tb");
    expect(result).toContain("0\t1\t2");
    expect(result).toContain("1\t3\t4");
    expect(onResult).toHaveBeenCalledTimes(1);
    expect(onResult.mock.calls[0][0]).toBe("op1");
    expect(onResult.mock.calls[0][1].state).toBe("Completed");
  });

  test("truncates a result that exceeds the char limit, keeping head and tail rows", async () => {
    const state = stateWith(makeOperator("op1"));
    const rows = Array.from({ length: 20 }, (_, i) => ({ n: i }));
    resolveFetch(fetchSpy, {
      success: true,
      state: "Completed",
      operators: { op1: { state: "Completed", inputTuples: 0, outputTuples: 20, resultMode: "table", result: rows } },
    });

    const result = await executeOperatorAndFormat(state, cfg({ maxOperatorResultCharLimit: 50 }), "op1");

    expect(result).toContain("\tn"); // header preserved
    expect(result).toContain("0\t0"); // a head row kept
    expect(result).toContain("19\t19"); // a tail row kept
    expect(result).not.toContain("10\t10"); // a middle row dropped
  });
});

describe("executeOperatorAndFormat — cancellation", () => {
  test("re-throws AbortError instead of formatting it as a result", async () => {
    const state = stateWith(makeOperator("op1"));
    const abortErr = new Error("aborted");
    abortErr.name = "AbortError";
    fetchSpy.mockRejectedValue(abortErr);

    await expect(
      executeOperatorAndFormat(state, cfg(), "op1", { abortSignal: new AbortController().signal })
    ).rejects.toThrow("aborted");
  });
});
