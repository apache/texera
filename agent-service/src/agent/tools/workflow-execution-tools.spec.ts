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
import { createExecuteOperatorTool, executeOperatorAndFormat, type ExecutionConfig } from "./workflow-execution-tools";
import { WorkflowState } from "../workflow-state";
import { WorkflowSystemMetadata } from "../util/workflow-system-metadata";
import type { OperatorLink, OperatorPredicate, PortDescription } from "../../types/workflow";
import type { OperatorInfo, SyncExecutionResult } from "../../types/execution";

function makeOperator(
  id: string,
  inputPorts: PortDescription[] = [],
  overrides: Partial<OperatorPredicate> = {}
): OperatorPredicate {
  return {
    operatorID: id,
    operatorType: "TestOp",
    operatorVersion: "1.0",
    operatorProperties: {},
    inputPorts,
    outputPorts: [],
    showAdvanced: false,
    ...overrides,
  };
}

function makeLink(source: string, sourcePort: string, target: string, targetPort: string): OperatorLink {
  return {
    linkID: `${source}.${sourcePort}->${target}.${targetPort}`,
    source: { operatorID: source, portID: sourcePort },
    target: { operatorID: target, portID: targetPort },
  };
}

function stateWith(...operators: OperatorPredicate[]): WorkflowState {
  const state = new WorkflowState();
  for (const op of operators) state.addOperator(op);
  return state;
}

// The JSON body of the nth fetch the code under test issued.
function requestBody(spy: ReturnType<typeof spyOn>, callIndex = 0): any {
  return JSON.parse((spy.mock.calls[callIndex][1] as RequestInit).body as string);
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

  test("merges schema and connection violations into one blocking message", async () => {
    validateSpy.mockReturnValue({ isValid: false, messages: { limit: "must be a number" } });
    const state = stateWith(makeOperator("op1", [{ portID: "input-0" }]));

    const result = await executeOperatorAndFormat(state, cfg(), "op1");

    expect(result).toBe(
      "[ERROR] Operator op1:\n  - limit: must be a number\n  - inputs: input-0 requires at least 1 input, has 0."
    );
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  test("rejects a single-input port that has more than one incoming link, naming it by display name", async () => {
    const dst = makeOperator("dst", [{ portID: "input-0", displayName: "Left", disallowMultiInputs: true }]);
    const state = stateWith(makeOperator("a"), makeOperator("b"), dst);
    state.addLink(makeLink("a", "output-0", "dst", "input-0"));
    state.addLink(makeLink("b", "output-0", "dst", "input-0"));

    const result = await executeOperatorAndFormat(state, cfg(), "dst");

    expect(result).toBe("[ERROR] Operator dst:\n  - inputs: Left requires 1 input, has 2.");
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

describe("executeOperatorAndFormat — request construction", () => {
  test("sends the upstream sub-DAG with port ordinals resolved from each operator's port list", async () => {
    const src = makeOperator("src", [], {
      operatorProperties: { limit: 3 },
      outputPorts: [{ portID: "output-0" }, { portID: "output-1" }],
    });
    const dst = makeOperator("dst", [{ portID: "input-0" }, { portID: "input-1" }]);
    const state = stateWith(src, dst);
    state.addLink(makeLink("src", "output-1", "dst", "input-0"));
    // An unknown source port falls back to ordinal 0 rather than -1.
    state.addLink(makeLink("src", "output-9", "dst", "input-1"));
    resolveFetch(fetchSpy, {
      success: true,
      state: "Completed",
      operators: {
        dst: { state: "Completed", inputTuples: 0, outputTuples: 1, resultMode: "table", result: [{ a: 1 }] },
      },
    });

    await executeOperatorAndFormat(state, cfg({ workflowId: 7, computingUnitId: 3, executionTimeoutMs: 4200 }), "dst");

    expect(String(fetchSpy.mock.calls[0][0])).toBe("http://localhost:8085/api/execution/7/3/run");
    expect((fetchSpy.mock.calls[0][1] as RequestInit).headers).toMatchObject({ Authorization: "Bearer tok" });

    const body = requestBody(fetchSpy);
    expect(body.executionName).toBe("agent-execution");
    // 4200 rather than a round 4500: with 4500 the assertion holds for ceil, round *and*
    // floor-plus-one, so it would not notice the rounding being changed. 4200 separates them.
    expect(body.timeoutSeconds).toBe(5);
    expect(body.targetOperatorIds).toEqual(["dst"]);
    expect(body.logicalPlan.opsToViewResult).toEqual(["dst"]);
    expect(body.logicalPlan.links).toEqual([
      {
        fromOpId: "src",
        fromPortId: { id: 1, internal: false },
        toOpId: "dst",
        toPortId: { id: 0, internal: false },
      },
      {
        fromOpId: "src",
        fromPortId: { id: 0, internal: false },
        toOpId: "dst",
        toPortId: { id: 1, internal: false },
      },
    ]);
    // Operator properties are flattened into the wire operator alongside its ports.
    expect(body.logicalPlan.operators).toContainEqual(
      expect.objectContaining({ operatorID: "src", operatorType: "TestOp", limit: 3 })
    );
  });

  test("sends the whole workflow, not an upstream slice, when the operator id is empty", async () => {
    // The plan builder only takes the sub-DAG path for a *truthy* target id, so an empty
    // operator id falls through to the "every operator" branch. `sink` and `orphan` are the
    // tell: neither is upstream of `mid`, so a sub-DAG walk would have dropped them.
    const src = makeOperator("src", [], {
      operatorProperties: { limit: 3 },
      outputPorts: [{ portID: "output-0" }, { portID: "output-1" }],
    });
    const mid = makeOperator("mid", [{ portID: "input-0" }], { outputPorts: [{ portID: "output-0" }] });
    const sink = makeOperator("sink", [{ portID: "input-0" }, { portID: "input-1" }]);
    const orphan = makeOperator("orphan");
    const off = makeOperator("off", [], { isDisabled: true });
    const state = stateWith(src, mid, sink, orphan, off);
    state.addLink(makeLink("src", "output-1", "mid", "input-0"));
    state.addLink(makeLink("mid", "output-0", "sink", "input-1"));
    resolveFetch(fetchSpy, { success: true, state: "Completed", operators: {} });

    await executeOperatorAndFormat(state, cfg(), "");

    const body = requestBody(fetchSpy);
    expect(body.logicalPlan.operators.map((op: { operatorID: string }) => op.operatorID).sort()).toEqual([
      // getAllEnabledOperators() does not filter on isDisabled, so `off` is sent too.
      "mid",
      "off",
      "orphan",
      "sink",
      "src",
    ]);
    expect(body.logicalPlan.operators).toContainEqual(
      expect.objectContaining({ operatorID: "src", operatorType: "TestOp", limit: 3, outputPorts: src.outputPorts })
    );
    expect(body.logicalPlan.links).toEqual([
      { fromOpId: "src", fromPortId: { id: 1, internal: false }, toOpId: "mid", toPortId: { id: 0, internal: false } },
      { fromOpId: "mid", fromPortId: { id: 0, internal: false }, toOpId: "sink", toPortId: { id: 1, internal: false } },
    ]);
    // The empty id matches no operator, so nothing is requested back.
    expect(body.logicalPlan.opsToViewResult).toEqual([]);

    // Contrast: a real id takes the sub-DAG path and keeps only what feeds it.
    fetchSpy.mockClear();
    resolveFetch(fetchSpy, { success: true, state: "Completed", operators: {} });
    await executeOperatorAndFormat(state, cfg(), "mid");
    expect(
      requestBody(fetchSpy)
        .logicalPlan.operators.map((op: { operatorID: string }) => op.operatorID)
        .sort()
    ).toEqual(["mid", "src"]);
  });
});

describe("executeOperatorAndFormat — result rendering", () => {
  test("labels input shapes with the upstream operator id, ordered by port index", async () => {
    const dst = makeOperator("dst", [{ portID: "input-0" }, { portID: "input-1" }]);
    const state = stateWith(makeOperator("a"), makeOperator("b"), dst);
    state.addLink(makeLink("a", "output-0", "dst", "input-0"));
    state.addLink(makeLink("b", "output-0", "dst", "input-1"));
    resolveFetch(fetchSpy, {
      success: true,
      state: "Completed",
      operators: {
        dst: {
          state: "Completed",
          inputTuples: 0,
          outputTuples: 99,
          resultMode: "table",
          totalRowCount: 4,
          // Deliberately out of order to pin the sort by port index.
          inputPortShapes: [
            { portIndex: 1, rows: 20, columns: 2 },
            { portIndex: 0, rows: 5, columns: 3 },
          ],
          result: [{ x: 1 }],
        },
      },
    });

    const result = await executeOperatorAndFormat(state, cfg(), "dst");

    expect(result).toContain("Input operator(table shape): a(5, 3), b(20, 2)");
    expect(result).toContain("Output table shape: (4, 1)"); // totalRowCount wins over outputTuples
  });

  test("falls back to outputTuples for the row count and appends operator warnings", async () => {
    const state = stateWith(makeOperator("op1"));
    resolveFetch(fetchSpy, {
      success: true,
      state: "Completed",
      operators: {
        op1: {
          state: "Completed",
          inputTuples: 0,
          outputTuples: 3,
          resultMode: "table",
          warnings: ["result truncated"],
          result: [{ x: 1 }],
        },
      },
    });

    const result = await executeOperatorAndFormat(state, cfg(), "op1");

    expect(result.split("\n").slice(0, 3)).toEqual([
      "Executed operator op1",
      "Output table shape: (3, 1)",
      "result truncated",
    ]);
  });

  test("renders backend row indices, inserting an ellipsis row where they skip", async () => {
    const state = stateWith(makeOperator("op1"));
    resolveFetch(fetchSpy, {
      success: true,
      state: "Completed",
      operators: {
        op1: {
          state: "Completed",
          inputTuples: 0,
          outputTuples: 4,
          resultMode: "table",
          totalRowCount: 4,
          result: [
            { __row_index__: 0, a: 1, b: "x" },
            { __row_index__: 1, a: null, b: "NULL" },
            { __row_index__: 10, a: true, b: "has\ttab\nand newline" },
            { __row_index__: 11, a: { k: 1 }, b: undefined },
          ],
        },
      },
    });

    const result = await executeOperatorAndFormat(state, cfg(), "op1");

    expect(result.split("\n").slice(1)).toEqual([
      "Output table shape: (4, 2)", // __row_index__ is internal and not counted as a column
      "\ta\tb",
      "0\t1\tx",
      "1\tNaN\tNaN",
      "...\t...\t...",
      "10\ttrue\thas\\ttab\\nand newline",
      '11\t{"k":1}\t',
    ]);
  });

  test("emits only the summary and shape line when the operator produced zero rows", async () => {
    const state = stateWith(makeOperator("op1"));
    resolveFetch(fetchSpy, {
      success: true,
      state: "Completed",
      operators: {
        op1: { state: "Completed", inputTuples: 0, outputTuples: 0, resultMode: "table", result: [] },
      },
    });

    const result = await executeOperatorAndFormat(state, cfg(), "op1");

    expect(result).toBe("Executed operator op1\nOutput table shape: (0, 0)");
  });
});

describe("createExecuteOperatorTool", () => {
  test("resolves the config per invocation and forwards the operator id and onResult hook", async () => {
    const state = stateWith(makeOperator("op1"));
    resolveFetch(fetchSpy, {
      success: true,
      state: "Completed",
      operators: {
        op1: { state: "Completed", inputTuples: 0, outputTuples: 1, resultMode: "table", result: [{ a: 1 }] },
      },
    });
    const onResult = mock((_id: string, _info: OperatorInfo) => {});
    let workflowId = 42;
    const getConfig = mock(() => cfg({ workflowId }));

    const executeTool = createExecuteOperatorTool(state, getConfig, onResult);
    const output = await executeTool.execute!({ operatorId: "op1" }, {} as any);
    // Invoked twice, with the workflow id changing in between. One invocation cannot tell
    // per-invocation resolution from a config captured once when the tool was built: the call
    // count is 1 either way. The second URL is what proves the later config is the one used.
    workflowId = 43;
    await executeTool.execute!({ operatorId: "op1" }, {} as any);

    expect(getConfig).toHaveBeenCalledTimes(2);
    expect(output).toContain("Executed operator op1");
    expect(String(fetchSpy.mock.calls[0][0])).toContain("/api/execution/42/0/run");
    expect(String(fetchSpy.mock.calls[1][0])).toContain("/api/execution/43/0/run");
    expect(onResult).toHaveBeenCalledTimes(2);
    expect(onResult.mock.calls[0][0]).toBe("op1");
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
