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
import { executeOperatorAndFormat, createExecuteOperatorTool, type ExecutionConfig } from "./workflow-execution-tools";
import { WorkflowState } from "../workflow-state";
import { WorkflowSystemMetadata } from "../util/workflow-system-metadata";
import {
  ConsoleMessageType,
  OperatorResultMode,
  OperatorState,
  WorkflowExecutionState,
  WorkflowFatalErrorType,
  type OperatorExecutionSummary,
  type SampleRow,
  type WorkflowExecutionSummary,
  type WorkflowFatalError,
} from "../../types/execution";
import type { OperatorLink, OperatorPredicate, PortDescription } from "../../types/workflow";

// --- fixtures -------------------------------------------------------------

function makeOperator(
  id: string,
  opts: { inputPorts?: PortDescription[]; outputPorts?: PortDescription[]; operatorType?: string } = {}
): OperatorPredicate {
  return {
    operatorID: id,
    operatorType: opts.operatorType ?? "TestOp",
    operatorVersion: "1",
    operatorProperties: {},
    inputPorts: opts.inputPorts ?? [],
    outputPorts: opts.outputPorts ?? [{ portID: "out-0" }],
    showAdvanced: false,
  };
}

// A valid two-operator DAG: src (no inputs) --> tgt (one required input).
function makeLinearState(): { state: WorkflowState; source: string; target: string } {
  const state = new WorkflowState();
  const source = "src";
  const target = "tgt";
  state.addOperator(makeOperator(source, { inputPorts: [], outputPorts: [{ portID: "out-0" }] }));
  state.addOperator(
    makeOperator(target, {
      inputPorts: [{ portID: "in-0", disallowMultiInputs: true }],
      outputPorts: [{ portID: "out-0" }],
    })
  );
  const link: OperatorLink = {
    linkID: "link-1",
    source: { operatorID: source, portID: "out-0" },
    target: { operatorID: target, portID: "in-0" },
  };
  state.addLink(link);
  return { state, source, target };
}

function makeConfig(overrides: Partial<ExecutionConfig> = {}): ExecutionConfig {
  return { userToken: "tok", workflowId: 1, ...overrides };
}

function makeFatal(message: string): WorkflowFatalError {
  return {
    type: { name: WorkflowFatalErrorType.EXECUTION_FAILURE },
    timestamp: { seconds: 0, nanos: 0 },
    message,
    details: "",
    operatorId: "",
    workerId: "",
  };
}

// --- fetch / metadata stubbing -------------------------------------------

const originalFetch = globalThis.fetch;
let validateSpy: ReturnType<typeof spyOn>;

function setFetchResolving(value: Response): void {
  globalThis.fetch = mock(async () => value) as unknown as typeof fetch;
}

function setFetchRejecting(error: Error): void {
  globalThis.fetch = mock(async () => {
    throw error;
  }) as unknown as typeof fetch;
}

function jsonResponse(summary: WorkflowExecutionSummary): Response {
  return new Response(JSON.stringify(summary), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
}

beforeEach(() => {
  // Default: every operator's schema validates, so validation never short-circuits.
  validateSpy = spyOn(WorkflowSystemMetadata.getInstance(), "validateOperatorProperties").mockReturnValue({
    isValid: true,
  });
});

afterEach(() => {
  globalThis.fetch = originalFetch;
  validateSpy.mockRestore();
});

// --- tests ----------------------------------------------------------------

describe("executeOperatorAndFormat - pre-flight guards", () => {
  test("returns an error when the target operator is absent from the workflow", async () => {
    const state = new WorkflowState();
    const out = await executeOperatorAndFormat(state, makeConfig(), "missing");
    expect(out).toBe("[ERROR] Cannot execute: workflow has no operators.");
  });

  test("blocks on the target operator's schema validation errors", async () => {
    const { state, target } = makeLinearState();
    validateSpy.mockReturnValue({ isValid: false, messages: { attributeName: "must not be empty" } });

    const out = await executeOperatorAndFormat(state, makeConfig(), target);
    expect(out).toContain("[ERROR]");
    expect(out).toContain(`Operator ${target}:`);
    expect(out).toContain("- attributeName: must not be empty");
  });

  test("blocks when a disallow-multi-input port has no incoming link", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("solo", { inputPorts: [{ portID: "in-0", disallowMultiInputs: true }] }));

    const out = await executeOperatorAndFormat(state, makeConfig(), "solo");
    expect(out).toContain("[ERROR]");
    expect(out).toContain("requires 1 input, has 0");
  });

  test("blocks when a regular input port has no incoming link", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("solo", { inputPorts: [{ portID: "in-0" }] }));

    const out = await executeOperatorAndFormat(state, makeConfig(), "solo");
    expect(out).toContain("[ERROR]");
    expect(out).toContain("requires at least 1 input, has 0");
  });
});

describe("executeOperatorAndFormat - successful runs", () => {
  test("renders the shape, warnings, gaps, and every cell type", async () => {
    const { state, source, target } = makeLinearState();

    const sampleTuples: SampleRow[] = [
      {
        rowIndex: 0,
        tuple: {
          num: 1,
          bool: true,
          str: "hello",
          nil: null,
          nullstr: "NULL",
          escaped: "a\tb\nc",
          obj: { k: 1 },
        },
      },
      {
        rowIndex: 3, // gap after 0 -> a "..." separator row is inserted
        tuple: {
          num: 2,
          bool: false,
          str: "world",
          nil: null,
          nullstr: "kept",
          escaped: "plain",
          obj: [1, 2],
        },
      },
    ];

    const summary: WorkflowExecutionSummary = {
      success: true,
      state: WorkflowExecutionState.COMPLETED,
      operators: {
        [target]: {
          state: OperatorState.COMPLETED,
          errorMessages: [],
          resultSummary: { resultMode: OperatorResultMode.TABLE, sampleTuples, tuplesCount: 10 },
          consoleLogsSummary: {
            messages: [
              { msgType: ConsoleMessageType.PRINT, title: "WARNING: truncated output", message: "" },
              { msgType: ConsoleMessageType.PRINT, title: "just info, not a warning", message: "" },
            ],
          },
        },
        [source]: { state: OperatorState.COMPLETED, errorMessages: [] },
        // An errored sibling: the notify loop must skip it (covers the false branch).
        ghost: { state: OperatorState.FAILED, errorMessages: [makeFatal("ignored")] },
      },
      errors: [],
    };
    setFetchResolving(jsonResponse(summary));

    const onResult = mock(() => {});
    const out = await executeOperatorAndFormat(state, makeConfig(), target, { onResult });

    expect(out).toContain(`Executed operator ${target}`);
    expect(out).toContain("Output table shape: (10, 7)");
    expect(out).toContain("WARNING: truncated output");
    expect(out).not.toContain("just info");
    expect(out).toContain("NaN"); // null cell
    expect(out).toContain("a\\tb\\nc"); // tab/newline escaped
    expect(out).toContain('{"k":1}'); // object serialized
    expect(out).toContain("...\t"); // gap separator

    // Notified for the two clean operators only.
    const notified = onResult.mock.calls.map(c => (c as unknown as [string])[0]);
    expect(notified.sort()).toEqual([source, target]);
  });

  test("returns a placeholder when the operator produced no result summary", async () => {
    const { state, target } = makeLinearState();
    const summary: WorkflowExecutionSummary = {
      success: true,
      state: WorkflowExecutionState.COMPLETED,
      operators: { [target]: { state: OperatorState.COMPLETED, errorMessages: [] } },
      errors: [],
    };
    setFetchResolving(jsonResponse(summary));

    const out = await executeOperatorAndFormat(state, makeConfig(), target);
    expect(out).toBe("(no result data)");
  });

  test("emits only the shape line when the result has zero sample rows", async () => {
    const { state, target } = makeLinearState();
    const summary: WorkflowExecutionSummary = {
      success: true,
      state: WorkflowExecutionState.COMPLETED,
      operators: {
        [target]: {
          state: OperatorState.COMPLETED,
          errorMessages: [],
          resultSummary: { resultMode: OperatorResultMode.TABLE, sampleTuples: [], tuplesCount: 0 },
        },
      },
      errors: [],
    };
    setFetchResolving(jsonResponse(summary));

    const out = await executeOperatorAndFormat(state, makeConfig(), target);
    expect(out).toContain(`Executed operator ${target}`);
    expect(out).toContain("Output table shape: (0, 0)");
  });

  test("truncates output that exceeds the character budget while keeping the header", async () => {
    const { state, target } = makeLinearState();
    const sampleTuples: SampleRow[] = Array.from({ length: 12 }, (_, i) => ({
      rowIndex: i,
      tuple: { col: `value-${i}` },
    }));
    const summary: WorkflowExecutionSummary = {
      success: true,
      state: WorkflowExecutionState.COMPLETED,
      operators: {
        [target]: {
          state: OperatorState.COMPLETED,
          errorMessages: [],
          resultSummary: { resultMode: OperatorResultMode.TABLE, sampleTuples, tuplesCount: 12 },
        },
      },
      errors: [],
    };
    setFetchResolving(jsonResponse(summary));

    const out = await executeOperatorAndFormat(state, makeConfig({ maxOperatorResultCharLimit: 40 }), target);
    expect(out).toContain("col"); // header retained
    // Not every one of the 12 rows survives the budget.
    const dataRowCount = out.split("\n").filter(l => /^\d+\t/.test(l)).length;
    expect(dataRowCount).toBeLessThan(12);
  });
});

describe("executeOperatorAndFormat - execution failures", () => {
  test("formats per-operator errors and notifies with a synthetic failure summary (FAILED)", async () => {
    const { state, target } = makeLinearState();
    const summary: WorkflowExecutionSummary = {
      success: false,
      state: WorkflowExecutionState.FAILED,
      operators: {
        [target]: { state: OperatorState.FAILED, errorMessages: [makeFatal("kaboom")] },
      },
      errors: [],
    };
    setFetchResolving(jsonResponse(summary));

    const onResult = mock(() => {});
    const out = await executeOperatorAndFormat(state, makeConfig(), target, { onResult });

    expect(out).toContain("[ERROR]");
    expect(out).toContain("Execution error:");
    expect(out).toContain("kaboom");

    expect(onResult).toHaveBeenCalledTimes(1);
    const [, info] = onResult.mock.calls[0] as unknown as [string, OperatorExecutionSummary];
    expect(info.state).toBe(OperatorState.FAILED);
    expect(info.errorMessages[0].type.name).toBe(WorkflowFatalErrorType.EXECUTION_FAILURE);
    expect(info.errorMessages[0].message).toContain("kaboom");
  });

  test("extracts per-operator errors even when the aggregate state stays Completed", async () => {
    const { state, target } = makeLinearState();
    // A console ERROR marks the run unsuccessful without flipping the state.
    const summary: WorkflowExecutionSummary = {
      success: false,
      state: WorkflowExecutionState.COMPLETED,
      operators: {
        [target]: { state: OperatorState.COMPLETED, errorMessages: [makeFatal("console kaboom")] },
      },
      errors: [],
    };
    setFetchResolving(jsonResponse(summary));

    const out = await executeOperatorAndFormat(state, makeConfig(), target);
    expect(out).toContain("Execution error:");
    expect(out).toContain(`${target}: console kaboom`);
  });

  test("labels compilation failures distinctly from runtime errors", async () => {
    const { state, target } = makeLinearState();
    const summary: WorkflowExecutionSummary = {
      success: false,
      state: WorkflowExecutionState.COMPILATION_FAILED,
      operators: {},
      errors: ["schema mismatch on port 0"],
    };
    setFetchResolving(jsonResponse(summary));

    const out = await executeOperatorAndFormat(state, makeConfig(), target);
    expect(out).toContain("Compilation error:");
    expect(out).toContain("schema mismatch on port 0");
  });

  test("reports a timeout when the workflow was KILLED (no onResult callback)", async () => {
    const { state, target } = makeLinearState();
    const summary: WorkflowExecutionSummary = {
      success: false,
      state: WorkflowExecutionState.KILLED,
      operators: {},
      errors: [],
    };
    setFetchResolving(jsonResponse(summary));

    const out = await executeOperatorAndFormat(state, makeConfig(), target);
    expect(out).toContain("[ERROR]");
    expect(out).toContain("Workflow execution was killed (timeout).");
  });

  test("stores the Killed state in the synthetic summary when the workflow was KILLED", async () => {
    const { state, target } = makeLinearState();
    const summary: WorkflowExecutionSummary = {
      success: false,
      state: WorkflowExecutionState.KILLED,
      operators: {},
      errors: [],
    };
    setFetchResolving(jsonResponse(summary));

    const onResult = mock(() => {});
    await executeOperatorAndFormat(state, makeConfig(), target, { onResult });

    expect(onResult).toHaveBeenCalledTimes(1);
    const [, info] = onResult.mock.calls[0] as unknown as [string, OperatorExecutionSummary];
    expect(info.state).toBe(OperatorState.KILLED);
    expect(info.errorMessages[0].message).toContain("killed (timeout)");
  });

  test("surfaces general errors when the HTTP request itself fails (ERROR state)", async () => {
    const { state, target } = makeLinearState();
    setFetchResolving(new Response("backend detail", { status: 500, statusText: "Server Error" }));

    const out = await executeOperatorAndFormat(state, makeConfig(), target);
    expect(out).toContain("[ERROR]");
    expect(out).toContain("Execution request failed: 500");
    expect(out).toContain("backend detail");
  });

  test("surfaces the message when fetch rejects with a non-abort error", async () => {
    const { state, target } = makeLinearState();
    setFetchRejecting(new Error("network down"));

    const out = await executeOperatorAndFormat(state, makeConfig(), target);
    expect(out).toContain("[ERROR]");
    expect(out).toContain("network down");
  });

  test("returns a 'no result' error when the operator is missing from a successful run", async () => {
    const { state, target } = makeLinearState();
    const summary: WorkflowExecutionSummary = {
      success: true,
      state: WorkflowExecutionState.COMPLETED,
      operators: {},
      errors: [],
    };
    setFetchResolving(jsonResponse(summary));

    const out = await executeOperatorAndFormat(state, makeConfig(), target);
    expect(out).toContain("[ERROR]");
    expect(out).toContain(`No result found for operator: ${target}`);
  });

  test("joins operator error messages when a successful run still reports operator errors", async () => {
    const { state, target } = makeLinearState();
    const summary: WorkflowExecutionSummary = {
      success: true,
      state: WorkflowExecutionState.COMPLETED,
      operators: {
        [target]: { state: OperatorState.FAILED, errorMessages: [makeFatal("e1"), makeFatal("e2")] },
      },
      errors: [],
    };
    setFetchResolving(jsonResponse(summary));

    const onResult = mock(() => {});
    const out = await executeOperatorAndFormat(state, makeConfig(), target, { onResult });
    expect(out).toContain("[ERROR]");
    expect(out).toContain("e1; e2");
    expect(onResult).toHaveBeenCalledTimes(1);
  });
});

describe("executeOperatorAndFormat - abort and callback failures", () => {
  test("re-throws AbortError raised by the HTTP layer", async () => {
    const { state, target } = makeLinearState();
    const abortErr = new Error("aborted");
    abortErr.name = "AbortError";
    setFetchRejecting(abortErr);

    await expect(executeOperatorAndFormat(state, makeConfig(), target)).rejects.toThrow("aborted");
  });

  test("wraps a throwing onResult callback into an error result", async () => {
    const { state, target } = makeLinearState();
    const summary: WorkflowExecutionSummary = {
      success: true,
      state: WorkflowExecutionState.COMPLETED,
      operators: {
        [target]: {
          state: OperatorState.COMPLETED,
          errorMessages: [],
          resultSummary: {
            resultMode: OperatorResultMode.TABLE,
            sampleTuples: [{ rowIndex: 0, tuple: { col: "v" } }],
            tuplesCount: 1,
          },
        },
      },
      errors: [],
    };
    setFetchResolving(jsonResponse(summary));

    const onResult = () => {
      throw new Error("cb boom");
    };
    const out = await executeOperatorAndFormat(state, makeConfig(), target, { onResult });
    expect(out).toContain("[ERROR] Execution failed: cb boom");
  });
});

describe("createExecuteOperatorTool", () => {
  test("builds a tool whose execute delegates to executeOperatorAndFormat", async () => {
    const { state, target } = makeLinearState();
    const summary: WorkflowExecutionSummary = {
      success: true,
      state: WorkflowExecutionState.COMPLETED,
      operators: {
        [target]: {
          state: OperatorState.COMPLETED,
          errorMessages: [],
          resultSummary: {
            resultMode: OperatorResultMode.TABLE,
            sampleTuples: [{ rowIndex: 0, tuple: { col: "v" } }],
            tuplesCount: 1,
          },
        },
      },
      errors: [],
    };
    setFetchResolving(jsonResponse(summary));

    const onResult = mock(() => {});
    const executeTool = createExecuteOperatorTool(state, () => makeConfig({ computingUnitId: 2 }), onResult);
    expect(typeof executeTool.execute).toBe("function");

    const out = await (executeTool.execute as (a: { operatorId: string }, o: object) => Promise<string>)(
      { operatorId: target },
      {}
    );
    expect(out).toContain(`Executed operator ${target}`);
    expect(onResult).toHaveBeenCalledWith(target, expect.anything());
  });
});
