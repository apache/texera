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
import { WorkflowState } from "../workflow-state";
import type { OperatorLink, OperatorPredicate } from "../../types/workflow";
import type { WorkflowCompilationResponse, WorkflowFatalError } from "../../api/compile-api";
import {
  OperatorState,
  WorkflowFatalErrorType,
  type OperatorExecutionSummary,
  type Tuple,
} from "../../types/execution";
import {
  createGetOperatorResultTool,
  createGetWorkflowTool,
  describeWorkflow,
  operatorResult,
  type WorkflowReadContext,
} from "./workflow-read-tools";
import { formatOperatorOutput } from "./tools-utility";

function makeOperator(id: string, overrides: Partial<OperatorPredicate> = {}): OperatorPredicate {
  return {
    operatorID: id,
    operatorType: "TestOp",
    operatorVersion: "1.0",
    operatorProperties: {},
    inputPorts: [],
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

function fatalError(message: string, operatorId = "op1"): WorkflowFatalError {
  return {
    type: { name: WorkflowFatalErrorType.EXECUTION_FAILURE },
    timestamp: { seconds: 0, nanos: 0 },
    message,
    details: "",
    operatorId,
    workerId: "",
  };
}

function tuple(row: Record<string, unknown>): Tuple {
  return {
    schema: { attributes: Object.keys(row).map(name => ({ attributeName: name, attributeType: "string" })) },
    fields: Object.values(row),
  };
}

function completed(rows: Record<string, unknown>[], total = rows.length): OperatorExecutionSummary {
  return {
    state: OperatorState.COMPLETED,
    errorMessages: [],
    resultSummary: {
      resultMode: { type: "PaginationMode" },
      sampleTuples: rows.map((row, i) => [i, tuple(row)] as const),
      totalTuplesCount: total,
    },
  };
}

function failed(message: string): OperatorExecutionSummary {
  return { state: OperatorState.FAILED, errorMessages: [fatalError(message)] };
}

/** A failure as the execute tool stores it: a fixed header, a section label, then the traceback. */
const PYTHON_FAILURE = [
  "Execution failed due to the following error:",
  "Execution error:",
  "  op1: Traceback (most recent call last):",
  '  File "udf.py", line 3, in process_tuple',
  "    yield tuple_['a'] / 0",
  "ZeroDivisionError: division by zero",
].join("\n");

/** A read context over `workflowState`; compiling answers with `compilation` unless told otherwise. */
function contextOf(
  workflowState: WorkflowState,
  options: {
    compilation?: WorkflowCompilationResponse | null;
    compileError?: Error;
    results?: Record<string, OperatorExecutionSummary>;
    outdated?: string[];
    charLimit?: number;
  } = {}
): WorkflowReadContext & { compiles: () => number } {
  let compiles = 0;
  return {
    workflowState,
    compileWorkflow: async () => {
      compiles++;
      if (options.compileError) throw options.compileError;
      return options.compilation === undefined
        ? { operatorOutputSchemas: {}, operatorErrors: {} }
        : options.compilation;
    },
    getResult: operatorId => {
      const summary = options.results?.[operatorId];
      return summary ? { summary, outdated: (options.outdated ?? []).includes(operatorId) } : undefined;
    },
    getResultCharLimit: () => options.charLimit,
    compiles: () => compiles,
  };
}

describe("getWorkflow", () => {
  test("says the workflow is empty, without compiling it", async () => {
    const context = contextOf(new WorkflowState());
    expect(await describeWorkflow(context)).toBe("The workflow is empty.");
    expect(context.compiles()).toBe(0);
  });

  test("lists every operator under its id and type, then the links", async () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));
    const listing = await describeWorkflow(contextOf(workflowState));
    expect(listing).toStartWith("## Operators\n\n### Operator `op1` (TestOp)\nSummary: op1\n");
  });

  test("orders operators and links topologically rather than by insertion order", async () => {
    const workflowState = new WorkflowState();
    // Inserted leaf-first, so insertion order is the reverse of the dataflow order.
    workflowState.addOperator(makeOperator("c"));
    workflowState.addOperator(makeOperator("b"));
    workflowState.addOperator(makeOperator("a"));
    workflowState.addLink(makeLink("b", "output-0", "c", "input-0"));
    workflowState.addLink(makeLink("a", "output-0", "c", "input-1"));
    workflowState.addLink(makeLink("a", "output-0", "b", "input-0"));

    const listing = await describeWorkflow(contextOf(workflowState));

    expect(listing.indexOf("### Operator `a`")).toBeLessThan(listing.indexOf("### Operator `b`"));
    expect(listing.indexOf("### Operator `b`")).toBeLessThan(listing.indexOf("### Operator `c`"));
    // Links are sorted by source rank, then by target rank — a → b precedes a → c.
    const linkLines = listing.slice(listing.indexOf("## Links")).split("\n").slice(1);
    expect(linkLines).toEqual(["- a → b", "- a → c", "- b → c"]);
  });

  test("renders input schemas by port ordinal and falls through to the first defined output schema", async () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("src", { outputPorts: [{ portID: "output-0" }] }));
    workflowState.addOperator(makeOperator("dst", { inputPorts: [{ portID: "input-0" }] }));
    workflowState.addLink(makeLink("src", "output-0", "dst", "input-0"));

    const compilation: WorkflowCompilationResponse = {
      operatorOutputSchemas: {
        src: {
          "0_false": [
            { attributeName: "a", attributeType: "string" },
            { attributeName: "n", attributeType: "integer" },
          ],
        },
        // Port 0 carries no schema, so the output line must come from port 1.
        dst: { "0_false": undefined, "1_false": [{ attributeName: "flag", attributeType: "boolean" }] },
      },
      operatorErrors: {},
    };

    const listing = await describeWorkflow(contextOf(workflowState, { compilation }));

    expect(listing).toContain("Input Schema (port 0): [a: string, n: integer]");
    expect(listing).toContain("Output Schema: [a: string, n: integer]");
    expect(listing).toContain("Output Schema: [flag: boolean]");
    // The wire port id "0_false" is trimmed down to the bare ordinal.
    expect(listing).not.toContain("port 0_false");
  });

  test("renders non-empty properties and JSON-encodes non-string values", async () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(
      makeOperator("op1", {
        customDisplayName: "My Filter",
        operatorProperties: { name: "alice", limit: 5, tags: ["x", "y"], blank: "", missing: null, absent: undefined },
      })
    );

    const listing = await describeWorkflow(contextOf(workflowState));

    expect(listing).toContain("Summary: My Filter");
    expect(listing).toContain("  name: alice");
    expect(listing).toContain("  limit: 5");
    expect(listing).toContain('  tags: ["x","y"]');
    expect(listing).not.toContain("blank");
    expect(listing).not.toContain("missing");
    expect(listing).not.toContain("absent");
  });

  test("surfaces a per-operator compilation error", async () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));
    const compilation: WorkflowCompilationResponse = {
      operatorOutputSchemas: {},
      operatorErrors: {
        op1: { ...fatalError("attribute x not found"), type: { name: WorkflowFatalErrorType.COMPILATION_ERROR } },
      },
    };

    const listing = await describeWorkflow(contextOf(workflowState, { compilation }));

    expect(listing).toContain("Compilation Error: attribute x not found");
  });

  test("says why schemas are missing when compiling fails or gets no answer", async () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));

    const thrown = await describeWorkflow(contextOf(workflowState, { compileError: new Error("plan is broken") }));
    expect(thrown).toContain("### Operator `op1` (TestOp)");
    expect(thrown).toEndWith("No schemas: compiling the workflow failed (plan is broken).");

    const unanswered = await describeWorkflow(contextOf(workflowState, { compilation: null }));
    expect(unanswered).toEndWith("No schemas: compiling the workflow failed (the compile service gave no answer).");
  });

  test("tells for each operator whether it has a result, how big it is, or why it failed", async () => {
    const workflowState = new WorkflowState();
    for (const id of ["none", "table", "single", "zero", "bad", "empty", "stale"]) {
      workflowState.addOperator(makeOperator(id));
    }

    const listing = await describeWorkflow(
      contextOf(workflowState, {
        results: {
          table: completed([{ a: 1, b: 2 }], 150),
          single: completed([{ a: 1 }]),
          zero: completed([], 0),
          bad: failed(PYTHON_FAILURE),
          empty: { state: OperatorState.COMPLETED, errorMessages: [] },
          stale: completed([{ a: 1 }]),
          // A result for an operator no longer in the workflow is not listed.
          deleted: completed([{ a: 1 }]),
        },
        outdated: ["stale"],
      })
    );

    const resultOf = (id: string) => {
      const section = listing.slice(listing.indexOf(`### Operator \`${id}\``));
      return section.split("\n").find(line => line.startsWith("Result: "));
    };
    expect(resultOf("none")).toBe("Result: none, not executed in this conversation");
    expect(resultOf("table")).toBe("Result: 150 rows, 2 columns");
    expect(resultOf("single")).toBe("Result: 1 row, 1 column");
    expect(resultOf("zero")).toBe("Result: 0 rows");
    // The exception, not the failure text's fixed header.
    expect(resultOf("bad")).toBe("Result: failed: ZeroDivisionError: division by zero");
    expect(resultOf("empty")).toBe("Result: executed, no output table");
    expect(resultOf("stale")).toBe(
      "Result: outdated, it or an operator it reads from changed after its last execution"
    );
    expect(listing).not.toContain("deleted");
    // The records themselves are left to getOperatorResult.
    expect(listing).not.toContain("Output table shape");
  });

  test("is a tool that takes no input and returns the listing", async () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));
    const getWorkflow = createGetWorkflowTool(contextOf(workflowState));
    expect(getWorkflow.description).toContain("getOperatorResult");
    expect(await (getWorkflow as any).execute({}, {})).toStartWith("## Operators");
  });
});

describe("getOperatorResult", () => {
  const workflowState = new WorkflowState();
  for (const id of ["op1", "op2", "op3", "op4", "op5"]) workflowState.addOperator(makeOperator(id));
  const rows = Array.from({ length: 30 }, (_, i) => ({ id: i, name: `row-${i}-${"x".repeat(40)}` }));
  const context = contextOf(workflowState, {
    results: {
      op1: completed(rows),
      op2: failed(PYTHON_FAILURE),
      op4: { state: OperatorState.COMPLETED, errorMessages: [] },
      op5: completed(rows),
      gone: completed(rows),
    },
    outdated: ["op5"],
    charLimit: 300,
  });

  test("returns the latest result within the result size limit, without claiming an execution now", () => {
    const result = operatorResult(context, "op1");
    const executed = formatOperatorOutput("op1", completed(rows), 300);
    expect(executed).toStartWith("Executed operator op1\n");
    expect(result).toBe(executed.replace("Executed operator op1\n", "Latest result of operator op1:\n"));
    expect(result).toStartWith("Latest result of operator op1:\nOutput table shape: (30, 2)");
  });

  test("returns the whole error an operator failed with", () => {
    expect(operatorResult(context, "op2")).toBe(`Operator op2 failed when it was last executed:\n${PYTHON_FAILURE}`);
  });

  test("says when an operator has not been executed, or has no output table", () => {
    expect(operatorResult(context, "op3")).toBe(
      "Operator op3 has no result: it has not been executed in this conversation."
    );
    expect(operatorResult(context, "op4")).toBe("Operator op4 was executed and has no output table.");
  });

  test("withholds an outdated result and says to execute again", () => {
    expect(operatorResult(context, "op5")).toBe(
      "Operator op5's latest result is outdated: it or an operator it reads from changed after that execution. " +
        "Execute it again for a current result."
    );
  });

  test("fails for an operator that is not in the workflow, even with an old result", () => {
    expect(operatorResult(context, "gone")).toBe("[ERROR] Operator gone not found");
    expect(operatorResult(context, "nope")).toBe("[ERROR] Operator nope not found");
  });

  test("is a tool that takes the operator id", async () => {
    const getOperatorResult = createGetOperatorResultTool(context);
    expect(getOperatorResult.description).toContain("does not execute anything");
    expect(await (getOperatorResult as any).execute({ operatorId: "op3" }, {})).toBe(
      "Operator op3 has no result: it has not been executed in this conversation."
    );
  });
});
