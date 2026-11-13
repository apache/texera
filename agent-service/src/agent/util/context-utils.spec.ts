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
import type { ModelMessage } from "ai";
import { assembleContext } from "./context-utils";
import { WorkflowState } from "../workflow-state";
import type { ReActStep } from "../../types/agent";
import type { OperatorLink, OperatorPredicate } from "../../types/workflow";
import type { WorkflowCompilationResponse } from "../../api/compile-api";
import { WorkflowFatalErrorType, type LastRunReport } from "../../types/execution";

function step(messageId: string, role: "user" | "agent", stepId: number, content: string, isEnd: boolean): ReActStep {
  return {
    id: `${messageId}-${stepId}`,
    messageId,
    stepId,
    timestamp: 0,
    role,
    content,
    isBegin: true,
    isEnd,
  };
}

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

// assembleContext always returns a single user message whose content is a joined string.
function contentOf(result: ModelMessage[]): string {
  expect(result).toHaveLength(1);
  expect(result[0].role).toBe("user");
  expect(typeof result[0].content).toBe("string");
  return result[0].content as string;
}

describe("assembleContext", () => {
  test("returns a single user message with no task sections for empty steps", () => {
    const result = assembleContext([], new WorkflowState(), new Map());
    expect(result).toHaveLength(1);
    expect(result[0].role).toBe("user");
    expect(contentOf(result)).toBe("");
  });

  test("renders a completed task under a Completed Tasks heading", () => {
    const steps = [step("m1", "user", 0, "do X", true), step("m1", "agent", 1, "thinking", true)];
    const content = contentOf(assembleContext(steps, new WorkflowState(), new Map()));
    expect(content).toContain("# Completed Tasks");
    expect(content).toContain("## Task (completed)");
    expect(content).toContain("do X"); // the user request
    expect(content).toContain("### Turn 1"); // the agent step
  });

  test("renders an unfinished task under an Ongoing Task heading", () => {
    const steps = [step("m1", "user", 0, "do Y", true), step("m1", "agent", 1, "working", false)];
    const content = contentOf(assembleContext(steps, new WorkflowState(), new Map()));
    expect(content).toContain("# Ongoing Task");
    expect(content).toContain("## Task (ongoing)");
  });

  test("appends a Current Dataflow section when the workflow has operators", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));
    const content = contentOf(assembleContext([], workflowState, new Map()));
    expect(content).toContain("# Current Dataflow");
    expect(content).toContain("## Operators");
  });

  test("groups steps sharing a messageId into a single task", () => {
    const steps = [
      step("m1", "user", 0, "req", true),
      step("m1", "agent", 1, "turn one", false),
      step("m1", "agent", 2, "turn two", true),
    ];
    const content = contentOf(assembleContext(steps, new WorkflowState(), new Map()));
    expect((content.match(/## Task/g) ?? []).length).toBe(1);
    expect(content).toContain("### Turn 1");
    expect(content).toContain("### Turn 2");
  });

  test("labels each tool call with the status of the result at the same index", () => {
    const agentStep: ReActStep = {
      ...step("m1", "agent", 1, "acting", true),
      toolCalls: [
        { toolName: "addOperator", toolCallId: "c1", input: {} },
        { toolName: "executeOperator", toolCallId: "c2", input: {} },
        { toolName: "deleteOperator", toolCallId: "c3", input: {} },
      ],
      // Deliberately shorter than toolCalls: a call still awaiting its result
      // must not be reported as failed.
      toolResults: [
        { toolCallId: "c1", output: "ok" },
        { toolCallId: "c2", output: "bad", isError: true },
      ],
    };
    const content = contentOf(
      assembleContext([step("m1", "user", 0, "req", true), agentStep], new WorkflowState(), new Map())
    );
    expect(content).toContain("- addOperator (succeeded)");
    expect(content).toContain("- executeOperator (failed)");
    expect(content).toContain("- deleteOperator (succeeded)");
  });
});

describe("assembleContext — dataflow serialization", () => {
  test("orders operators and links topologically rather than by insertion order", () => {
    const workflowState = new WorkflowState();
    // Inserted leaf-first, so insertion order is the reverse of the dataflow order.
    workflowState.addOperator(makeOperator("c"));
    workflowState.addOperator(makeOperator("b"));
    workflowState.addOperator(makeOperator("a"));
    workflowState.addLink(makeLink("b", "output-0", "c", "input-0"));
    workflowState.addLink(makeLink("a", "output-0", "c", "input-1"));
    workflowState.addLink(makeLink("a", "output-0", "b", "input-0"));

    const content = contentOf(assembleContext([], workflowState, new Map()));

    expect(content.indexOf("### Operator `a`")).toBeLessThan(content.indexOf("### Operator `b`"));
    expect(content.indexOf("### Operator `b`")).toBeLessThan(content.indexOf("### Operator `c`"));
    // Links are sorted by source rank, then by target rank — a → b precedes a → c.
    const linkLines = content.slice(content.indexOf("## Links")).split("\n").slice(1);
    expect(linkLines).toEqual(["- a → b", "- a → c", "- b → c"]);
  });

  test("renders input schemas by port ordinal and falls through to the first defined output schema", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("src", { outputPorts: [{ portID: "output-0" }] }));
    workflowState.addOperator(makeOperator("dst", { inputPorts: [{ portID: "input-0" }] }));
    workflowState.addLink(makeLink("src", "output-0", "dst", "input-0"));

    const compilationResult: WorkflowCompilationResponse = {
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

    const content = contentOf(assembleContext([], workflowState, new Map(), false, compilationResult));

    expect(content).toContain("Input Schema (port 0): [a: string, n: integer]");
    expect(content).toContain("Output Schema: [a: string, n: integer]");
    expect(content).toContain("Output Schema: [flag: boolean]");
    // The wire port id "0_false" is trimmed down to the bare ordinal.
    expect(content).not.toContain("port 0_false");
  });

  test("renders non-empty properties and JSON-encodes non-string values", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(
      makeOperator("op1", {
        operatorProperties: { name: "alice", limit: 5, tags: ["x", "y"], blank: "", missing: null, absent: undefined },
      })
    );

    const content = contentOf(assembleContext([], workflowState, new Map()));

    expect(content).toContain("Summary: op1"); // no display name -> falls back to the id
    expect(content).toContain("  name: alice");
    expect(content).toContain("  limit: 5");
    expect(content).toContain('  tags: ["x","y"]');
    expect(content).not.toContain("blank");
    expect(content).not.toContain("missing");
    expect(content).not.toContain("absent");
  });

  test("omits properties under redaction unless the operator's result reports an error", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1", { operatorProperties: { secret: "s3cr3t" } }));

    const redacted = contentOf(assembleContext([], workflowState, new Map(), true));
    expect(redacted).toContain("(TestOp, not-executed)");
    expect(redacted).not.toContain("Properties:");

    // A failing operator keeps its properties even when redacting, so the model
    // can see what caused the failure.
    const failed = contentOf(assembleContext([], workflowState, new Map([["op1", "[ERROR] boom"]]), true));
    expect(failed).toContain("(TestOp, failed)");
    expect(failed).toContain("  secret: s3cr3t");
  });

  test("marks an operator executed and indents every line of its result", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1", { customDisplayName: "My Filter" }));

    const content = contentOf(assembleContext([], workflowState, new Map([["op1", "header\nrow1"]])));

    expect(content).toContain("### Operator `op1` (TestOp, executed)");
    expect(content).toContain("Summary: My Filter");
    expect(content).toContain("Result:\n  header\n  row1");
  });

  test("surfaces a per-operator compilation error", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));
    const compilationResult: WorkflowCompilationResponse = {
      operatorOutputSchemas: {},
      operatorErrors: {
        op1: {
          type: { name: WorkflowFatalErrorType.COMPILATION_ERROR },
          timestamp: { seconds: 0, nanos: 0 },
          message: "attribute x not found",
          details: "",
          operatorId: "op1",
          workerId: "",
        },
      },
    };

    const content = contentOf(assembleContext([], workflowState, new Map(), false, compilationResult));

    expect(content).toContain("Compilation Error: attribute x not found");
  });
});

describe("assembleContext — last run report", () => {
  const failedRun: LastRunReport = {
    state: "Failed",
    operatorErrors: [{ operatorId: "op1", message: "ValueError: bad row\n  at line 3" }],
    workflowErrors: [],
  };

  test("renders the run error under the operator that produced it and nowhere else", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));
    workflowState.addOperator(makeOperator("op2"));

    const content = contentOf(assembleContext([], workflowState, new Map(), false, null, failedRun));

    const op1 = content.indexOf("### Operator `op1`");
    const op2 = content.indexOf("### Operator `op2`");
    const err = content.indexOf("Last Run Error (from the user's run): ValueError: bad row\n    at line 3");
    expect(err).toBeGreaterThan(op1);
    expect(err).toBeLessThan(op2);
    expect(content.match(/Last Run Error/g)).toHaveLength(1);
  });

  test("marks an operator with a run error as failed even without an agent result", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));
    workflowState.addOperator(makeOperator("op2"));

    const content = contentOf(assembleContext([], workflowState, new Map(), false, null, failedRun));

    expect(content).toContain("### Operator `op1` (TestOp, failed)");
    expect(content).toContain("### Operator `op2` (TestOp, not-executed)");
  });

  test("shows properties under redaction when the only error is from the user's run", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1", { operatorProperties: { code: "print(1)" } }));

    const redacted = contentOf(assembleContext([], workflowState, new Map(), true));
    expect(redacted).not.toContain("code: print(1)");

    const withRunError = contentOf(assembleContext([], workflowState, new Map(), true, null, failedRun));
    expect(withRunError).toContain("code: print(1)");
  });

  test("appends a Last Run block with the state and workflow-level errors", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));
    const report: LastRunReport = {
      state: "Killed",
      operatorErrors: [],
      workflowErrors: ["computing unit out of memory", "worker lost"],
    };

    const content = contentOf(assembleContext([], workflowState, new Map(), false, null, report));

    expect(content).toContain(
      "## Last Run\nState: Killed\nLast Run Error (from the user's run), not tied to one operator:\n- computing unit out of memory\n- worker lost"
    );
    expect(content.indexOf("## Last Run")).toBeGreaterThan(content.indexOf("## Operators"));
  });

  test("says the run finished cleanly when both error lists are empty", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));
    const report: LastRunReport = { state: "Completed", operatorErrors: [], workflowErrors: [] };

    const content = contentOf(assembleContext([], workflowState, new Map(), false, null, report));

    expect(content).toContain("## Last Run\nState: Completed\nNo errors reported.");
    expect(content).not.toContain("Last Run Error");
  });

  test("renders no Last Run block when no report is given", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));

    const content = contentOf(assembleContext([], workflowState, new Map(), false, null));

    expect(content).not.toContain("## Last Run");
    expect(content).not.toContain("Last Run Error");
  });

  test("lists the errors of operators the agent does not know, with their messages", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));
    const report: LastRunReport = {
      state: "Failed",
      operatorErrors: [
        { operatorId: "udf-1", message: "boom" },
        { operatorId: "__DummyOperator", message: "bang" },
      ],
      workflowErrors: [],
    };

    const content = contentOf(assembleContext([], workflowState, new Map(), false, null, report));

    expect(
      content.endsWith(
        "## Last Run\nState: Failed\n" +
          "Last Run Error (from the user's run), for operators not in the workflow as you see it:\n" +
          "- udf-1: boom\n- __DummyOperator: bang"
      )
    ).toBe(true);
    expect(content).toContain("### Operator `op1` (TestOp, not-executed)");
    expect(content).not.toContain("No errors reported.");
    expect(content).not.toContain("no longer in the workflow");
  });

  test("lists an unknown operator's error next to one shown under a known operator", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("scan-1"));
    const report: LastRunReport = {
      state: "Failed",
      operatorErrors: [
        { operatorId: "scan-1", message: "file not found" },
        { operatorId: "udf-1", message: "ValueError: bad row" },
      ],
      workflowErrors: ["worker lost"],
    };

    const content = contentOf(assembleContext([], workflowState, new Map(), false, null, report));

    expect(content).toContain("### Operator `scan-1` (TestOp, failed)");
    expect(content).toContain("Last Run Error (from the user's run): file not found");
    expect(
      content.endsWith(
        "## Last Run\nState: Failed\n" +
          "Last Run Error (from the user's run), for operators not in the workflow as you see it:\n" +
          "- udf-1: ValueError: bad row\n" +
          "Last Run Error (from the user's run), not tied to one operator:\n" +
          "- worker lost"
      )
    ).toBe(true);
    expect(content.match(/file not found/g)).toHaveLength(1);
    expect(content).not.toContain("No errors reported.");
  });

  test("renders the Last Run block when the agent's workflow has no operators", () => {
    const report: LastRunReport = {
      state: "Running",
      operatorErrors: [{ operatorId: "udf-1", message: "boom" }],
      workflowErrors: ["worker lost"],
      operatorStats: [{ operatorId: "udf-1", state: "Paused", inputRows: 1, outputRows: 0 }],
    };

    const content = contentOf(assembleContext([], new WorkflowState(), new Map(), false, null, report));

    expect(
      content.endsWith(
        "# Current Dataflow\n## Last Run\nState: Running (not finished)\n" +
          "Last Run Error (from the user's run), for operators not in the workflow as you see it:\n" +
          "- udf-1: boom\n" +
          "Last Run Error (from the user's run), not tied to one operator:\n" +
          "- worker lost"
      )
    ).toBe(true);
    expect(content).not.toContain("## Operators");
    expect(content).not.toContain("Last Run (from the user's run):");
  });

  test("says no errors were reported for an empty workflow and a clean report", () => {
    const report: LastRunReport = { state: "Completed", operatorErrors: [], workflowErrors: [] };

    const content = contentOf(assembleContext([], new WorkflowState(), new Map(), false, null, report));

    expect(content.endsWith("# Current Dataflow\n## Last Run\nState: Completed\nNo errors reported.")).toBe(true);
  });

  test("renders no dataflow section for an empty workflow without a report", () => {
    const content = contentOf(assembleContext([], new WorkflowState(), new Map(), false, null));

    expect(content).not.toContain("# Current Dataflow");
    expect(content).not.toContain("## Last Run");
  });

  test("indents the continuation lines of a multi-line run error", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));
    const report: LastRunReport = {
      state: "Failed",
      operatorErrors: [
        { operatorId: "op1", message: "ValueError: bad row\nTraceback\n  File udf.py, line 6" },
        { operatorId: "udf-1", message: "KeyError: x\nTraceback" },
      ],
      workflowErrors: ["worker lost\nstack line"],
    };

    const content = contentOf(assembleContext([], workflowState, new Map(), false, null, report));

    expect(content).toContain(
      "Last Run Error (from the user's run): ValueError: bad row\n  Traceback\n    File udf.py, line 6\n"
    );
    expect(content).toContain("- udf-1: KeyError: x\n  Traceback\n");
    expect(content.endsWith("- worker lost\n  stack line")).toBe(true);
  });

  test("does not say no errors were reported when an operator error is shown", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));

    const content = contentOf(assembleContext([], workflowState, new Map(), false, null, failedRun));

    expect(content).toContain("Last Run Error (from the user's run)");
    expect(content).not.toContain("No errors reported.");
    expect(content).not.toContain("not in the workflow as you see it");
  });

  test("marks an operator with a run error as failed and leaves out the agent's older result for it", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1", { operatorProperties: { code: "print(1)" } }));
    const results = new Map([["op1", "header\nrow1"]]);

    const content = contentOf(assembleContext([], workflowState, results, true, null, failedRun));

    expect(content).toContain("### Operator `op1` (TestOp, failed)");
    expect(content).toContain("Last Run Error (from the user's run): ValueError: bad row");
    expect(content).toContain("code: print(1)");
    expect(content).not.toContain("Result:");
    expect(content).not.toContain("row1");
  });

  test("leaves out the agent's older failed result for an operator with a run error", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));
    const results = new Map([["op1", "[ERROR] KeyError: stale"]]);

    const content = contentOf(assembleContext([], workflowState, results, false, null, failedRun));

    expect(content).toContain("### Operator `op1` (TestOp, failed)");
    expect(content).toContain("Last Run Error (from the user's run): ValueError: bad row");
    expect(content).not.toContain("Result:");
    expect(content).not.toContain("KeyError: stale");
  });

  test("keeps the agent's result and status for an operator the report has no error for", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));
    workflowState.addOperator(makeOperator("op2"));
    workflowState.addOperator(makeOperator("op3"));
    const results = new Map([
      ["op2", "header\nrow1"],
      ["op3", "[ERROR] KeyError: mine"],
    ]);
    const report: LastRunReport = {
      ...failedRun,
      operatorStats: [{ operatorId: "op2", state: "Completed", inputRows: 3, outputRows: 3 }],
    };

    const content = contentOf(assembleContext([], workflowState, results, false, null, report));

    expect(content).toContain("### Operator `op1` (TestOp, failed)");
    expect(content).toContain("### Operator `op2` (TestOp, executed)");
    expect(content).toContain(
      "Last Run (from the user's run): Completed, input rows 3, output rows 3\nResult:\n  header\n  row1"
    );
    expect(content).toContain("### Operator `op3` (TestOp, failed)");
    expect(content).toContain("Result:\n  [ERROR] KeyError: mine");
    expect(content.match(/Result:/g)).toHaveLength(2);
  });

  test("renders the row counts of the user's run under the operator they belong to and nowhere else", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));
    workflowState.addOperator(makeOperator("op2"));
    const report: LastRunReport = {
      state: "Completed",
      operatorErrors: [],
      workflowErrors: [],
      operatorStats: [{ operatorId: "op1", state: "Completed", inputRows: 5000, outputRows: 0 }],
    };

    const content = contentOf(assembleContext([], workflowState, new Map(), false, null, report));

    const op1 = content.indexOf("### Operator `op1`");
    const op2 = content.indexOf("### Operator `op2`");
    const line = content.indexOf("Last Run (from the user's run): Completed, input rows 5000, output rows 0");
    expect(line).toBeGreaterThan(op1);
    expect(line).toBeLessThan(op2);
    expect(content.match(/Last Run \(from the user's run\)/g)).toHaveLength(1);
  });

  test("row counts alone change neither the status nor what redaction hides", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1", { operatorProperties: { code: "print(1)" } }));
    const report: LastRunReport = {
      state: "Completed",
      operatorErrors: [],
      workflowErrors: [],
      operatorStats: [{ operatorId: "op1", state: "Completed", inputRows: 3, outputRows: 0 }],
    };

    const content = contentOf(assembleContext([], workflowState, new Map(), true, null, report));

    expect(content).toContain("### Operator `op1` (TestOp, not-executed)");
    expect(content).not.toContain("code: print(1)");
    expect(content).toContain("Last Run (from the user's run): Completed, input rows 3, output rows 0");
    expect(content).toContain("## Last Run\nState: Completed\nNo errors reported.");
  });

  test("reports a cache-reused operator without row counts", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));
    const report: LastRunReport = {
      state: "Completed",
      operatorErrors: [],
      workflowErrors: [],
      operatorStats: [{ operatorId: "op1", state: "Completed", inputRows: 0, outputRows: 0, reusedFromCache: true }],
    };

    const content = contentOf(assembleContext([], workflowState, new Map(), false, null, report));

    expect(content).toContain("Last Run (from the user's run): Completed, reused from cache, row counts not reported");
    expect(content).not.toContain("output rows 0");
  });

  test("ignores row counts of operators that are no longer in the workflow", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));
    const report: LastRunReport = {
      state: "Completed",
      operatorErrors: [],
      workflowErrors: [],
      operatorStats: [{ operatorId: "gone", state: "Completed", inputRows: 7, outputRows: 7 }],
    };

    const content = contentOf(assembleContext([], workflowState, new Map(), false, null, report));

    expect(content).not.toContain("Last Run (from the user's run)");
    expect(content).not.toContain("gone");
    expect(content).toContain("## Last Run\nState: Completed\nNo errors reported.");
  });

  test("shows both the row counts and the error of an operator that failed", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));
    workflowState.addOperator(makeOperator("op2"));
    const report: LastRunReport = {
      ...failedRun,
      operatorStats: [
        { operatorId: "op1", state: "Running", inputRows: 12, outputRows: 4 },
        { operatorId: "op2", state: "Uninitialized", inputRows: 0, outputRows: 0 },
      ],
    };

    const content = contentOf(assembleContext([], workflowState, new Map(), false, null, report));

    const op1 = content.indexOf("### Operator `op1`");
    const op2 = content.indexOf("### Operator `op2`");
    const stats = content.indexOf("Last Run (from the user's run): Running, input rows 12, output rows 4");
    const error = content.indexOf("Last Run Error (from the user's run): ValueError: bad row");
    expect(stats).toBeGreaterThan(op1);
    expect(error).toBeGreaterThan(stats);
    expect(error).toBeLessThan(op2);
    expect(content).toContain("Last Run (from the user's run): Uninitialized, input rows 0, output rows 0");
    expect(content).toContain("### Operator `op1` (TestOp, failed)");
    expect(content).toContain("### Operator `op2` (TestOp, not-executed)");
  });

  test("marks the state of a run that is still in flight as not finished", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));

    for (const state of ["Running", "Pausing", "Paused", "Resuming", "Recovering"] as const) {
      const content = contentOf(assembleContext([], workflowState, new Map(), false, null, { ...failedRun, state }));

      expect(content.endsWith(`## Last Run\nState: ${state} (not finished)`)).toBe(true);
    }
  });

  test("renders the state of a finished run without the not-finished mark", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));

    for (const state of ["Completed", "Failed", "Killed", "Terminated"] as const) {
      const content = contentOf(assembleContext([], workflowState, new Map(), false, null, { ...failedRun, state }));

      expect(content.endsWith(`## Last Run\nState: ${state}`)).toBe(true);
      expect(content).not.toContain("not finished");
    }
  });

  test("shows the error and the row counts so far of an operator that paused on an error", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));
    workflowState.addOperator(makeOperator("op2"));
    const report: LastRunReport = {
      state: "Running",
      operatorErrors: [{ operatorId: "op2", message: "ValueError: column price is missing" }],
      workflowErrors: [],
      operatorStats: [
        { operatorId: "op1", state: "Completed", inputRows: 0, outputRows: 1 },
        { operatorId: "op2", state: "Paused", inputRows: 1, outputRows: 0 },
      ],
    };

    const content = contentOf(assembleContext([], workflowState, new Map(), false, null, report));

    expect(content).toContain("### Operator `op1` (TestOp, not-executed)");
    expect(content).toContain("### Operator `op2` (TestOp, failed)");
    expect(content).toContain("Last Run (from the user's run): Completed, input rows 0, output rows 1");
    const stats = content.indexOf("Last Run (from the user's run): Paused, input rows 1, output rows 0");
    const error = content.indexOf("Last Run Error (from the user's run): ValueError: column price is missing");
    expect(stats).toBeGreaterThan(content.indexOf("### Operator `op2`"));
    expect(error).toBeGreaterThan(stats);
    expect(content.endsWith("## Last Run\nState: Running (not finished)")).toBe(true);
  });

  test("says no errors were reported for an in-flight report that carries none", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));
    const report: LastRunReport = { state: "Paused", operatorErrors: [], workflowErrors: [] };

    const content = contentOf(assembleContext([], workflowState, new Map(), false, null, report));

    expect(content).toContain("## Last Run\nState: Paused (not finished)\nNo errors reported.");
  });

  test("renders no row count line when the report carries no stats", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));

    const content = contentOf(assembleContext([], workflowState, new Map(), false, null, failedRun));

    expect(content).not.toContain("Last Run (from the user's run)");
  });
});
