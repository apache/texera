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
      operatorErrors: { op1: { type: "CompilationError", message: "attribute x not found" } },
    };

    const content = contentOf(assembleContext([], workflowState, new Map(), false, compilationResult));

    expect(content).toContain("Compilation Error: attribute x not found");
  });
});
