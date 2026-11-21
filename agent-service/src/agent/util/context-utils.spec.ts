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
import { dataflowMessage } from "./context-utils";
import { WorkflowState } from "../workflow-state";
import type { OperatorLink, OperatorPredicate } from "../../types/workflow";
import type { WorkflowCompilationResponse } from "../../api/compile-api";
import { WorkflowFatalErrorType } from "../../types/execution";

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

// The dataflow block is one user message whose content is a string.
function contentOf(message: ModelMessage | null): string {
  expect(message).not.toBeNull();
  expect(message!.role).toBe("user");
  expect(typeof message!.content).toBe("string");
  return message!.content as string;
}

describe("dataflowMessage", () => {
  test("is absent for an empty workflow", () => {
    expect(dataflowMessage(new WorkflowState(), new Map())).toBeNull();
  });

  test("is one user message headed Current Dataflow, with the operators under it", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));
    const content = contentOf(dataflowMessage(workflowState, new Map()));
    expect(content).toStartWith("# Current Dataflow\n## Operators\n");
    expect(content).toContain("### Operator `op1` (TestOp, not-executed)");
  });
});

describe("dataflowMessage — serialization", () => {
  test("orders operators and links topologically rather than by insertion order", () => {
    const workflowState = new WorkflowState();
    // Inserted leaf-first, so insertion order is the reverse of the dataflow order.
    workflowState.addOperator(makeOperator("c"));
    workflowState.addOperator(makeOperator("b"));
    workflowState.addOperator(makeOperator("a"));
    workflowState.addLink(makeLink("b", "output-0", "c", "input-0"));
    workflowState.addLink(makeLink("a", "output-0", "c", "input-1"));
    workflowState.addLink(makeLink("a", "output-0", "b", "input-0"));

    const content = contentOf(dataflowMessage(workflowState, new Map()));

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

    const content = contentOf(dataflowMessage(workflowState, new Map(), false, compilationResult));

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

    const content = contentOf(dataflowMessage(workflowState, new Map()));

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

    const redacted = contentOf(dataflowMessage(workflowState, new Map(), true));
    expect(redacted).toContain("(TestOp, not-executed)");
    expect(redacted).not.toContain("Properties:");

    // A failing operator keeps its properties even when redacting, so the model
    // can see what caused the failure.
    const failed = contentOf(dataflowMessage(workflowState, new Map([["op1", "[ERROR] boom"]]), true));
    expect(failed).toContain("(TestOp, failed)");
    expect(failed).toContain("  secret: s3cr3t");
  });

  test("marks an operator executed and indents every line of its result", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1", { customDisplayName: "My Filter" }));

    const content = contentOf(dataflowMessage(workflowState, new Map([["op1", "header\nrow1"]])));

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

    const content = contentOf(dataflowMessage(workflowState, new Map(), false, compilationResult));

    expect(content).toContain("Compilation Error: attribute x not found");
  });
});
