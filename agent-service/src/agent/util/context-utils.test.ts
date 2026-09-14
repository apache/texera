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
import { assembleContext } from "./context-utils";
import { WorkflowState } from "../workflow-state";
import type { ReActStep } from "../../types/agent";
import type { OperatorPredicate, OperatorLink } from "../../types/workflow";

function userStep(messageId: string, content: string): ReActStep {
  return {
    id: `${messageId}-u`,
    messageId,
    stepId: 0,
    timestamp: 0,
    role: "user",
    content,
    isBegin: true,
    isEnd: true,
  };
}

function agentStep(messageId: string, opts: Partial<ReActStep> & { stepId: number }): ReActStep {
  return {
    id: `${messageId}-a${opts.stepId}`,
    messageId,
    stepId: opts.stepId,
    timestamp: 0,
    role: "agent",
    content: opts.content ?? "",
    isBegin: false,
    isEnd: opts.isEnd ?? false,
    toolCalls: opts.toolCalls,
    toolResults: opts.toolResults,
  };
}

function makeOperator(id: string, overrides: Partial<OperatorPredicate> = {}): OperatorPredicate {
  return {
    operatorID: id,
    operatorType: "TestOp",
    operatorVersion: "1.0",
    operatorProperties: {},
    inputPorts: [{ portID: "input-0", displayName: "Input 0" }],
    outputPorts: [{ portID: "output-0", displayName: "Output 0" }],
    showAdvanced: false,
    ...overrides,
  };
}

function makeLink(linkId: string, sourceId: string, targetId: string): OperatorLink {
  return {
    linkID: linkId,
    source: { operatorID: sourceId, portID: "output-0" },
    target: { operatorID: targetId, portID: "input-0" },
  };
}

function contentOf(
  steps: ReActStep[],
  state: WorkflowState,
  results = new Map<string, string>(),
  redact = false
): string {
  const messages = assembleContext(steps, state, results, redact);
  expect(messages).toHaveLength(1);
  expect(messages[0].role).toBe("user");
  return messages[0].content as string;
}

describe("assembleContext - tasks", () => {
  test("empty input yields a single empty user message", () => {
    const messages = assembleContext([], new WorkflowState(), new Map());
    expect(messages).toEqual([{ role: "user", content: "" }]);
  });

  test("a finished agent turn renders under Completed Tasks", () => {
    const steps = [userStep("m1", "load the file"), agentStep("m1", { stepId: 1, content: "done", isEnd: true })];
    const content = contentOf(steps, new WorkflowState());
    expect(content).toContain("# Completed Tasks");
    expect(content).toContain("## Task (completed)");
    expect(content).toContain("### User request");
    expect(content).toContain("load the file");
    expect(content).toContain("### Turn 1");
    expect(content).not.toContain("# Ongoing Task");
  });

  test("an unfinished agent turn renders under Ongoing Task with the continue prompt", () => {
    const steps = [userStep("m1", "keep going"), agentStep("m1", { stepId: 1, content: "thinking", isEnd: false })];
    const content = contentOf(steps, new WorkflowState());
    expect(content).toContain("# Ongoing Task");
    expect(content).toContain("Above is user's request");
    expect(content).not.toContain("# Completed Tasks");
  });

  test("tool calls are listed with succeeded/failed status", () => {
    const steps = [
      userStep("m1", "do it"),
      agentStep("m1", {
        stepId: 1,
        isEnd: true,
        toolCalls: [
          { toolName: "addOperator", toolCallId: "t1", input: {} },
          { toolName: "modifyOperator", toolCallId: "t2", input: {} },
        ],
        toolResults: [
          { toolCallId: "t1", output: "ok", isError: false },
          { toolCallId: "t2", output: "[ERROR] bad", isError: true },
        ],
      }),
    ];
    const content = contentOf(steps, new WorkflowState());
    expect(content).toContain("- addOperator (succeeded)");
    expect(content).toContain("- modifyOperator (failed)");
  });
});

describe("assembleContext - dataflow", () => {
  function twoOpState(): WorkflowState {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op1"));
    state.addOperator(makeOperator("op2"));
    state.addLink(makeLink("l1", "op1", "op2"));
    return state;
  }

  test("renders the operator list and links of a non-empty workflow", () => {
    const content = contentOf([], twoOpState());
    expect(content).toContain("# Current Dataflow");
    expect(content).toContain("## Operators");
    expect(content).toContain("### Operator `op1` (TestOp, not-executed)");
    expect(content).toContain("## Links");
    expect(content).toContain("- op1 → op2");
  });

  test("an operator with a result is marked executed and shows the result block", () => {
    const results = new Map<string, string>([["op1", "rows: 10"]]);
    const content = contentOf([], twoOpState(), results);
    expect(content).toContain("### Operator `op1` (TestOp, executed)");
    expect(content).toContain("Result:");
    expect(content).toContain("rows: 10");
  });

  test("an operator whose result contains [ERROR] is marked failed", () => {
    const results = new Map<string, string>([["op1", "[ERROR] boom"]]);
    const content = contentOf([], twoOpState(), results);
    expect(content).toContain("### Operator `op1` (TestOp, failed)");
  });

  test("no dataflow section when the workflow is empty", () => {
    const content = contentOf([], new WorkflowState());
    expect(content).not.toContain("# Current Dataflow");
  });
});
