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
import type { OperatorPredicate } from "../../types/workflow";

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

function makeOperator(id: string): OperatorPredicate {
  return {
    operatorID: id,
    operatorType: "TestOp",
    operatorVersion: "1.0",
    operatorProperties: {},
    inputPorts: [],
    outputPorts: [],
    showAdvanced: false,
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
});
