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
import { autoLayoutWorkflow } from "./auto-layout";
import { WorkflowState } from "../workflow-state";
import type { OperatorPredicate, OperatorLink } from "../../types/workflow";

function makeOperator(id: string): OperatorPredicate {
  return {
    operatorID: id,
    operatorType: "TestOp",
    operatorVersion: "1.0",
    operatorProperties: {},
    inputPorts: [{ portID: "input-0", displayName: "Input 0" }],
    outputPorts: [{ portID: "output-0", displayName: "Output 0" }],
    showAdvanced: false,
  };
}

function makeLink(linkID: string, src: string, tgt: string): OperatorLink {
  return {
    linkID,
    source: { operatorID: src, portID: "output-0" },
    target: { operatorID: tgt, portID: "input-0" },
  };
}

describe("autoLayoutWorkflow", () => {
  test("is a no-op when the workflow has no operators", () => {
    const state = new WorkflowState();
    expect(() => autoLayoutWorkflow(state)).not.toThrow();
    expect(state.getAllOperators()).toHaveLength(0);
  });

  test("assigns a finite numeric position to a single operator", () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op1"));

    autoLayoutWorkflow(state);

    const pos = state.getOperatorPosition("op1");
    expect(pos).toBeDefined();
    expect(Number.isFinite(pos!.x)).toBe(true);
    expect(Number.isFinite(pos!.y)).toBe(true);
  });

  test("places linked operators left-to-right along the chain (rankdir LR)", () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("a"));
    state.addOperator(makeOperator("b"));
    state.addOperator(makeOperator("c"));
    state.addLink(makeLink("l1", "a", "b"));
    state.addLink(makeLink("l2", "b", "c"));

    autoLayoutWorkflow(state);

    const a = state.getOperatorPosition("a")!;
    const b = state.getOperatorPosition("b")!;
    const c = state.getOperatorPosition("c")!;

    expect(a.x).toBeLessThan(b.x);
    expect(b.x).toBeLessThan(c.x);
  });

  test("assigns positions to disconnected operators as well", () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("solo-1"));
    state.addOperator(makeOperator("solo-2"));
    state.addOperator(makeOperator("solo-3"));

    autoLayoutWorkflow(state);

    for (const id of ["solo-1", "solo-2", "solo-3"]) {
      const pos = state.getOperatorPosition(id);
      expect(pos).toBeDefined();
      expect(Number.isFinite(pos!.x)).toBe(true);
      expect(Number.isFinite(pos!.y)).toBe(true);
    }
  });

  test("overwrites pre-existing operator positions", () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("a"), { x: -9999, y: -9999 });
    state.addOperator(makeOperator("b"), { x: -9999, y: -9999 });
    state.addLink(makeLink("l1", "a", "b"));

    autoLayoutWorkflow(state);

    const a = state.getOperatorPosition("a")!;
    const b = state.getOperatorPosition("b")!;
    expect(a.x).not.toBe(-9999);
    expect(b.x).not.toBe(-9999);
    expect(a.x).toBeLessThan(b.x);
  });

  test("places parallel branches at distinct y positions on the same rank", () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("source"));
    state.addOperator(makeOperator("branch-top"));
    state.addOperator(makeOperator("branch-bottom"));
    state.addLink(makeLink("l1", "source", "branch-top"));
    state.addLink(makeLink("l2", "source", "branch-bottom"));

    autoLayoutWorkflow(state);

    const top = state.getOperatorPosition("branch-top")!;
    const bottom = state.getOperatorPosition("branch-bottom")!;
    // Both branches sit downstream of source so share an x rank...
    expect(top.x).toBe(bottom.x);
    // ...but dagre separates them vertically by nodesep.
    expect(top.y).not.toBe(bottom.y);
  });
});
