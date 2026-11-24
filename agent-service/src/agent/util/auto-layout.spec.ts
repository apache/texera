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
import type { CommentBox, OperatorPredicate, OperatorLink } from "../../types/workflow";

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

function makeCommentBox(id: string, x: number, y: number, anchoredOperatorID?: string): CommentBox {
  return {
    commentBoxID: id,
    comments: [],
    commentBoxPosition: { x, y },
    ...(anchoredOperatorID !== undefined ? { anchoredOperatorID } : {}),
  };
}

function makeLink(linkID: string, src: string, tgt: string): OperatorLink {
  return {
    linkID,
    source: { operatorID: src, portID: "output-0" },
    target: { operatorID: tgt, portID: "input-0" },
  };
}

// Sentinel coordinate that the layout pass must overwrite. Using a single
// shared value for every operator means a no-op layout would leave every
// node piled on the same point, which the assertions below detect.
const SENTINEL = -9999;
const SENTINEL_POS = { x: SENTINEL, y: SENTINEL };

describe("autoLayoutWorkflow", () => {
  test("is a no-op when the workflow has no operators", () => {
    const state = new WorkflowState();
    expect(() => autoLayoutWorkflow(state)).not.toThrow();
    expect(state.getAllOperators()).toHaveLength(0);
  });

  test("assigns a finite numeric position to a single operator", () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op1"), SENTINEL_POS);

    autoLayoutWorkflow(state);

    const pos = state.getOperatorPosition("op1");
    expect(pos).toBeDefined();
    expect(Number.isFinite(pos!.x)).toBe(true);
    expect(Number.isFinite(pos!.y)).toBe(true);
    // A regression to a no-op would leave the sentinel in place.
    expect(pos!.x).not.toBe(SENTINEL);
    expect(pos!.y).not.toBe(SENTINEL);
  });

  test("places linked operators left-to-right along the chain (rankdir LR)", () => {
    const state = new WorkflowState();
    // Seed every node with the same sentinel so the chain ordering can
    // only emerge from the layout pass, not from incidental insertion order.
    state.addOperator(makeOperator("a"), SENTINEL_POS);
    state.addOperator(makeOperator("b"), SENTINEL_POS);
    state.addOperator(makeOperator("c"), SENTINEL_POS);
    state.addLink(makeLink("l1", "a", "b"));
    state.addLink(makeLink("l2", "b", "c"));

    autoLayoutWorkflow(state);

    const a = state.getOperatorPosition("a")!;
    const b = state.getOperatorPosition("b")!;
    const c = state.getOperatorPosition("c")!;

    expect(a.x).toBeLessThan(b.x);
    expect(b.x).toBeLessThan(c.x);
  });

  test("matches the frontend rank separation for linked operators", () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("a"), SENTINEL_POS);
    state.addOperator(makeOperator("b"), SENTINEL_POS);
    state.addLink(makeLink("l1", "a", "b"));

    autoLayoutWorkflow(state);

    const a = state.getOperatorPosition("a")!;
    const b = state.getOperatorPosition("b")!;
    // dagre stores node centers, so the gap is node width (200) + ranksep (80).
    expect(b.x - a.x).toBe(280);
  });

  test("assigns positions to disconnected operators as well", () => {
    const state = new WorkflowState();
    // Seeding each disconnected node with the same sentinel forces the
    // layout pass to actually touch them; otherwise they'd stay collapsed.
    state.addOperator(makeOperator("solo-1"), SENTINEL_POS);
    state.addOperator(makeOperator("solo-2"), SENTINEL_POS);
    state.addOperator(makeOperator("solo-3"), SENTINEL_POS);

    autoLayoutWorkflow(state);

    for (const id of ["solo-1", "solo-2", "solo-3"]) {
      const pos = state.getOperatorPosition(id);
      expect(pos).toBeDefined();
      expect(Number.isFinite(pos!.x)).toBe(true);
      expect(Number.isFinite(pos!.y)).toBe(true);
      expect(pos!.x).not.toBe(SENTINEL);
      expect(pos!.y).not.toBe(SENTINEL);
    }
  });

  test("overwrites pre-existing operator positions", () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("a"), SENTINEL_POS);
    state.addOperator(makeOperator("b"), SENTINEL_POS);
    state.addLink(makeLink("l1", "a", "b"));

    autoLayoutWorkflow(state);

    const a = state.getOperatorPosition("a")!;
    const b = state.getOperatorPosition("b")!;
    // Both axes must be overwritten — a regression that left y stale
    // while updating x would otherwise sneak past.
    expect(a.x).not.toBe(SENTINEL);
    expect(a.y).not.toBe(SENTINEL);
    expect(b.x).not.toBe(SENTINEL);
    expect(b.y).not.toBe(SENTINEL);
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

  describe("comment boxes", () => {
    // Two linked operators far from where dagre puts them, each with a comment box beside it.
    function stateWithComments(): WorkflowState {
      const state = new WorkflowState();
      state.setWorkflowContent({
        operators: [makeOperator("a"), makeOperator("b")],
        operatorPositions: { a: { x: 1000, y: 1000 }, b: { x: 1300, y: 1000 } },
        links: [makeLink("l1", "a", "b")],
        commentBoxes: [
          makeCommentBox("anchored-to-a", 1000, 900, "a"),
          makeCommentBox("anchored-to-b", 1320, 1100, "b"),
          makeCommentBox("floating", 1000, 1200),
          makeCommentBox("anchored-to-missing", 500, 500, "gone"),
        ],
        settings: { dataTransferBatchSize: 400 },
      });
      return state;
    }

    function commentBoxPosition(state: WorkflowState, id: string) {
      return state.getWorkflowContent().commentBoxes.find(box => box.commentBoxID === id)!.commentBoxPosition;
    }

    test("moves an anchored comment box with its operator, keeping its offset", () => {
      const state = stateWithComments();

      autoLayoutWorkflow(state);

      const a = state.getOperatorPosition("a")!;
      const b = state.getOperatorPosition("b")!;
      expect(commentBoxPosition(state, "anchored-to-a")).toEqual({ x: a.x, y: a.y - 100 });
      expect(commentBoxPosition(state, "anchored-to-b")).toEqual({ x: b.x + 20, y: b.y + 100 });
    });

    test("keeps the anchor on each moved comment box", () => {
      const state = stateWithComments();

      autoLayoutWorkflow(state);

      const anchors = Object.fromEntries(
        state.getWorkflowContent().commentBoxes.map(box => [box.commentBoxID, box.anchoredOperatorID])
      );
      expect(anchors).toEqual({
        "anchored-to-a": "a",
        "anchored-to-b": "b",
        floating: undefined,
        "anchored-to-missing": "gone",
      });
    });

    test("leaves floating comment boxes and ones anchored to a missing operator in place", () => {
      const state = stateWithComments();

      autoLayoutWorkflow(state);

      expect(commentBoxPosition(state, "floating")).toEqual({ x: 1000, y: 1200 });
      expect(commentBoxPosition(state, "anchored-to-missing")).toEqual({ x: 500, y: 500 });
    });
  });
});
