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
import { WorkflowState } from "./workflow-state";
import type { OperatorPredicate, OperatorLink, WorkflowContent } from "../types/workflow";

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

describe("WorkflowState - operators", () => {
  test("add and get operator round-trips", () => {
    const state = new WorkflowState();
    const op = makeOperator("op1");
    state.addOperator(op);
    expect(state.getOperator("op1")).toEqual(op);
    expect(state.getAllOperators()).toHaveLength(1);
  });

  test("delete operator removes connected links", () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op1"));
    state.addOperator(makeOperator("op2"));
    state.addLink(makeLink("l1", "op1", "op2"));

    expect(state.deleteOperator("op1")).toBe(true);
    expect(state.getOperator("op1")).toBeUndefined();
    expect(state.getAllLinks()).toHaveLength(0);
  });

  test("delete on missing operator returns false", () => {
    const state = new WorkflowState();
    expect(state.deleteOperator("missing")).toBe(false);
  });

  test("updateOperatorProperties merges, does not replace", () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op1", { operatorProperties: { a: 1, b: 2 } }));
    state.updateOperatorProperties("op1", { b: 99, c: 3 });

    expect(state.getOperator("op1")?.operatorProperties).toEqual({ a: 1, b: 99, c: 3 });
  });

  test("updateOperatorDisplayName sets customDisplayName", () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op1"));
    expect(state.updateOperatorDisplayName("op1", "Filter rows")).toBe(true);
    expect(state.getOperator("op1")?.customDisplayName).toBe("Filter rows");
  });

  test("update on missing operator returns false", () => {
    const state = new WorkflowState();
    expect(state.updateOperatorProperties("missing", { a: 1 })).toBe(false);
    expect(state.updateOperatorDisplayName("missing", "x")).toBe(false);
  });
});

describe("WorkflowState - links", () => {
  test("add and delete link", () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op1"));
    state.addOperator(makeOperator("op2"));
    const link = makeLink("l1", "op1", "op2");
    state.addLink(link);

    expect(state.getAllLinks()).toEqual([link]);
    expect(state.deleteLink("l1")).toBe(true);
    expect(state.getAllLinks()).toEqual([]);
  });

  test("getLinksConnectedToOperator returns both inbound and outbound", () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op1"));
    state.addOperator(makeOperator("op2"));
    state.addOperator(makeOperator("op3"));
    state.addLink(makeLink("l1", "op1", "op2"));
    state.addLink(makeLink("l2", "op2", "op3"));

    const connected = state.getLinksConnectedToOperator("op2");
    expect(connected.map(l => l.linkID).sort()).toEqual(["l1", "l2"]);
  });
});

describe("WorkflowState - generated ids", () => {
  test("generateLinkId is monotonically increasing", () => {
    const state = new WorkflowState();
    expect(state.generateLinkId()).toBe("link-1");
    expect(state.generateLinkId()).toBe("link-2");
    expect(state.generateLinkId()).toBe("link-3");
  });

  test("generateOperatorId is namespaced by type", () => {
    const state = new WorkflowState();
    expect(state.generateOperatorId("Filter")).toBe("Filter-operator-1");
    expect(state.generateOperatorId("Filter")).toBe("Filter-operator-2");
    expect(state.generateOperatorId("Sort")).toBe("Sort-operator-3");
  });
});

describe("WorkflowState - getSubDAG", () => {
  test("walks ancestors of the target operator", () => {
    // op1 -> op2 -> op4
    //        op3 -> op4
    // sub-DAG of op4 should include all four.
    const state = new WorkflowState();
    state.addOperator(makeOperator("op1"));
    state.addOperator(makeOperator("op2"));
    state.addOperator(makeOperator("op3"));
    state.addOperator(makeOperator("op4"));
    state.addLink(makeLink("l1", "op1", "op2"));
    state.addLink(makeLink("l2", "op2", "op4"));
    state.addLink(makeLink("l3", "op3", "op4"));

    const subDag = state.getSubDAG("op4");
    expect(subDag.operators.map(o => o.operatorID).sort()).toEqual(["op1", "op2", "op3", "op4"]);
    expect(subDag.links.map(l => l.linkID).sort()).toEqual(["l1", "l2", "l3"]);
  });

  test("excludes downstream operators", () => {
    // op1 -> op2 -> op3
    // sub-DAG of op2 should include op1 and op2 but not op3.
    const state = new WorkflowState();
    state.addOperator(makeOperator("op1"));
    state.addOperator(makeOperator("op2"));
    state.addOperator(makeOperator("op3"));
    state.addLink(makeLink("l1", "op1", "op2"));
    state.addLink(makeLink("l2", "op2", "op3"));

    const subDag = state.getSubDAG("op2");
    expect(subDag.operators.map(o => o.operatorID).sort()).toEqual(["op1", "op2"]);
  });

  test("disabled upstream operators are skipped", () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op1", { isDisabled: true }));
    state.addOperator(makeOperator("op2"));
    state.addLink(makeLink("l1", "op1", "op2"));

    const subDag = state.getSubDAG("op2");
    expect(subDag.operators.map(o => o.operatorID)).toEqual(["op2"]);
  });
});

describe("WorkflowState - toLogicalPlan", () => {
  test("produces operators, port-indexed links, and an empty reuse list", () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op1"));
    state.addOperator(makeOperator("op2"));
    state.addLink(makeLink("l1", "op1", "op2"));

    const plan = state.toLogicalPlan();

    expect(plan.operators.map(o => o.operatorID)).toEqual(["op1", "op2"]);
    // operatorProperties are spread onto each logical operator; type + ports carry through
    expect(plan.operators[0].operatorType).toBe("TestOp");
    expect(plan.links).toEqual([
      { fromOpId: "op1", fromPortId: { id: 0, internal: false }, toOpId: "op2", toPortId: { id: 0, internal: false } },
    ]);
    expect(plan.opsToReuseResult).toEqual([]);
  });

  test("resolves the link port indices from the operators' port lists", () => {
    const state = new WorkflowState();
    state.addOperator(
      makeOperator("src", {
        outputPorts: [
          { portID: "output-0", displayName: "Output 0" },
          { portID: "output-1", displayName: "Output 1" },
        ],
      })
    );
    state.addOperator(
      makeOperator("dst", {
        inputPorts: [
          { portID: "input-0", displayName: "Input 0" },
          { portID: "input-1", displayName: "Input 1" },
        ],
      })
    );
    state.addLink({
      linkID: "l1",
      source: { operatorID: "src", portID: "output-1" },
      target: { operatorID: "dst", portID: "input-1" },
    });

    expect(state.toLogicalPlan().links[0]).toEqual({
      fromOpId: "src",
      fromPortId: { id: 1, internal: false },
      toOpId: "dst",
      toPortId: { id: 1, internal: false },
    });
  });
});

describe("WorkflowState - traversal", () => {
  // chain: op1 -> op2 -> op3
  function seedChain(): WorkflowState {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op1"));
    state.addOperator(makeOperator("op2"));
    state.addOperator(makeOperator("op3"));
    state.addLink(makeLink("l1", "op1", "op2"));
    state.addLink(makeLink("l2", "op2", "op3"));
    return state;
  }

  test("getSubDAG collects the target and every upstream operator and link", () => {
    const sub = seedChain().getSubDAG("op3");
    expect(sub.operators.map(o => o.operatorID).sort()).toEqual(["op1", "op2", "op3"]);
    expect(sub.links.map(l => l.linkID).sort()).toEqual(["l1", "l2"]);
  });
});

describe("WorkflowState - workflow content round-trip", () => {
  test("setWorkflowContent replaces the state and getWorkflowContent reflects it", () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("stale")); // must be cleared by setWorkflowContent

    const content: WorkflowContent = {
      operators: [makeOperator("op1"), makeOperator("op2")],
      operatorPositions: { op1: { x: 1, y: 2 } },
      links: [makeLink("l1", "op1", "op2")],
      commentBoxes: [],
      settings: { dataTransferBatchSize: 123 },
    };
    state.setWorkflowContent(content);

    const out = state.getWorkflowContent();
    expect(out.operators.map(o => o.operatorID)).toEqual(["op1", "op2"]);
    expect(out.links.map(l => l.linkID)).toEqual(["l1"]);
    expect(out.operatorPositions).toEqual({ op1: { x: 1, y: 2 } });
    expect(out.settings).toEqual({ dataTransferBatchSize: 123 });
    expect(state.getOperator("stale")).toBeUndefined();
  });

  test("setWorkflowContent falls back to defaults when settings/commentBoxes are absent", () => {
    const state = new WorkflowState();
    // The runtime guards against missing optional fields even though the type requires them.
    state.setWorkflowContent({ operators: [], operatorPositions: {}, links: [] } as unknown as WorkflowContent);

    const out = state.getWorkflowContent();
    expect(out.settings).toEqual({ dataTransferBatchSize: 400 }); // DEFAULT_WORKFLOW_SETTINGS
    expect(out.commentBoxes).toEqual([]);
  });

  test("carries the Form View definition through an agent save untouched", () => {
    const state = new WorkflowState();
    const formBinding = {
      fields: [{ id: "f1", operatorID: "op1", propertyKey: "limit", displayName: "Limit" }],
      resultOperatorIds: ["op2"],
    };
    state.setWorkflowContent({
      operators: [makeOperator("op1"), makeOperator("op2")],
      operatorPositions: {},
      links: [],
      commentBoxes: [],
      settings: { dataTransferBatchSize: 400 },
      formBinding,
    });

    // The agent edits the graph but never the form definition; it must come back intact.
    state.addOperator(makeOperator("op3"));
    expect(state.getWorkflowContent().formBinding).toEqual(formBinding);
  });

  test("adds no formBinding key when the workflow never had one", () => {
    const state = new WorkflowState();
    state.setWorkflowContent({
      operators: [],
      operatorPositions: {},
      links: [],
      commentBoxes: [],
      settings: { dataTransferBatchSize: 400 },
    });

    expect("formBinding" in state.getWorkflowContent()).toBe(false);
  });
});
