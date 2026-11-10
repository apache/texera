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

import { beforeEach, describe, it, expect } from "vitest";
import { VersionDiff, versionHighlights } from "./version-diff";
import { ExecutionMode, type WorkflowContent } from "../../core/common/type/workflow";
import type { CommentBox, OperatorPredicate } from "../../core/workspace/types/workflow-common.interface";

function buildOperator(overrides: Partial<OperatorPredicate> = {}): OperatorPredicate {
  return {
    operatorID: "op-1",
    operatorType: "Filter",
    operatorVersion: "v1",
    operatorProperties: {},
    inputPorts: [],
    outputPorts: [],
    showAdvanced: false,
    ...overrides,
  };
}

function buildContent(overrides: Partial<WorkflowContent> = {}): WorkflowContent {
  return {
    operators: [],
    operatorPositions: {},
    links: [],
    commentBoxes: [] as CommentBox[],
    settings: { dataTransferBatchSize: 400, executionMode: ExecutionMode.PIPELINED },
    ...overrides,
  };
}

let service: VersionDiff;
beforeEach(() => {
  service = new VersionDiff();
});
describe("getWorkflowsDifference", () => {
  it("keeps removed-operator neighbor brackets on the same side as the original paper highlights", () => {
    const current = buildContent({
      links: [
        { linkID: "out", source: { operatorID: "dead", portID: "o" }, target: { operatorID: "right", portID: "i" } },
        { linkID: "in", source: { operatorID: "left", portID: "o" }, target: { operatorID: "dead", portID: "i" } },
      ],
    });
    expect(versionHighlights({ added: ["added"], modified: ["changed"], deleted: ["dead"] }, current)).toEqual({
      added: ["added"],
      modified: ["changed"],
      deleted: ["dead"],
      leftBrackets: ["right"],
      rightBrackets: ["left"],
      operatorPropertyDiff: {},
    });
  });
  it("retains arg1 property scope and legacy loose comparison while rejecting malformed or duplicate operators", () => {
    const current = buildOperator({ operatorProperties: { empty: null, count: 1 } }),
      historical = buildOperator({ operatorProperties: { count: "1", newProperty: "added" } });
    expect(service.getOperatorsDifference(current, historical).size).toBe(0);
    expect(() => service.getWorkflowsDifference(null as unknown as WorkflowContent, buildContent())).toThrow(
      "workflow"
    );
    expect(() =>
      service.getWorkflowsDifference(buildContent({ operators: [current, current] }), buildContent())
    ).toThrow("operators");
    expect(() =>
      versionHighlights({ added: [], modified: [], deleted: [] }, {
        ...buildContent(),
        links: [null],
      } as unknown as WorkflowContent)
    ).toThrow("links");
  });
  it("classifies operators into added, deleted, and modified", () => {
    // Forward diff semantics: ids present in arg2 but not arg1 are "added"
    // (to go from arg1 to arg2 you'd add them), ids in arg1 only are
    // "deleted", and ids in both with differing content are "modified".
    const a1 = buildOperator({ operatorID: "stay", operatorProperties: { a: 1 } });
    const a2 = buildOperator({ operatorID: "stay", operatorProperties: { a: 2 } });
    const onlyInArg1 = buildOperator({ operatorID: "in-arg1-only" });
    const onlyInArg2 = buildOperator({ operatorID: "in-arg2-only" });

    const diff = service.getWorkflowsDifference(
      buildContent({ operators: [a1, onlyInArg1] }),
      buildContent({ operators: [a2, onlyInArg2] })
    );

    expect(diff.added.sort()).toEqual(["in-arg2-only"]);
    expect(diff.modified.sort()).toEqual(["stay"]);
    expect(diff.deleted.sort()).toEqual(["in-arg1-only"]);
  });

  it("returns empty diffs when the contents are identical", () => {
    const op = buildOperator();
    const diff = service.getWorkflowsDifference(buildContent({ operators: [op] }), buildContent({ operators: [op] }));
    expect(diff).toEqual({ added: [], modified: [], deleted: [] });
    expect(service.operatorPropertyDiff).toEqual({});
  });

  it("leaves an operator that is identical in both versions out of the modified list", () => {
    // The two contents differ overall (so the diff loop runs at all), but one
    // of the two operators is untouched: it must be reported neither as
    // modified nor given a property diff. The untouched pair is deliberately
    // two *distinct* objects with equal contents, so a shallow `!==` in place
    // of the deep compare would wrongly report it as modified.
    const untouchedA = buildOperator({ operatorID: "same", operatorProperties: { a: 1 } });
    const untouchedB = buildOperator({ operatorID: "same", operatorProperties: { a: 1 } });
    const before = buildOperator({ operatorID: "moved", operatorProperties: { a: 1 } });
    const after = buildOperator({ operatorID: "moved", operatorProperties: { a: 2 } });

    const diff = service.getWorkflowsDifference(
      buildContent({ operators: [untouchedA, before] }),
      buildContent({ operators: [untouchedB, after] })
    );

    expect(diff.modified).toEqual(["moved"]);
    expect(diff.added).toEqual([]);
    expect(diff.deleted).toEqual([]);
    expect(service.operatorPropertyDiff["same"]).toBeUndefined();
    expect(service.operatorPropertyDiff["moved"]).toBeDefined();
  });

  it("records per-property and version-bump diffs in operatorPropertyDiff", () => {
    const live = buildOperator({
      operatorID: "x",
      operatorVersion: "v2",
      operatorProperties: { foo: "a", bar: 1 },
    });
    const old = buildOperator({
      operatorID: "x",
      operatorVersion: "v1",
      operatorProperties: { foo: "b", bar: 1 },
    });

    service.getWorkflowsDifference(buildContent({ operators: [live] }), buildContent({ operators: [old] }));

    const diffMap = service.operatorPropertyDiff["x"];
    expect(diffMap.get("foo" as unknown as string)).toContain("rgb(255, 118, 20)");
    expect(diffMap.has("bar" as unknown as string)).toBe(false);
    expect(diffMap.has("operatorVersion" as unknown as string)).toBe(true);
  });
});
