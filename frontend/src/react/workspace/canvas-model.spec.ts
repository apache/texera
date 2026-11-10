/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as joint from "jointjs";
import { CanvasModel } from "./canvas-model";
import { WorkflowSession } from "./workflow-session";
import { SharedModel } from "../../core/workspace/service/workflow-graph/model/shared-model";
import { ExecutionMode, type Workflow } from "../../core/common/type/workflow";
import type { OperatorMetadata } from "../../core/workspace/types/operator-schema.interface";

const metadata: OperatorMetadata = {
  operators: [
    {
      operatorType: "Test",
      operatorVersion: "1",
      jsonSchema: {},
      additionalMetadata: {
        userFriendlyName: "Test operator",
        operatorGroupName: "Test",
        inputPorts: [{}],
        outputPorts: [{}],
      },
    },
  ],
  groups: [],
};
const workflow = (): Workflow => ({
  wid: 3,
  name: "Test",
  description: "",
  isPublished: 0,
  readonly: false,
  creationTime: 1,
  lastModifiedTime: 1,
  content: {
    operators: ["a", "b"].map(operatorID => ({
      operatorID,
      operatorType: "Test",
      operatorVersion: "1",
      inputPorts: [{ portID: "input-0" }],
      outputPorts: [{ portID: "output-0" }],
      operatorProperties: {},
      showAdvanced: false,
    })),
    operatorPositions: { a: { x: 20, y: 20 }, b: { x: 200, y: 20 } },
    links: [],
    commentBoxes: [],
    settings: { dataTransferBatchSize: 400, executionMode: ExecutionMode.PIPELINED },
  },
});
const dispose: (() => void)[] = [];
function setup(readonly = false) {
  const data = workflow();
  data.readonly = readonly;
  const session = new WorkflowSession(data, {
    shared: new SharedModel(3, undefined, undefined, { connect: false }),
    save: async item => item,
    autosaveMs: 0,
  });
  const onError = vi.fn();
  const canvas = new CanvasModel(session, metadata, onError);
  dispose.push(() => {
    canvas.dispose();
    session.dispose();
  });
  return { session, canvas, onError };
}
const original = HTMLCanvasElement.prototype.getContext;
beforeEach(() => {
  HTMLCanvasElement.prototype.getContext = (() => null) as typeof original;
});
afterEach(() => {
  dispose.splice(0).forEach(cleanup => cleanup());
});

describe("existing JointJS shapes in the React canvas", () => {
  it("persists comment dragging and reflects remote comment moves in the same cell", () => {
    const { session, canvas } = setup();
    session.addComment({ commentBoxID: "c", comments: [], commentBoxPosition: { x: 1, y: 2 } });
    const cell = canvas.graph.getCell("c") as joint.dia.Element;
    cell.position(50, 60);
    expect(session.graph.getCommentBox("c").commentBoxPosition).toEqual({ x: 50, y: 60 });
    session.moveComment("c", { x: 80, y: 90 });
    expect(cell.position()).toEqual({ x: 80, y: 90 });
    session.deleteCommentBox("c");
    expect(canvas.graph.getCell("c")).toBeUndefined();
  });
  it("keeps IDs, original operator shape and port IDs, and moves the same cell while dragging", () => {
    const { canvas, session } = setup();
    const cell = canvas.graph.getCell("a") as joint.dia.Element;
    expect(cell.getPorts().map(port => port.id)).toEqual(["input-0", "output-0"]);
    cell.position(80, 90);
    expect(session.getSnapshot().workflow.content.operatorPositions.a).toEqual({ x: 80, y: 90 });
    expect(canvas.graph.getCell("a")).toBe(cell);
  });
  it("commits a dragged link only once both endpoints have valid operator ports", () => {
    const { canvas, session } = setup();
    const link = new joint.dia.Link({ id: "ab", source: { id: "a", port: "output-0" }, target: { x: 100, y: 50 } });
    canvas.graph.addCell(link);
    expect(session.getSnapshot().workflow.content.links).toEqual([]);
    link.target({ id: "b", port: "input-0" });
    expect(session.getSnapshot().workflow.content.links).toEqual([
      { linkID: "ab", source: { operatorID: "a", portID: "output-0" }, target: { operatorID: "b", portID: "input-0" } },
    ]);
    session.deleteOperators(["a"]);
    expect(canvas.graph.getCell("ab")).toBeUndefined();
  });
  it("rejects invalid ports and read-only position writes without corrupting the workflow", () => {
    const { canvas, session, onError } = setup(true);
    (canvas.graph.getCell("a") as joint.dia.Element).position(90, 90);
    expect(onError).toHaveBeenCalled();
    expect(session.getSnapshot().workflow.content.operatorPositions.a).toEqual({ x: 20, y: 20 });
    expect((canvas.graph.getCell("a") as joint.dia.Element).position()).toEqual({ x: 20, y: 20 });
  });
  it("removes every listener when the canvas view unmounts while retaining the session", () => {
    const { canvas, session } = setup();
    canvas.dispose();
    session.moveOperator("a", { x: 99, y: 99 });
    expect((canvas.graph.getCell("a") as joint.dia.Element).position()).toEqual({ x: 20, y: 20 });
    (canvas.graph.getCell("a") as joint.dia.Element).position(55, 55);
    expect(session.getSnapshot().workflow.content.operatorPositions.a).toEqual({ x: 99, y: 99 });
  });
});

afterEach(() => {
  HTMLCanvasElement.prototype.getContext = original;
});
