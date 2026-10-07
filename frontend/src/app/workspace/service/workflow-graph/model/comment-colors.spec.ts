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

import { TestBed } from "@angular/core/testing";
import * as Y from "yjs";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { OperatorMetadataService } from "../../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../operator-metadata/stub-operator-metadata.service";
import { WorkflowActionService } from "./workflow-action.service";
import { WorkflowGraph } from "./workflow-graph";
import { UndoRedoService } from "../../undo-redo/undo-redo.service";
import { mockCommentBox } from "./mock-workflow-data";
import { CommentBox } from "../../../types/workflow-common.interface";
import { WORKFLOW_COLOR_PRESETS, WorkflowColor } from "../../../types/workflow-color";

describe("persistent comment colors", () => {
  let service: WorkflowActionService;
  let graph: WorkflowGraph;
  let undo: UndoRedoService;
  const id = mockCommentBox.commentBoxID;
  const defaultFill = "#F2F4F5";
  const fill = (color: WorkflowColor) => WORKFLOW_COLOR_PRESETS.find(preset => preset.id === color)!.fill;
  const renderedFill = () => service.getJointGraph().getCell(id).attr("rect/fill");

  beforeEach(async () => {
    TestBed.configureTestingModule({
      providers: [{ provide: OperatorMetadataService, useClass: StubOperatorMetadataService }, ...commonTestProviders],
    });
    service = TestBed.inject(WorkflowActionService);
    graph = service.getTexeraGraph() as WorkflowGraph;
    undo = TestBed.inject(UndoRedoService);
    service.addCommentBox(structuredClone(mockCommentBox));
    graph.sharedModel.undoManager.clear();
  });

  afterEach(() => graph.destroyYModel());

  it("keeps old workflows gray without adding a color field to saved JSON", () => {
    expect(renderedFill()).toBe(defaultFill);
    expect(service.getWorkflowContent().commentBoxes).toEqual([mockCommentBox]);
  });

  it.each(["yellow", "blue", "green", "pink", "purple", "orange"] as const)(
    "persists and renders the %s preset without changing text or position",
    color => {
      const changed = vi.fn();
      const subscription = service.workflowChanged().subscribe(changed);
      service.setCommentBoxColor(id, color);
      expect(graph.getCommentBox(id)).toEqual({ ...mockCommentBox, color });
      expect(renderedFill()).toBe(fill(color));
      expect(service.getWorkflowContent().commentBoxes[0].color).toBe(color);
      expect(changed).toHaveBeenCalledTimes(1);
      subscription.unsubscribe();
    }
  );

  it("does not change selection borders when recoloring", () => {
    const cell = service.getJointGraph().getCell(id);
    cell.attr("rect/stroke", "#123456");
    cell.attr("rect/stroke-width", 3);
    service.setCommentBoxColor(id, "green");
    expect(cell.attr("rect/stroke")).toBe("#123456");
    expect(cell.attr("rect/stroke-width")).toBe(3);
  });

  it("resets to the legacy shape and emits one autosave notification", () => {
    service.setCommentBoxColor(id, "blue");
    const changed = vi.fn();
    const subscription = service.workflowChanged().subscribe(changed);
    service.setCommentBoxColor(id, undefined);
    expect(graph.getCommentBox(id)).toEqual(mockCommentBox);
    expect(renderedFill()).toBe(defaultFill);
    expect(changed).toHaveBeenCalledTimes(1);
    expect(graph.getSharedCommentBoxType(id).has("color")).toBe(false);
    subscription.unsubscribe();
  });

  it("ignores repeated selections without adding undo entries or autosave events", () => {
    service.setCommentBoxColor(id, "blue");
    const undoCount = undo.getUndoLength();
    const changed = vi.fn();
    const subscription = service.workflowChanged().subscribe(changed);
    service.setCommentBoxColor(id, "blue");
    expect(changed).not.toHaveBeenCalled();
    expect(undo.getUndoLength()).toBe(undoCount);
    subscription.unsubscribe();
  });

  it("keeps rapid selections separate from each other in undo and redo", () => {
    service.setCommentBoxColor(id, "blue");
    service.setCommentBoxColor(id, "pink");
    service.setCommentBoxColor(id, undefined);
    expect(undo.getUndoLength()).toBe(3);
    undo.undoAction();
    expect(graph.getCommentBox(id).color).toBe("pink");
    expect(renderedFill()).toBe(fill("pink"));
    undo.undoAction();
    expect(graph.getCommentBox(id).color).toBe("blue");
    undo.undoAction();
    expect(graph.getCommentBox(id)).toEqual(mockCommentBox);
    undo.redoAction();
    expect(renderedFill()).toBe(fill("blue"));
    undo.redoAction();
    undo.redoAction();
    expect(renderedFill()).toBe(defaultFill);
    expect(graph.hasCommentBox(id)).toBe(true);
  });

  it("restores color after serialized workflow reload and copy with a new id", () => {
    service.setCommentBoxColor(id, "purple");
    const workflow = JSON.parse(JSON.stringify(service.getWorkflow()));
    service.reloadWorkflow(workflow, false, false);
    graph = service.getTexeraGraph() as WorkflowGraph;
    expect(renderedFill()).toBe(fill("purple"));
    service.addCommentBox({ ...graph.getCommentBox(id), commentBoxID: "copied" });
    expect(service.getJointGraph().getCell("copied").attr("rect/fill")).toBe(fill("purple"));
    expect(graph.getCommentBox("copied").comments).toEqual(mockCommentBox.comments);
  });

  it.each(["url(https://example.com/x)", "", "BLUE", "__proto__", null, 1, {}])(
    "rejects invalid local values %j without modifying the document",
    color => {
      expect(() => service.setCommentBoxColor(id, color as WorkflowColor)).toThrow(/Invalid workflow color/);
      expect(graph.getCommentBox(id)).toEqual(mockCommentBox);
      expect(undo.getUndoLength()).toBe(0);
    }
  );

  it("rejects a missing comment id", () => {
    expect(() => service.setCommentBoxColor("missing", "blue")).toThrow(/does not exist/);
    expect(graph.getAllCommentBoxes()).toEqual([mockCommentBox]);
  });

  it("does not edit a locked workflow", () => {
    service.disableWorkflowModification();
    service.setCommentBoxColor(id, "blue");
    expect(graph.getCommentBox(id)).toEqual(mockCommentBox);
    expect(undo.getUndoLength()).toBe(0);
  });

  it("does not edit readonly workflow metadata even before the modification stream is locked", () => {
    service.setWorkflowMetadata({ ...service.getWorkflowMetadata(), readonly: true });
    service.setCommentBoxColor(id, "blue");
    expect(graph.getCommentBox(id)).toEqual(mockCommentBox);
  });

  it.each(["url(https://example.com/x)", "toString", null, 42, { unexpected: true }])(
    "safely renders imported malformed color %j and permits reset",
    color => {
      service.addCommentBox({
        ...mockCommentBox,
        commentBoxID: "imported",
        color,
      } as unknown as CommentBox);
      expect(service.getJointGraph().getCell("imported").attr("rect/fill")).toBe(defaultFill);
      service.setCommentBoxColor("imported", undefined);
      expect(graph.getCommentBox("imported")).not.toHaveProperty("color");
    }
  );

  it("converges with a concurrent remote color change and retains remote text", () => {
    const remote = new Y.Doc();
    try {
      Y.applyUpdate(remote, Y.encodeStateAsUpdate(graph.sharedModel.yDoc));
      const remoteBox = remote.getMap<Y.Map<unknown>>("commentBoxMap").get(id)!;
      service.setCommentBoxColor(id, "blue");
      remoteBox.set("color", new Y.Text("orange"));
      (remoteBox.get("comments") as Y.Array<unknown>).push([
        {
          content: "remote note – 中文",
          creatorName: "Coeditor",
          creatorID: 99,
          creationTime: "2026-09-29",
        },
      ]);
      const remoteUpdate = Y.encodeStateAsUpdate(remote);
      Y.applyUpdate(remote, Y.encodeStateAsUpdate(graph.sharedModel.yDoc));
      Y.applyUpdate(graph.sharedModel.yDoc, remoteUpdate, remote);
      expect(graph.getCommentBox(id)).toEqual(remoteBox.toJSON());
      expect(["blue", "orange"]).toContain(graph.getCommentBox(id).color);
      expect(renderedFill()).toBe(fill(graph.getCommentBox(id).color!));
      expect(graph.getCommentBox(id).comments.at(-1)!.content).toBe("remote note – 中文");
    } finally {
      remote.destroy();
    }
  });

  it("redraws remote reset and malformed nested text changes safely", () => {
    service.setCommentBoxColor(id, "green");
    const remote = new Y.Doc();
    try {
      Y.applyUpdate(remote, Y.encodeStateAsUpdate(graph.sharedModel.yDoc));
      const remoteBox = remote.getMap<Y.Map<unknown>>("commentBoxMap").get(id)!;
      const color = remoteBox.get("color") as Y.Text;
      color.insert(color.length, " invalid");
      Y.applyUpdate(graph.sharedModel.yDoc, Y.encodeStateAsUpdate(remote), remote);
      expect(renderedFill()).toBe(defaultFill);
      remoteBox.delete("color");
      Y.applyUpdate(graph.sharedModel.yDoc, Y.encodeStateAsUpdate(remote), remote);
      expect(graph.getCommentBox(id)).toEqual(mockCommentBox);
      expect(renderedFill()).toBe(defaultFill);
    } finally {
      remote.destroy();
    }
  });

  it("does not undo a coeditor's note when undoing a local color choice", () => {
    service.setCommentBoxColor(id, "blue");
    const remote = new Y.Doc();
    const note = { content: "Coeditor note", creatorName: "Coeditor", creatorID: 99, creationTime: "2026-09-29" };
    try {
      Y.applyUpdate(remote, Y.encodeStateAsUpdate(graph.sharedModel.yDoc));
      const remoteBox = remote.getMap<Y.Map<unknown>>("commentBoxMap").get(id)!;
      (remoteBox.get("comments") as Y.Array<unknown>).push([note]);
      // A provider-origin update must not be captured as a local undo step.
      Y.applyUpdate(graph.sharedModel.yDoc, Y.encodeStateAsUpdate(remote), remote);
      expect(undo.getUndoLength()).toBe(1);
      undo.undoAction();
      expect(graph.getCommentBox(id).comments).toEqual([...mockCommentBox.comments, note]);
      expect(renderedFill()).toBe(defaultFill);
    } finally {
      remote.destroy();
    }
  });

  it("renders and announces remote colors in a locked workflow without adding local undo history", () => {
    service.disableWorkflowModification();
    const remote = new Y.Doc();
    const changed = vi.fn();
    const subscription = service.workflowChanged().subscribe(changed);
    try {
      Y.applyUpdate(remote, Y.encodeStateAsUpdate(graph.sharedModel.yDoc));
      remote.getMap<Y.Map<unknown>>("commentBoxMap").get(id)!.set("color", new Y.Text("green"));
      Y.applyUpdate(graph.sharedModel.yDoc, Y.encodeStateAsUpdate(remote), remote);
      expect(renderedFill()).toBe(fill("green"));
      expect(changed).toHaveBeenCalledTimes(1);
      expect(undo.getUndoLength()).toBe(0);
    } finally {
      subscription.unsubscribe();
      remote.destroy();
    }
  });
});
