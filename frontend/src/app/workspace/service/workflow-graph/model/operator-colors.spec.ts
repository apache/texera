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
import { mockPoint, mockScanPredicate, mockSentimentPredicate } from "./mock-workflow-data";
import { OperatorPredicate } from "../../../types/workflow-common.interface";
import { WORKFLOW_COLOR_PRESETS, WorkflowColor } from "../../../types/workflow-color";
import { HeatmapView } from "../../heatmap/heatmap-scoring";
import { JointUIService } from "../../joint-ui/joint-ui.service";
import { ExecuteWorkflowService } from "../../execute-workflow/execute-workflow.service";

describe("persistent operator colors", () => {
  let service: WorkflowActionService;
  let graph: WorkflowGraph;
  let undo: UndoRedoService;
  const id = mockScanPredicate.operatorID;
  const otherId = mockSentimentPredicate.operatorID;
  const fill = (color: WorkflowColor) => WORKFLOW_COLOR_PRESETS.find(preset => preset.id === color)!.fill;
  const renderedFill = () => service.getJointGraph().getCell(id).attr("rect.body/fill");

  beforeEach(async () => {
    TestBed.configureTestingModule({
      providers: [{ provide: OperatorMetadataService, useClass: StubOperatorMetadataService }, ...commonTestProviders],
    });
    service = TestBed.inject(WorkflowActionService);
    graph = service.getTexeraGraph() as WorkflowGraph;
    undo = TestBed.inject(UndoRedoService);
    service.addOperator(structuredClone(mockScanPredicate), mockPoint);
    service.addOperator(structuredClone(mockSentimentPredicate), { x: 400, y: 200 });
    graph.sharedModel.undoManager.clear();
  });

  afterEach(() => graph.destroyYModel());

  it("keeps legacy operators white without adding a color property", () => {
    expect(renderedFill()).toBe("#FFFFFF");
    expect(graph.getOperator(id)).not.toHaveProperty("color");
  });

  it.each(["yellow", "blue", "green", "pink", "purple", "orange"] as const)(
    "persists and renders %s without changing execution properties",
    color => {
      const before = graph.getOperator(id);
      const changed = vi.fn();
      const subscription = service.workflowChanged().subscribe(changed);
      service.setOperatorsColor([id], color);
      expect(graph.getOperator(id)).toEqual({ ...before, color });
      expect(renderedFill()).toBe(fill(color));
      expect(service.getWorkflowContent().operators.find(op => op.operatorID === id)!.color).toBe(color);
      expect(changed).toHaveBeenCalledTimes(1);
      subscription.unsubscribe();
    }
  );

  it("does not alter status/validation borders or selection highlights", () => {
    const cell = service.getJointGraph().getCell(id);
    cell.attr("rect.body/stroke", "red");
    cell.attr("rect.body/stroke-width", 3);
    cell.attr("rect.boundary/stroke", "#123456");
    service.setOperatorsColor([id], "green");
    expect(cell.attr("rect.body/stroke")).toBe("red");
    expect(cell.attr("rect.body/stroke-width")).toBe(3);
    expect(cell.attr("rect.boundary/stroke")).toBe("#123456");
  });

  it("does not change the logical plan sent to the execution or compilation services", () => {
    const before = ExecuteWorkflowService.getLogicalPlanRequest(graph);
    service.setOperatorsColor([id, otherId], "blue");
    expect(ExecuteWorkflowService.getLogicalPlanRequest(graph)).toEqual(before);
    service.setOperatorsColor([id, otherId], undefined);
    expect(ExecuteWorkflowService.getLogicalPlanRequest(graph)).toEqual(before);
  });

  it("applies a multi-selection as one undo step and preserves the selection", () => {
    const wrapper = service.getJointGraphWrapper();
    wrapper.setMultiSelectMode(true);
    wrapper.highlightOperators(id, otherId);
    const selected = [...wrapper.getCurrentHighlightedOperatorIDs()];
    service.setOperatorsColor([id, otherId, id], "blue");
    expect(undo.getUndoLength()).toBe(1);
    expect(graph.getOperator(id).color).toBe("blue");
    expect(graph.getOperator(otherId).color).toBe("blue");
    expect(wrapper.getCurrentHighlightedOperatorIDs()).toEqual(selected);
    undo.undoAction();
    expect(graph.getOperator(id)).not.toHaveProperty("color");
    expect(graph.getOperator(otherId)).not.toHaveProperty("color");
    undo.redoAction();
    expect(graph.getOperator(id).color).toBe("blue");
    expect(graph.getOperator(otherId).color).toBe("blue");
  });

  it("separates rapid choices and reset, including from prior property edits", () => {
    service.setOperatorProperty(id, { path: "new-path" });
    graph.sharedModel.undoManager.stopCapturing();
    service.setOperatorsColor([id], "blue");
    service.setOperatorsColor([id], "pink");
    service.setOperatorsColor([id], undefined);
    expect(undo.getUndoLength()).toBe(4);
    expect(graph.getSharedOperatorType(id).has("color")).toBe(false);
    undo.undoAction();
    expect(renderedFill()).toBe(fill("pink"));
    undo.undoAction();
    expect(renderedFill()).toBe(fill("blue"));
    undo.undoAction();
    expect(renderedFill()).toBe("#FFFFFF");
    expect(graph.getOperator(id).operatorProperties).toEqual({ path: "new-path" });
    undo.redoAction();
    undo.redoAction();
    undo.redoAction();
    expect(renderedFill()).toBe("#FFFFFF");
  });

  it("does not autosave or add history for empty, repeated or already-reset choices", () => {
    const changed = vi.fn();
    const subscription = service.workflowChanged().subscribe(changed);
    service.setOperatorsColor([], "blue");
    service.setOperatorsColor([id], undefined);
    expect(undo.getUndoLength()).toBe(0);
    expect(changed).not.toHaveBeenCalled();
    service.setOperatorsColor([id], "blue");
    changed.mockClear();
    service.setOperatorsColor([id, id], "blue");
    expect(undo.getUndoLength()).toBe(1);
    expect(changed).not.toHaveBeenCalled();
    subscription.unsubscribe();
  });

  it("validates every id before editing any selected operator", () => {
    expect(() => service.setOperatorsColor([id, "missing"], "blue")).toThrow("operator with ID missing doesn't exist");
    expect(graph.getOperator(id)).not.toHaveProperty("color");
    expect(undo.getUndoLength()).toBe(0);
  });

  it.each(["url(https://example.com/x)", "", "BLUE", "__proto__", null, 1, {}])(
    "rejects invalid local values %j without partial changes",
    color => {
      expect(() => service.setOperatorsColor([id, otherId], color as WorkflowColor)).toThrow(/Invalid workflow color/);
      expect(graph.getOperator(id)).not.toHaveProperty("color");
      expect(graph.getOperator(otherId)).not.toHaveProperty("color");
      expect(undo.getUndoLength()).toBe(0);
    }
  );

  it("does not edit a workflow with modification disabled", () => {
    service.disableWorkflowModification();
    service.setOperatorsColor([id], "blue");
    expect(graph.getOperator(id)).not.toHaveProperty("color");
    expect(undo.getUndoLength()).toBe(0);
  });

  it("does not edit readonly metadata before the modification stream locks", () => {
    service.setWorkflowMetadata({ ...service.getWorkflowMetadata(), readonly: true });
    service.setOperatorsColor([id], "blue");
    expect(graph.getOperator(id)).not.toHaveProperty("color");
  });

  it.each(["url(https://example.com/x)", "toString", null, 42, { unexpected: true }])(
    "safely renders malformed imported color %j and permits reset",
    color => {
      service.addOperator(
        { ...mockScanPredicate, operatorID: "imported", color } as unknown as OperatorPredicate,
        mockPoint
      );
      expect(service.getJointGraph().getCell("imported").attr("rect.body/fill")).toBe("#FFFFFF");
      service.setOperatorsColor(["imported"], undefined);
      expect(graph.getOperator("imported")).not.toHaveProperty("color");
    }
  );

  it("preserves colors through JSON reload and duplication", () => {
    service.setOperatorsColor([id], "purple");
    const workflow = JSON.parse(JSON.stringify(service.getWorkflow()));
    service.reloadWorkflow(workflow, false, false);
    graph = service.getTexeraGraph() as WorkflowGraph;
    expect(renderedFill()).toBe(fill("purple"));
    service.addOperator({ ...graph.getOperator(id), operatorID: "copy" }, mockPoint);
    expect(service.getJointGraph().getCell("copy").attr("rect.body/fill")).toBe(fill("purple"));
  });

  it("uses disabled gray ahead of a custom color", () => {
    service.disableOperators([id]);
    service.setOperatorsColor([id], "purple");
    expect(graph.getOperator(id).color).toBe("purple");
    expect(renderedFill()).toBe("#E0E0E0");
    expect(JointUIService.getOperatorFillColor({ ...graph.getOperator(id), isDisabled: false })).toBe(fill("purple"));
  });

  it("does not overwrite an active heat map when a color is changed or reset", () => {
    const wrapper = service.getJointGraphWrapper();
    wrapper.setHeatmapView(HeatmapView.Runtime);
    service.getJointGraph().getCell(id).attr("rect.body/fill", "#123456");
    service.setOperatorsColor([id], "blue");
    expect(renderedFill()).toBe("#123456");
    service.setOperatorsColor([id], undefined);
    expect(renderedFill()).toBe("#123456");
    // Full editor restoration is tested with a real paper in browser mode.
  });

  it("converges concurrent color choices while retaining unrelated remote properties", () => {
    const remote = new Y.Doc();
    try {
      Y.applyUpdate(remote, Y.encodeStateAsUpdate(graph.sharedModel.yDoc));
      const remoteOp = remote.getMap<Y.Map<unknown>>("operatorIDMap").get(id)!;
      service.setOperatorsColor([id], "blue");
      remoteOp.set("color", new Y.Text("orange"));
      remoteOp.set("customDisplayName", new Y.Text("Coeditor – 中文"));
      const update = Y.encodeStateAsUpdate(remote);
      Y.applyUpdate(remote, Y.encodeStateAsUpdate(graph.sharedModel.yDoc));
      Y.applyUpdate(graph.sharedModel.yDoc, update, remote);
      expect(graph.getOperator(id)).toEqual(remoteOp.toJSON());
      expect(["blue", "orange"]).toContain(graph.getOperator(id).color);
      expect(renderedFill()).toBe(fill(graph.getOperator(id).color!));
      undo.undoAction();
      expect(graph.getOperator(id).customDisplayName).toBe("Coeditor – 中文");
    } finally {
      remote.destroy();
    }
  });

  it("renders remote changes in a locked view without local undo history", () => {
    service.disableWorkflowModification();
    const remote = new Y.Doc();
    const changed = vi.fn();
    const subscription = service.workflowChanged().subscribe(changed);
    try {
      Y.applyUpdate(remote, Y.encodeStateAsUpdate(graph.sharedModel.yDoc));
      const op = remote.getMap<Y.Map<unknown>>("operatorIDMap").get(id)!;
      op.set("color", new Y.Text("green"));
      Y.applyUpdate(graph.sharedModel.yDoc, Y.encodeStateAsUpdate(remote), remote);
      expect(renderedFill()).toBe(fill("green"));
      expect(changed).toHaveBeenCalledTimes(1);
      expect(undo.getUndoLength()).toBe(0);
      (op.get("color") as Y.Text).insert(0, "bad ");
      Y.applyUpdate(graph.sharedModel.yDoc, Y.encodeStateAsUpdate(remote), remote);
      expect(renderedFill()).toBe("#FFFFFF");
      op.delete("color");
      Y.applyUpdate(graph.sharedModel.yDoc, Y.encodeStateAsUpdate(remote), remote);
      expect(graph.getOperator(id)).not.toHaveProperty("color");
    } finally {
      subscription.unsubscribe();
      remote.destroy();
    }
  });
});
