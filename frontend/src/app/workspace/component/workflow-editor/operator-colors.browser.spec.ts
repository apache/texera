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

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { page, userEvent } from "vitest/browser";
import { WorkflowEditorComponent } from "./workflow-editor.component";
import { MiniMapComponent } from "./mini-map/mini-map.component";
import { OperatorColorPickerComponent } from "./operator-color-picker/operator-color-picker.component";
import { workflowEditorTestImports, workflowEditorTestProviders } from "./workflow-editor.test-utils";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { WorkflowGraph } from "../../service/workflow-graph/model/workflow-graph";
import {
  mockPoint,
  mockScanPredicate,
  mockSentimentPredicate,
} from "../../service/workflow-graph/model/mock-workflow-data";
import { UndoRedoService } from "../../service/undo-redo/undo-redo.service";
import { WorkflowStatusService } from "../../service/workflow-status/workflow-status.service";
import { HeatmapView } from "../../service/heatmap/heatmap-scoring";
import { HEATMAP_NO_DATA_COLOR } from "../../service/heatmap/heatmap-color";
import { OperatorState } from "../../types/execute-workflow.interface";
import { JointUIService } from "../../service/joint-ui/joint-ui.service";
import { OperatorMenuService } from "../../service/operator-menu/operator-menu.service";

describe("operator colors with real editor and minimap rendering", () => {
  let editor: ComponentFixture<WorkflowEditorComponent>;
  let minimap: ComponentFixture<MiniMapComponent>;
  let picker: ComponentFixture<OperatorColorPickerComponent>;
  let action: WorkflowActionService;
  let graph: WorkflowGraph;
  let container: HTMLDivElement;
  const id = mockScanPredicate.operatorID;
  const secondId = mockSentimentPredicate.operatorID;
  const rect = () => editor.componentInstance.paper.findViewByModel(id).el.querySelector<SVGRectElement>("rect.body")!;
  const modelFill = () => action.getJointGraph().getCell(id).attr("rect.body/fill");
  const nextFrame = () => new Promise<void>(resolve => requestAnimationFrame(() => resolve()));

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [...workflowEditorTestImports, MiniMapComponent, OperatorColorPickerComponent],
      providers: [...workflowEditorTestProviders, { provide: NZ_MODAL_DATA, useValue: { operatorIDs: [id] } }],
    }).compileComponents();
    action = TestBed.inject(WorkflowActionService);
    graph = action.getTexeraGraph() as WorkflowGraph;
    container = document.createElement("div");
    container.style.cssText = "width:960px; background:white;";
    document.body.appendChild(container);
    editor = TestBed.createComponent(WorkflowEditorComponent);
    // TestBed removes prior rootN hosts when creating another fixture. Keep these
    // real component hosts mounted together, as they are in the workspace.
    editor.nativeElement.removeAttribute("id");
    editor.nativeElement.style.cssText = "display:block; width:900px; height:400px;";
    container.appendChild(editor.nativeElement);
    editor.detectChanges();
    minimap = TestBed.createComponent(MiniMapComponent);
    minimap.nativeElement.removeAttribute("id");
    container.appendChild(minimap.nativeElement);
    minimap.detectChanges();
    action.addOperator(structuredClone(mockScanPredicate), mockPoint);
    action.addOperator(structuredClone(mockSentimentPredicate), { x: 450, y: 100 });
    graph.sharedModel.undoManager.clear();
    picker = TestBed.createComponent(OperatorColorPickerComponent);
    picker.nativeElement.style.display = "block";
    picker.nativeElement.style.width = "400px";
    container.appendChild(picker.nativeElement);
    picker.detectChanges();
    await nextFrame();
    expect(rect().isConnected).toBe(true);
  });

  afterEach(() => {
    picker?.destroy();
    minimap?.destroy();
    editor?.destroy();
    graph?.destroyYModel();
    container?.remove();
  });

  it("updates actual canvas and minimap fills, preserves borders, and supports undo/reset", async () => {
    TestBed.inject(JointUIService).changeOperatorState(editor.componentInstance.paper, id, OperatorState.Running);
    const border = rect().getAttribute("stroke");
    await page.getByRole("button", { name: "Blue", exact: true }).click();
    picker.detectChanges();
    await nextFrame();
    expect(getComputedStyle(rect()).fill).toBe("rgb(214, 228, 255)");
    const miniRect = minimap.nativeElement.querySelector('[model-id="' + id + '"] rect.body') as SVGRectElement;
    expect(miniRect).not.toBeNull();
    expect(getComputedStyle(miniRect).fill).toBe("rgb(214, 228, 255)");
    expect(rect().getAttribute("stroke")).toBe(border);
    TestBed.inject(UndoRedoService).undoAction();
    expect(getComputedStyle(rect()).fill).toBe("rgb(255, 255, 255)");
    TestBed.inject(UndoRedoService).redoAction();
    await page.getByRole("button", { name: "Default", exact: true }).click();
    picker.detectChanges();
    expect(graph.getOperator(id)).not.toHaveProperty("color");
    expect(getComputedStyle(rect()).fill).toBe("rgb(255, 255, 255)");
  });

  it("supports Tab, Enter and Space with visible focus and named selection", async () => {
    const buttons = picker.nativeElement.querySelectorAll("button") as NodeListOf<HTMLButtonElement>;
    buttons[0].focus();
    await userEvent.keyboard("{Tab}{Tab}");
    expect(document.activeElement).toBe(buttons[2]);
    expect(getComputedStyle(buttons[2]).outlineWidth).toBe("3px");
    expect(getComputedStyle(buttons[2]).outlineStyle).toBe("solid");
    await userEvent.keyboard("{Enter}");
    picker.detectChanges();
    expect(graph.getOperator(id).color).toBe("blue");
    expect(buttons[2].getAttribute("aria-pressed")).toBe("true");
    expect(getComputedStyle(buttons[2]).boxShadow).not.toBe("none");
    await userEvent.keyboard("{Tab}{Tab} ");
    picker.detectChanges();
    expect(graph.getOperator(id).color).toBe("pink");
  });

  it("opens the real menu dialog, applies a preset and closes with Done", async () => {
    const wrapper = action.getJointGraphWrapper();
    wrapper.unhighlightOperators(...wrapper.getCurrentHighlightedOperatorIDs());
    wrapper.highlightOperators(id);
    TestBed.inject(OperatorMenuService).openOperatorColorPicker();
    const dialog = page.getByRole("dialog");
    await dialog.getByRole("button", { name: "Blue", exact: true }).click();
    expect(graph.getOperator(id).color).toBe("blue");
    expect(getComputedStyle(rect()).fill).toBe("rgb(214, 228, 255)");
    await dialog.getByRole("button", { name: "Done", exact: true }).click();
    await expect.poll(() => document.querySelector('[role="dialog"]')).toBeNull();
  });

  it("wraps named choices in a narrow panel without clipping", async () => {
    picker.nativeElement.style.width = "280px";
    await nextFrame();
    const buttons = [...picker.nativeElement.querySelectorAll("button")] as HTMLButtonElement[];
    const bounds = picker.nativeElement.querySelector(".color-options").getBoundingClientRect();
    expect(new Set(buttons.map(button => button.getBoundingClientRect().top)).size).toBeGreaterThan(1);
    for (const button of buttons) {
      const box = button.getBoundingClientRect();
      expect(box.left).toBeGreaterThanOrEqual(bounds.left);
      expect(box.right).toBeLessThanOrEqual(bounds.right + 1);
      expect(button.scrollWidth).toBeLessThanOrEqual(button.clientWidth);
      expect(box.height).toBeGreaterThanOrEqual(32);
      expect(getComputedStyle(button).color).toBe("rgb(38, 38, 38)");
    }
  });

  it("shows disabled gray and restores the stored preset on re-enable", () => {
    action.setOperatorsColor([id], "purple");
    action.disableOperators([id]);
    expect(getComputedStyle(rect()).fill).toBe("rgb(224, 224, 224)");
    expect(graph.getOperator(id).color).toBe("purple");
    action.enableOperators([id]);
    expect(getComputedStyle(rect()).fill).toBe("rgb(239, 219, 255)");
  });

  it.each([HeatmapView.Runtime, HeatmapView.TimePerRow, HeatmapView.IoImbalance])(
    "keeps %s heat-map priority during color, disable, enable and state changes",
    view => {
      action.setOperatorsColor([id], "blue");
      const status = TestBed.inject(WorkflowStatusService);
      const metrics = {
        [id]: {
          operatorState: OperatorState.Running,
          aggregatedInputRowCount: 10,
          aggregatedOutputRowCount: 5,
          aggregatedDataProcessingTime: 100,
        },
        [secondId]: {
          operatorState: OperatorState.Running,
          aggregatedInputRowCount: 10,
          aggregatedOutputRowCount: 100,
          aggregatedDataProcessingTime: 200,
        },
      };
      status.setExternalStatus(metrics);
      const wrapper = action.getJointGraphWrapper();
      wrapper.setHeatmapView(view);
      const heatFill = modelFill();
      expect(heatFill).not.toBe("#d6e4ff");
      expect(heatFill).not.toBe(action.getJointGraph().getCell(secondId).attr("rect.body/fill"));
      action.setOperatorsColor([id], "green");
      expect(modelFill()).toBe(heatFill);
      action.disableOperators([id]);
      expect(modelFill()).toBe(heatFill);
      action.enableOperators([id]);
      expect(modelFill()).toBe(heatFill);
      status.setExternalStatus({ ...metrics, [id]: { ...metrics[id], operatorState: OperatorState.Completed } });
      expect(modelFill()).toBe(heatFill);
      const border = rect().getAttribute("stroke");
      wrapper.setHeatmapView(null);
      expect(modelFill()).toBe("#d9f7be");
      expect(rect().getAttribute("stroke")).toBe(border);
    }
  );

  it("keeps the no-data overlay through reset, and restores disabled gray when it is switched off", () => {
    action.setOperatorsColor([id], "orange");
    const wrapper = action.getJointGraphWrapper();
    wrapper.setHeatmapView(HeatmapView.TimePerRow);
    expect(modelFill()).toBe(HEATMAP_NO_DATA_COLOR);
    action.setOperatorsColor([id], undefined);
    expect(modelFill()).toBe(HEATMAP_NO_DATA_COLOR);
    action.disableOperators([id]);
    wrapper.setHeatmapView(null);
    expect(modelFill()).toBe("#E0E0E0");
    action.enableOperators([id]);
    expect(modelFill()).toBe("#FFFFFF");
  });

  it("reloads a colored workflow under an active overlay without showing the preset until it is off", () => {
    action.setOperatorsColor([id], "purple");
    const workflow = JSON.parse(JSON.stringify(action.getWorkflow()));
    const wrapper = action.getJointGraphWrapper();
    wrapper.setHeatmapView(HeatmapView.Runtime);
    action.reloadWorkflow(workflow, false, false);
    graph = action.getTexeraGraph() as WorkflowGraph;
    expect(modelFill()).toBe(HEATMAP_NO_DATA_COLOR);
    wrapper.setHeatmapView(null);
    expect(modelFill()).toBe("#efdbff");
  });
});
