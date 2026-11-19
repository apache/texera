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
import { testMacro, testPort, setMacroGraph } from "../../../common/testing/macro-graph";

/**
 * Mouse / pointer event tests for WorkflowEditorComponent.
 *
 * These tests exercise JointJS event paths (cell-view jQuery
 * `.trigger("mousedown" | "dblclick")` dispatch, blank-area paper
 * clicks, shift-click multi-select) that depend on real-DOM SVG hit
 * testing and canvas measurement. jsdom does not implement those
 * paths, so the tests live in a `.browser.spec.ts` file that is
 * skipped by the default jsdom `test` target and picked up only by
 * the `test-browser` target (Vitest + Playwright Chromium).
 *
 * Originally part of workflow-editor.component.spec.ts; commented out
 * in PR #5146 to keep CI green after the file was added to the jsdom
 * runner. Restored here per issue #5318.
 */

import { macroInstance } from "../../../common/util/workflow-macro";
import { readMacroInterface } from "../../../../../../common/workflow-core/src/main/typescript/workflow-macros";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import * as jQuery from "jquery";
import * as joint from "jointjs";

import { WorkflowEditorComponent } from "./workflow-editor.component";
import { NzModalCommentBoxComponent } from "./comment-box-modal/nz-modal-comment-box.component";
import { configureWorkflowEditor } from "./workflow-editor.test-utils";

import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import {
  mockCommentBox,
  mockPoint,
  mockResultPredicate,
  mockScanPredicate,
  mockSentimentPredicate,
} from "../../service/workflow-graph/model/mock-workflow-data";
import { createYTypeFromObject } from "../../types/shared-editing.interface";
import { page, userEvent } from "vitest/browser";

import { UndoRedoService } from "../../service/undo-redo/undo-redo.service";
import { exportedWorkflow } from "../../../common/type/workflow";
import { PortPropertyEditFrameComponent } from "../property-editor/port-property-edit-frame/port-property-edit-frame.component";

const createJQueryEvent = (event: string, properties?: object): JQuery.Event =>
  (jQuery as unknown as JQueryStatic).Event(event, properties);

describe("WorkflowEditorComponent - mouse and pointer event integration", () => {
  let component: WorkflowEditorComponent;
  let fixture: ComponentFixture<WorkflowEditorComponent>;
  let workflowActionService: WorkflowActionService;
  let nzModalService: NzModalService;

  beforeEach(configureWorkflowEditor);

  beforeEach(() => {
    fixture = TestBed.createComponent(WorkflowEditorComponent);
    component = fixture.componentInstance;
    workflowActionService = TestBed.inject(WorkflowActionService);
    workflowActionService.setHighlightingEnabled(true);
    nzModalService = TestBed.inject(NzModalService);
    fixture.detectChanges();
  });

  const prepareMacroCanvas = async (x = 300, y = 70) => {
    await page.viewport(1200, 600);
    workflowActionService.suspendWorkflow().shared.wsProvider.disconnect();
    fixture.nativeElement.style.cssText = "display:block;width:1200px;height:500px";
    component.paper.setDimensions(1200, 500);
    component.paper.translate(x, y);
  };
  const macroInterface = () => workflowActionService.getMacroInterface()!;
  const terminalID = (direction: "inputs" | "outputs") =>
    workflowActionService
      .getTexeraGraph()
      .getAllOperators()
      .find(op => op.operatorType === (direction === "inputs" ? "MacroInput" : "MacroOutput"))!.operatorID;
  const terminalSVG = (direction: "inputs" | "outputs", selector: string) =>
    component.paper.findViewByModel(terminalID(direction)).el.querySelector(selector)!;
  const magnet = (operatorID: string, portID: string) => {
    // JointJS recreates port SVG nodes during a render. Track the graph port through those updates.
    const id = `port:${operatorID}:${portID}`;
    const cell = component.paper.getModelById(operatorID) as joint.dia.Element;
    cell.portProp(portID, "attrs/.port-body/data-testid", id);
    return page.getByTestId(id);
  };

  it("connects each saved macro output to an ordinary operator by dragging its magnet", async () => {
    await prepareMacroCanvas(100, 50);
    const definition = testMacro("macro", [mockScanPredicate], {
      outputs: ["stream", "sorted"].map(portID => testPort(portID, mockScanPredicate.operatorID, "output-0")),
      name: "Macro",
    });
    workflowActionService.setMacroDependencies([definition]);
    workflowActionService.addOperator(macroInstance(definition, "macro"), { x: 100, y: 120 });
    for (const [index, operatorID] of ["first", "second"].entries())
      workflowActionService.addOperator({ ...mockSentimentPredicate, operatorID }, { x: 350, y: 60 + index * 160 });
    const graph = workflowActionService.getTexeraGraph();
    for (const [port, target] of [
      ["stream", "first"],
      ["sorted", "second"],
    ]) {
      await userEvent.dragAndDrop(magnet("macro", port), magnet(target, "input-0"));
      expect(graph.getAllLinks()).toContainEqual(
        expect.objectContaining({
          source: { operatorID: "macro", portID: port },
          target: { operatorID: target, portID: "input-0" },
        })
      );
    }
    const connected = graph.getAllLinks();
    TestBed.inject(UndoRedoService).undoAction();
    expect(graph.getAllLinks().length).toBeLessThan(connected.length);
    TestBed.inject(UndoRedoService).redoAction();
    expect(graph.getAllLinks()).toEqual(connected);
  });

  it("adds, wires, selects and removes grouped macro ports through real SVG controls", async () => {
    await prepareMacroCanvas();
    for (const [index, operatorID] of ["a", "b"].entries())
      workflowActionService.addOperator({ ...mockSentimentPredicate, operatorID }, { x: 100 + index * 200, y: 140 });
    setMacroGraph(workflowActionService, { id: "macro", inputs: [], outputs: [] });
    const graph = workflowActionService.getJointGraph();
    const operator = component.paper.findViewByModel("a").el;
    expect(terminalSVG("inputs", ".body").getBoundingClientRect().width).toBe(
      operator.querySelector(".body")!.getBoundingClientRect().width
    );
    expect(getComputedStyle(terminalSVG("inputs", ".add-output-port-button")).visibility).toBe("hidden");
    await userEvent.click(terminalSVG("inputs", ".boundary"));
    expect(getComputedStyle(terminalSVG("inputs", ".add-output-port-button")).visibility).toBe("visible");
    await userEvent.click(terminalSVG("inputs", ".add-output-port-button rect"));
    await userEvent.click(terminalSVG("inputs", ".add-output-port-button rect"));
    await userEvent.click(terminalSVG("outputs", ".boundary"));
    await userEvent.click(terminalSVG("outputs", ".add-input-port-button rect"));
    const {
      inputs: [first, second],
      outputs: [output],
    } = macroInterface();
    expect(graph.getElements()).toHaveLength(4);
    const boundaryMagnet = (direction: "inputs" | "outputs", id: string) => magnet(terminalID(direction), id);
    await userEvent.dragAndDrop(boundaryMagnet("inputs", first.portID), magnet("a", "input-0"));
    await userEvent.dragAndDrop(magnet("a", "output-0"), boundaryMagnet("outputs", output.portID));
    const firstInput = { operatorID: "a", portID: "input-0" };
    const firstOutput = { operatorID: "a", portID: "output-0" };
    expect(macroInterface().inputs[0].targets).toEqual([firstInput]);
    expect(macroInterface().outputs[0].targets).toEqual([firstOutput]);
    await userEvent.dragAndDrop(magnet("b", "output-0"), boundaryMagnet("outputs", output.portID));
    expect(macroInterface().outputs[0].targets).toEqual([firstOutput]);
    await userEvent.click(terminalSVG("outputs", ".boundary"));
    await userEvent.click(terminalSVG("outputs", ".add-input-port-button rect"));
    const secondOutput = macroInterface().outputs[1];
    await userEvent.dragAndDrop(magnet("b", "output-0"), boundaryMagnet("outputs", secondOutput.portID));
    expect(macroInterface().outputs.map(p => p.targets)).toEqual([
      [firstOutput],
      [{ operatorID: "b", portID: "output-0" }],
    ]);
    await userEvent.click(terminalSVG("inputs", ".boundary"));
    await userEvent.click(terminalSVG("inputs", ".joint-port-label"));
    expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedPortIDs()).toContainEqual({
      operatorID: terminalID("inputs"),
      portID: first.portID,
    });
    await userEvent.click(terminalSVG("inputs", ".remove-output-port-button rect"));
    expect(macroInterface().inputs.map(p => p.portID)).toEqual([second.portID]);
    TestBed.inject(UndoRedoService).undoAction();
    expect(macroInterface().inputs[0].targets).toEqual([firstInput]);
    await userEvent.click(boundaryMagnet("inputs", second.portID));
    await userEvent.click(terminalSVG("inputs", ".remove-output-port-button rect"));
    const content = workflowActionService.getWorkflowContent();
    expect(JSON.parse(JSON.stringify(exportedWorkflow(content, undefined)))).toEqual(
      JSON.parse(JSON.stringify(content))
    );
    expect(content.operators).toHaveLength(4);
    expect(content.links).toHaveLength(3);
    component.structureLocked = true;
    fixture.detectChanges();
    expect(terminalSVG("inputs", ".add-output-port-button").getBoundingClientRect().width).toBe(0);
  });

  it.each(["inputs", "outputs"] as const)(
    "removes a %s port with minus without first selecting a port",
    async direction => {
      await prepareMacroCanvas();
      const ports = [0, 1].map(i => ({ portID: `port-${i}`, targets: [] }));
      setMacroGraph(workflowActionService, { id: "macro", inputs: ports, outputs: ports });
      const clickBoundary = () => userEvent.click(terminalSVG(direction, ".boundary"));
      const side = direction === "inputs" ? "output" : "input";
      const minus = () => userEvent.click(terminalSVG(direction, `.remove-${side}-port-button rect`));
      await userEvent.click(terminalSVG(direction === "inputs" ? "outputs" : "inputs", ".port-body"));
      await clickBoundary();
      await minus();
      expect(macroInterface()[direction].map(p => p.portID)).toEqual(["port-0"]);
      TestBed.inject(UndoRedoService).undoAction();
      expect(macroInterface()[direction]).toEqual(ports);
      await clickBoundary();
      await minus();
      expect(macroInterface()[direction].map(p => p.portID)).toEqual(["port-0"]);
      await minus();
      await minus();
      expect(macroInterface()[direction]).toEqual([]);
    }
  );

  it("renames a saved terminal through the native port name editor, including clearing and Undo/Redo", async () => {
    workflowActionService.suspendWorkflow().shared.wsProvider.disconnect();
    workflowActionService.addOperator(mockSentimentPredicate, mockPoint);
    setMacroGraph(workflowActionService, {
      id: "macro",
      inputs: [testPort("rows", mockSentimentPredicate.operatorID, "input-0", "Rows")],
      outputs: [],
    });
    const before = workflowActionService.getWorkflowContent();
    const snapshot = JSON.stringify(before);
    const properties = TestBed.createComponent(PortPropertyEditFrameComponent);
    properties.componentRef.setInput("currentPortID", { operatorID: terminalID("inputs"), portID: "rows" });
    properties.detectChanges();
    await userEvent.click(properties.nativeElement.querySelector("button"));
    properties.detectChanges();
    const editor = properties.nativeElement.querySelector(".ql-editor") as HTMLElement;
    await userEvent.clear(editor);
    expect(macroInterface().inputs[0].displayName).toBe("");
    await userEvent.type(editor, "Rows 東京");
    await userEvent.keyboard("{Home}{ArrowRight}{ArrowRight}{ArrowRight}{ArrowRight} café");
    expect(macroInterface().inputs[0].displayName).toBe("Rows café 東京");
    expect(document.activeElement).toBe(editor);
    expect(workflowActionService.getWorkflowContent().links).toEqual(before.links);
    expect(JSON.stringify(before)).toBe(snapshot);
    const copied = JSON.parse(JSON.stringify(exportedWorkflow(workflowActionService.getWorkflowContent(), undefined)));
    expect(readMacroInterface(copied)!.inputs[0].displayName).toBe("Rows café 東京");
    properties.componentInstance.disconnectQuillFromText();
    TestBed.inject(UndoRedoService).undoAction();
    TestBed.inject(UndoRedoService).redoAction();
    expect(macroInterface().inputs[0].displayName).toBe("Rows café 東京");
    properties.componentRef.setInput("actsAsEditor", false);
    properties.detectChanges();
    expect(properties.nativeElement.querySelector("button").disabled).toBe(true);
    properties.destroy();
  });

  it("should try to highlight the operator when user mouse clicks on an operator", () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();
    // install a spy on the highlight operator function and pass the call through
    vi.spyOn(jointGraphWrapper, "highlightOperators");
    workflowActionService.addOperator(mockScanPredicate, mockPoint);

    // unhighlight the operator in case it's automatically highlighted
    jointGraphWrapper.unhighlightOperators(mockScanPredicate.operatorID);

    // find the joint Cell View object of the operator element
    const jointCellView = component.paper.findViewByModel(mockScanPredicate.operatorID);
    jointCellView.$el.trigger("mousedown");

    fixture.detectChanges();

    // assert the highlighted operator is correct
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([mockScanPredicate.operatorID]);
  });

  it("should highlight the commentBox when user clicks on a commentBox", () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();
    vi.spyOn(jointGraphWrapper, "highlightCommentBoxes");
    workflowActionService.addCommentBox(mockCommentBox);
    jointGraphWrapper.unhighlightCommentBoxes(mockCommentBox.commentBoxID);
    const jointCellView = component.paper.findViewByModel(mockCommentBox.commentBoxID);
    jointCellView.$el.trigger("mousedown");
    fixture.detectChanges();
    expect(jointGraphWrapper.getCurrentHighlightedCommentBoxIDs()).toEqual([mockCommentBox.commentBoxID]);
  });

  it("should open commentBox as NzModal when user double clicks on a commentBox", () => {
    const modalRef: NzModalRef = nzModalService.create({
      nzTitle: "CommentBox",
      nzContent: NzModalCommentBoxComponent,
      nzData: { commentBox: createYTypeFromObject(mockCommentBox) },
      nzAutofocus: null,
      nzFooter: [
        {
          label: "OK",
          onClick: () => {
            modalRef.destroy();
          },
          type: "primary",
        },
      ],
    });
    vi.spyOn(nzModalService, "create").mockReturnValue(modalRef);
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();
    workflowActionService.addCommentBox(mockCommentBox);
    jointGraphWrapper.highlightCommentBoxes(mockCommentBox.commentBoxID);
    const jointCellView = component.paper.findViewByModel(mockCommentBox.commentBoxID);
    jointCellView.$el.trigger("dblclick");
    expect(nzModalService.create).toHaveBeenCalled();
    fixture.detectChanges();
    modalRef.destroy();
  });

  it("should unhighlight all highlighted operators when user mouse clicks on the blank space", () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add and highlight two operators
    workflowActionService.addOperatorsAndLinks(
      [
        { op: mockScanPredicate, pos: mockPoint },
        { op: mockResultPredicate, pos: mockPoint },
      ],
      []
    );
    jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID, mockResultPredicate.operatorID);

    // assert that both operators are highlighted
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toContain(mockScanPredicate.operatorID);
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toContain(mockResultPredicate.operatorID);

    // find a blank area on the JointJS paper
    const blankPoint = { x: mockPoint.x + 100, y: mockPoint.y + 100 };
    expect(component.paper.findViewsFromPoint(blankPoint)).toEqual([]);

    // trigger a click on the blank area using JointJS paper's jQuery element
    const point = component.paper.localToClientPoint(blankPoint);
    const event = createJQueryEvent("mousedown", {
      clientX: point.x,
      clientY: point.y,
    });
    component.paper.$el.trigger(event);

    fixture.detectChanges();

    // assert that all operators are unhighlighted
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([]);
  });

  it("should highlight multiple operators when user clicks on them with shift key pressed", () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockResultPredicate, mockPoint);
    jointGraphWrapper.highlightOperators(mockResultPredicate.operatorID);

    // assert that only the last operator is highlighted
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toContain(mockResultPredicate.operatorID);
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).not.toContain(mockScanPredicate.operatorID);

    // find the joint Cell View object of the first operator element
    const jointCellView = component.paper.findViewByModel(mockScanPredicate.operatorID);

    // trigger a shift click on the cell view using its jQuery element
    const event = createJQueryEvent("mousedown", { shiftKey: true });
    jointCellView.$el.trigger(event);

    fixture.detectChanges();

    // assert that both operators are highlighted
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toContain(mockScanPredicate.operatorID);
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toContain(mockResultPredicate.operatorID);
  });

  it("should unhighlight the highlighted operator when user clicks on it with shift key pressed", () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);

    // assert that the operator is highlighted
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toContain(mockScanPredicate.operatorID);

    // find the joint Cell View object of the operator element
    const jointCellView = component.paper.findViewByModel(mockScanPredicate.operatorID);

    // trigger a shift click on the cell view using its jQuery element
    const event = createJQueryEvent("mousedown", { shiftKey: true });
    jointCellView.$el.trigger(event);

    fixture.detectChanges();

    // assert that the operator is unhighlighted
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).not.toContain(mockScanPredicate.operatorID);
  });
});
