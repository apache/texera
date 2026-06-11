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

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import * as jQuery from "jquery";

import { WorkflowEditorComponent } from "./workflow-editor.component";
import { NzModalCommentBoxComponent } from "./comment-box-modal/nz-modal-comment-box.component";
import { workflowEditorTestImports, workflowEditorTestProviders } from "./workflow-editor.test-utils";

import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import {
  mockCommentBox,
  mockPoint,
  mockResultPredicate,
  mockScanPredicate,
} from "../../service/workflow-graph/model/mock-workflow-data";
import { createYTypeFromObject } from "../../types/shared-editing.interface";

const createJQueryEvent = (event: string, properties?: object): JQuery.Event =>
  (jQuery as unknown as JQueryStatic).Event(event, properties);

describe("WorkflowEditorComponent - mouse and pointer event integration", () => {
  let component: WorkflowEditorComponent;
  let fixture: ComponentFixture<WorkflowEditorComponent>;
  let workflowActionService: WorkflowActionService;
  let nzModalService: NzModalService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: workflowEditorTestImports,
      providers: workflowEditorTestProviders,
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(WorkflowEditorComponent);
    component = fixture.componentInstance;
    workflowActionService = TestBed.inject(WorkflowActionService);
    workflowActionService.setHighlightingEnabled(true);
    nzModalService = TestBed.inject(NzModalService);
    fixture.detectChanges();
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
