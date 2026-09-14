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
import { WorkflowEditorComponent } from "../workflow-editor/workflow-editor.component";
import { workflowEditorTestImports, workflowEditorTestProviders } from "../workflow-editor/workflow-editor.test-utils";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import {
  mockScanPredicate,
  mockSentimentPredicate,
  mockScanSentimentLink,
  mockCommentBox,
} from "../../service/workflow-graph/model/mock-workflow-data";
import { NzContextMenuService } from "ng-zorro-antd/dropdown";

describe("Canvas selection gestures", () => {
  let fixture: ComponentFixture<WorkflowEditorComponent>;
  let editor: WorkflowEditorComponent;
  let actions: WorkflowActionService;
  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: workflowEditorTestImports,
      providers: workflowEditorTestProviders,
    }).compileComponents();
    fixture = TestBed.createComponent(WorkflowEditorComponent);
    editor = fixture.componentInstance;
    actions = TestBed.inject(WorkflowActionService);
    actions.setHighlightingEnabled(true);
    fixture.detectChanges();
    fixture.detectChanges();
    editor.paper.setDimensions(900, 600);
    actions.addOperator(mockScanPredicate, { x: 100, y: 100 });
    actions.addOperator(mockSentimentPredicate, { x: 300, y: 100 });
    actions.addLink(mockScanSentimentLink);
    actions.addCommentBox({ ...mockCommentBox, commentBoxPosition: { x: 150, y: 220 } });
  });
  afterEach(() => fixture.destroy());
  function drag(button: number, altKey = false): void {
    const start = editor.paper.localToClientPoint({ x: 50, y: 50 });
    const end = editor.paper.localToClientPoint({ x: 600, y: 400 });
    editor.paper.el.dispatchEvent(
      new MouseEvent("mousedown", { bubbles: true, button, altKey, clientX: start.x, clientY: start.y })
    );
    document.dispatchEvent(
      new MouseEvent("mousemove", { bubbles: true, buttons: button === 2 ? 2 : 1, clientX: end.x, clientY: end.y })
    );
    document.dispatchEvent(new MouseEvent("mouseup", { bubbles: true, button, clientX: end.x, clientY: end.y }));
    fixture.detectChanges();
  }
  it("selects operators, their link and comments by secondary drag without panning, even with zoom", () => {
    editor.paper.scale(0.75);
    editor.paper.translate(30, 40);
    const before = editor.paper.translate();
    drag(2);
    expect(actions.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()).toHaveLength(2);
    expect(actions.getJointGraphWrapper().getCurrentHighlightedCommentBoxIDs()).toContain(mockCommentBox.commentBoxID);
    expect(actions.getJointGraphWrapper().getCurrentHighlightedLinkIDs()).toContain(mockScanSentimentLink.linkID);
    expect(editor.paper.translate()).toEqual(before);
    expect(fixture.nativeElement.querySelector('[title="create a section box"]')).not.toBeNull();
  });
  it("supports Alt-primary drag and cancels the pending rectangle with Escape", () => {
    drag(0, true);
    expect(actions.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()).toHaveLength(2);
    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));
    fixture.detectChanges();
    expect(fixture.nativeElement.querySelector(".selection-actions")).toBeNull();
  });
  it("keeps a secondary click without dragging available for the context menu", () => {
    const spy = vi.spyOn(TestBed.inject(NzContextMenuService), "create").mockReturnValue({} as never);
    const options = { bubbles: true, button: 2, clientX: 20, clientY: 20 };
    editor.paper.el.dispatchEvent(new MouseEvent("mousedown", options));
    editor.paper.el.dispatchEvent(new MouseEvent("contextmenu", options));
    document.dispatchEvent(new MouseEvent("mouseup", options));
    expect(spy).toHaveBeenCalledTimes(1);
  });
});
