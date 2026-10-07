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
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { page, userEvent } from "vitest/browser";
import * as joint from "jointjs";
import { NzModalCommentBoxComponent } from "./nz-modal-comment-box.component";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowGraph } from "../../../service/workflow-graph/model/workflow-graph";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { UserService } from "../../../../common/service/user/user.service";
import { MOCK_USER, StubUserService } from "../../../../common/service/user/stub-user.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { UndoRedoService } from "../../../service/undo-redo/undo-redo.service";

// Real Chromium checks: jsdom cannot verify computed SVG fills, focus rings or wrapping.
describe("comment colors rendered in the browser", () => {
  let fixture: ComponentFixture<NzModalCommentBoxComponent>;
  let service: WorkflowActionService;
  let graph: WorkflowGraph;
  let container: HTMLDivElement;
  let paper: joint.dia.Paper;
  const id = "commentBox-color-test";
  const buttons = () => Array.from(container.querySelectorAll<HTMLButtonElement>(".comment-colors button"));
  const rect = () => paper.findViewByModel(id).el.querySelector<SVGRectElement>("rect.body")!;
  const nextFrame = () => new Promise<void>(resolve => requestAnimationFrame(() => resolve()));

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [NzModalCommentBoxComponent, HttpClientTestingModule, NoopAnimationsModule],
      providers: [
        ...commonTestProviders,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: UserService, useClass: StubUserService },
        { provide: NzModalRef, useValue: {} },
        { provide: NotificationService, useValue: { success: vi.fn(), error: vi.fn() } },
        {
          provide: NZ_MODAL_DATA,
          useFactory: (action: WorkflowActionService) => ({
            commentBox: action.getTexeraGraph().getSharedCommentBoxType(id),
          }),
          deps: [WorkflowActionService],
        },
      ],
    }).compileComponents();
    service = TestBed.inject(WorkflowActionService);
    graph = service.getTexeraGraph() as WorkflowGraph;
    service.addCommentBox({ commentBoxID: id, comments: [], commentBoxPosition: { x: 30, y: 20 } });
    graph.sharedModel.undoManager.clear();
    container = document.createElement("div");
    container.style.cssText = "width: 480px; padding: 12px; background: white;";
    document.body.appendChild(container);
    const canvas = document.createElement("div");
    container.appendChild(canvas);
    paper = new joint.dia.Paper({ el: canvas, model: service.getJointGraph(), width: 450, height: 110 });
    fixture = TestBed.createComponent(NzModalCommentBoxComponent);
    container.appendChild(fixture.nativeElement);
    (TestBed.inject(UserService) as unknown as StubUserService).userChangeSubject.next(MOCK_USER);
    fixture.detectChanges();
    await nextFrame();
  });

  afterEach(() => {
    fixture?.destroy();
    paper?.remove();
    graph?.destroyYModel();
    container?.remove();
    TestBed.resetTestingModule();
  });

  it("renders the selected SVG fill and restores it through undo and reset", async () => {
    expect(getComputedStyle(rect()).fill).toBe("rgb(242, 244, 245)");
    await page.getByRole("button", { name: "Blue", exact: true }).click();
    fixture.detectChanges();
    await nextFrame();
    expect(getComputedStyle(rect()).fill).toBe("rgb(214, 228, 255)");
    expect(buttons()[2].getAttribute("aria-pressed")).toBe("true");
    TestBed.inject(UndoRedoService).undoAction();
    fixture.detectChanges();
    expect(getComputedStyle(rect()).fill).toBe("rgb(242, 244, 245)");
    TestBed.inject(UndoRedoService).redoAction();
    fixture.detectChanges();
    await page.getByRole("button", { name: "Default (reset)", exact: true }).click();
    fixture.detectChanges();
    expect(getComputedStyle(rect()).fill).toBe("rgb(242, 244, 245)");
    expect(graph.getCommentBox(id)).not.toHaveProperty("color");
  });

  it("supports Tab, Enter and Space with a visible focus outline and non-color-only selection", async () => {
    buttons()[0].focus();
    await userEvent.keyboard("{Tab}{Tab}");
    expect(document.activeElement).toBe(buttons()[2]);
    const style = getComputedStyle(buttons()[2]);
    expect(style.outlineStyle).toBe("solid");
    expect(style.outlineWidth).toBe("3px");
    await userEvent.keyboard("{Enter}");
    fixture.detectChanges();
    expect(graph.getCommentBox(id).color).toBe("blue");
    expect(getComputedStyle(buttons()[2]).boxShadow).not.toBe("none");
    await userEvent.keyboard("{Tab}{Tab} ");
    fixture.detectChanges();
    expect(graph.getCommentBox(id).color).toBe("pink");
    expect(buttons()[4].getAttribute("aria-pressed")).toBe("true");
  });

  it("wraps all named choices inside a narrow dialog without clipping labels", async () => {
    container.style.width = "280px";
    await nextFrame();
    const palette = container.querySelector<HTMLElement>(".color-options")!;
    const bounds = palette.getBoundingClientRect();
    expect(new Set(buttons().map(button => button.getBoundingClientRect().top)).size).toBeGreaterThan(1);
    for (const button of buttons()) {
      const box = button.getBoundingClientRect();
      expect(box.left).toBeGreaterThanOrEqual(bounds.left);
      expect(box.right).toBeLessThanOrEqual(bounds.right + 1);
      expect(button.scrollWidth).toBeLessThanOrEqual(button.clientWidth);
      expect(box.height).toBeGreaterThanOrEqual(32);
      expect(getComputedStyle(button).color).toBe("rgb(38, 38, 38)");
    }
  });

  it("disables an already-open picker when workflow modification is locked", async () => {
    service.disableWorkflowModification();
    fixture.detectChanges();
    expect(buttons().every(button => button.disabled)).toBe(true);
    fixture.componentInstance.setColor("orange");
    expect(graph.getCommentBox(id)).not.toHaveProperty("color");
    expect(getComputedStyle(rect()).fill).toBe("rgb(242, 244, 245)");
  });
});
