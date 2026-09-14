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
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowSnippetService } from "../../../service/workflow-snippet/workflow-snippet.service";
import { WorkflowSnippet } from "../../../types/workflow-snippet.interface";
import { ReusableSectionDetailComponent } from "./reusable-section-detail.component";

describe("ReusableSectionDetailComponent", () => {
  let fixture: ComponentFixture<ReusableSectionDetailComponent>;
  const section = {
    id: "section-1",
    name: "Clean text",
    description: "Normalize input text",
    fragment: {
      operators: [
        { operatorID: "op-1", operatorType: "PythonUDF" },
        { operatorID: "op-2", operatorType: "Filter" },
        { operatorID: "op-3", operatorType: "Projection" },
        { operatorID: "op-4", operatorType: "Sort" },
      ],
      operatorPositions: {
        "op-1": { x: 40, y: 50 },
        "op-2": { x: 160, y: 50 },
        "op-3": { x: 520, y: 50 },
        "op-4": { x: 520, y: 50 },
      },
      links: [
        { linkID: "link-1", source: { operatorID: "op-1" }, target: { operatorID: "op-2" } },
        { linkID: "link-2", source: { operatorID: "op-2" }, target: { operatorID: "op-3" } },
        { linkID: "link-3", source: { operatorID: "op-3" }, target: { operatorID: "op-4" } },
      ],
      sectionBox: { name: "Clean text", color: "#1677ff", width: 700, height: 180 },
    },
  } as unknown as WorkflowSnippet;
  const service = { getSnippets: vi.fn(() => [section]), insert: vi.fn(), delete: vi.fn() };
  const modalRef = { close: vi.fn() };

  beforeEach(async () => {
    vi.clearAllMocks();
    await TestBed.configureTestingModule({
      imports: [ReusableSectionDetailComponent],
      providers: [
        { provide: NZ_MODAL_DATA, useValue: { reusableSectionID: "section-1" } },
        { provide: NzModalRef, useValue: modalRef },
        { provide: WorkflowSnippetService, useValue: service },
        {
          provide: WorkflowActionService,
          useValue: { getJointGraphWrapper: () => ({ getMainJointPaper: () => undefined }) },
        },
        { provide: NotificationService, useValue: { success: vi.fn(), error: vi.fn() } },
      ],
    }).compileComponents();
    fixture = TestBed.createComponent(ReusableSectionDetailComponent);
    fixture.detectChanges();
  });

  it("shows only the requested details and a graph preview", () => {
    const element = fixture.nativeElement as HTMLElement;
    expect(element.textContent).toContain("Normalize input text");
    expect(Array.from(element.querySelectorAll(".field-label")).map(label => label.textContent?.trim())).toEqual([
      "Description",
      "Preview",
    ]);
    expect(element.querySelector("svg.reusable-section-preview")).not.toBeNull();
    expect(element.textContent).toContain("Insert into workflow");
    expect(element.textContent).toContain("Delete");
  });

  it("compacts distant operators so preview connections stay short", () => {
    expect(fixture.componentInstance.links).toHaveLength(3);
    expect(fixture.componentInstance.links.every(link => Math.abs(link.x2 - link.x1) < 250)).toBe(true);
  });

  it("keeps compacted operators from overlapping", () => {
    const nodes = fixture.componentInstance.nodes;
    nodes.forEach((node, index) => {
      nodes.slice(index + 1).forEach(other => {
        const separatedHorizontally = node.x + 100 <= other.x || other.x + 100 <= node.x;
        const separatedVertically = node.y + 42 <= other.y || other.y + 42 <= node.y;
        expect(separatedHorizontally || separatedVertically).toBe(true);
      });
    });
  });

  it("fits the view box around every laid-out operator", () => {
    const [x, y, width, height] = fixture.componentInstance.viewBox.split(" ").map(Number);
    fixture.componentInstance.nodes.forEach(node => {
      expect(node.x).toBeGreaterThan(x);
      expect(node.y).toBeGreaterThan(y);
      expect(node.x + 100).toBeLessThan(x + width);
      expect(node.y + 42).toBeLessThan(y + height);
    });
  });

  it("inserts the section and closes only after insertion succeeds", () => {
    fixture.componentInstance.insert();
    expect(service.insert).toHaveBeenCalledWith("section-1", { x: 400, y: 200 });
    expect(modalRef.close).toHaveBeenCalledWith(true);
  });

  it("keeps the preview open and reports an insertion failure", () => {
    service.insert.mockImplementationOnce(() => {
      throw new Error("Workflow is read-only.");
    });
    fixture.componentInstance.insert();
    expect(modalRef.close).not.toHaveBeenCalled();
    expect(TestBed.inject(NotificationService).error).toHaveBeenCalledWith("Workflow is read-only.");
  });

  it("deletes the library definition and closes", () => {
    fixture.componentInstance.delete();
    expect(service.delete).toHaveBeenCalledWith("section-1");
    expect(modalRef.close).toHaveBeenCalledWith(true);
  });
});
