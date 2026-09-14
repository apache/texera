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
import { BehaviorSubject, of } from "rxjs";
import { NzModalService } from "ng-zorro-antd/modal";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { WorkflowSnippetService } from "../../../service/workflow-snippet/workflow-snippet.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowSnippet } from "../../../types/workflow-snippet.interface";
import { MySnippetsComponent } from "./my-snippets.component";

describe("MySnippetsComponent", () => {
  let fixture: ComponentFixture<MySnippetsComponent>;
  let component: MySnippetsComponent;
  let snippetsSubject: BehaviorSubject<readonly WorkflowSnippet[]>;
  let snippetService: {
    snippets$: typeof snippetsSubject;
    getSnippets: ReturnType<typeof vi.fn>;
    insert: ReturnType<typeof vi.fn>;
    delete: ReturnType<typeof vi.fn>;
  };

  const snippet = {
    id: "snippet-1",
    schemaVersion: 1,
    name: "Clean text",
    description: "Normalize and tokenize",
    createdAt: "2026-09-11T00:00:00.000Z",
    updatedAt: "2026-09-11T00:00:00.000Z",
    fragment: { operators: [{ operatorID: "op-1" }], operatorPositions: {}, links: [] },
  } as unknown as WorkflowSnippet;

  beforeEach(async () => {
    snippetsSubject = new BehaviorSubject<readonly WorkflowSnippet[]>([]);
    snippetService = { snippets$: snippetsSubject, getSnippets: vi.fn(), insert: vi.fn(), delete: vi.fn() };
    const actionService = {
      getWorkflowModificationEnabledStream: () => of(true),
      getJointGraphWrapper: () => ({ getMainJointPaper: () => ({ translate: () => ({ tx: 25, ty: 10 }) }) }),
    };

    await TestBed.configureTestingModule({
      imports: [MySnippetsComponent],
      providers: [
        { provide: WorkflowSnippetService, useValue: snippetService },
        { provide: WorkflowActionService, useValue: actionService },
        { provide: NotificationService, useValue: { success: vi.fn(), error: vi.fn() } },
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(MySnippetsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("renders only each reusable section name and its insert button", () => {
    snippetsSubject.next([snippet]);
    fixture.detectChanges();

    expect(fixture.nativeElement.textContent).toContain("Clean text");
    expect(fixture.nativeElement.textContent).not.toContain("Normalize and tokenize");
    expect(fixture.nativeElement.textContent).not.toContain("1 operators");
    component.insert(snippet);

    expect(snippetService.insert).toHaveBeenCalledWith("snippet-1", { x: 375, y: 190 });
  });

  it("opens a reusable section detail when its name is clicked", () => {
    const modalService = fixture.debugElement.injector.get(NzModalService);
    const createSpy = vi.spyOn(modalService, "create").mockReturnValue({} as any);

    component.open(snippet);

    expect(createSpy).toHaveBeenCalledWith(
      expect.objectContaining({ nzTitle: "Clean text", nzData: { reusableSectionID: "snippet-1" } })
    );
  });

  it("shows only a short empty-state message when none have been saved", () => {
    expect(fixture.nativeElement.textContent).toContain("Start saving section boxes");
    expect(fixture.nativeElement.textContent).not.toContain("Saved in this browser");
    expect(fixture.nativeElement.textContent).not.toContain("Alt + drag");
    expect(getComputedStyle(fixture.nativeElement.querySelector(".snippet-library")).paddingTop).toBe("12px");
  });
});
