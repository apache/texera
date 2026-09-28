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
import { WorkflowSnippetService } from "../../../service/workflow-snippet/workflow-snippet.service";
import { CreateSnippetModalComponent } from "./create-snippet-modal.component";

describe("CreateSnippetModalComponent", () => {
  let fixture: ComponentFixture<CreateSnippetModalComponent>;
  let component: CreateSnippetModalComponent;
  const modalRef = { close: vi.fn() };
  const snippetService = { createFromSectionBox: vi.fn() };
  const notificationService = { success: vi.fn(), error: vi.fn() };

  beforeEach(async () => {
    vi.clearAllMocks();
    await TestBed.configureTestingModule({
      imports: [CreateSnippetModalComponent],
      providers: [
        { provide: NZ_MODAL_DATA, useValue: { sectionBoxID: "commentBox-section", name: "Clean text" } },
        { provide: NzModalRef, useValue: modalRef },
        { provide: WorkflowSnippetService, useValue: snippetService },
        { provide: NotificationService, useValue: notificationService },
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(CreateSnippetModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("creates a reusable section from the section box and closes", () => {
    expect(fixture.nativeElement.textContent).not.toContain("Name");
    expect(fixture.nativeElement.textContent).toContain("Description");
    component.description = "Reusable cleaning";
    component.save();

    expect(snippetService.createFromSectionBox).toHaveBeenCalledWith("commentBox-section", "Reusable cleaning");
    expect(notificationService.success).toHaveBeenCalledWith('Reusable section box "Clean text" saved.');
    expect(modalRef.close).toHaveBeenCalledWith(true);
  });

  it("stays open and reports a storage error", () => {
    snippetService.createFromSectionBox.mockImplementationOnce(() => {
      throw new Error("Storage is full.");
    });

    component.save();

    expect(notificationService.error).toHaveBeenCalledWith("Storage is full.");
    expect(modalRef.close).not.toHaveBeenCalled();
  });
});
