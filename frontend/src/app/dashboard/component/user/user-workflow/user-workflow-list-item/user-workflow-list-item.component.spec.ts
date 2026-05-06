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

import { Component, ViewChild } from "@angular/core";
import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { UserWorkflowListItemComponent } from "./user-workflow-list-item.component";
import { FileSaverService } from "../../../../service/user/file/file-saver.service";
import { testWorkflowEntries } from "../../../user-dashboard-test-fixtures";
import { By } from "@angular/platform-browser";
import { StubWorkflowPersistService } from "../../../../../common/service/workflow-persist/stub-workflow-persist.service";
import { WorkflowPersistService } from "../../../../../common/service/workflow-persist/workflow-persist.service";
import { UserProjectService } from "../../../../service/user/project/user-project.service";
import { StubUserProjectService } from "../../../../service/user/project/stub-user-project.service";
import { NzListComponent } from "ng-zorro-antd/list";
import { NzModalModule } from "ng-zorro-antd/modal";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { provideRouter } from "@angular/router";
import { DashboardEntry } from "../../../../type/dashboard-entry";
import { NzTooltipModule } from "ng-zorro-antd/tooltip";
import { commonTestProviders } from "../../../../../common/testing/test-utils";

// UserWorkflowListItemComponent is rooted at <nz-list-item>; instantiating it
// outside an <nz-list> host throws "No provider found for NzListComponent".
@Component({
  standalone: true,
  imports: [NzListComponent, UserWorkflowListItemComponent],
  template: `
    <nz-list>
      <texera-user-workflow-list-item
        [entry]="entry"
        [editable]="editable"></texera-user-workflow-list-item>
    </nz-list>
  `,
})
class TestHostComponent {
  entry!: DashboardEntry;
  editable = true;
  @ViewChild(UserWorkflowListItemComponent, { static: true }) inner!: UserWorkflowListItemComponent;
}

describe("UserWorkflowListItemComponent", () => {
  let component: UserWorkflowListItemComponent;
  let fixture: ComponentFixture<TestHostComponent>;
  const fileSaverServiceSpy = { saveAs: vi.fn() } as any;
  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [TestHostComponent, NzModalModule, HttpClientTestingModule, NzTooltipModule],
      providers: [
        { provide: WorkflowPersistService, useValue: new StubWorkflowPersistService(testWorkflowEntries) },
        { provide: UserProjectService, useValue: new StubUserProjectService() },
        { provide: FileSaverService, useValue: fileSaverServiceSpy },
        provideRouter([]),
        ...commonTestProviders,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TestHostComponent);
    fixture.componentInstance.entry = testWorkflowEntries[0];
    fixture.componentInstance.editable = true;
    fixture.detectChanges();
    component = fixture.componentInstance.inner;
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("sends http request to backend to retrieve export json", () => {
    // Test the workflow download button.
    component.onClickDownloadWorkfllow();
    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledExactlyOnceWith(
      new Blob([JSON.stringify(testWorkflowEntries[0].workflow.workflow.content)], {
        type: "text/plain;charset=utf-8",
      }),
      "workflow 1.json"
    );
  });

  it("adding a workflow description adds a description to the workflow", waitForAsync(() => {
    fixture.whenStable().then(() => {
      let addWorkflowDescriptionBtn = fixture.debugElement.query(By.css(".add-description-btn"));
      expect(addWorkflowDescriptionBtn).toBeTruthy();
      addWorkflowDescriptionBtn.triggerEventHandler("click", null);
      fixture.detectChanges();
      let editableDescriptionInput = fixture.debugElement.nativeElement.querySelector(".workflow-editable-description");
      expect(editableDescriptionInput).toBeTruthy();
      vi.spyOn(component, "confirmUpdateWorkflowCustomDescription");
      sendInput(editableDescriptionInput, "dummy description added by focusing out the input element.").then(() => {
        fixture.detectChanges();
        editableDescriptionInput.dispatchEvent(new Event("focusout"));
        fixture.detectChanges();
        expect(component.confirmUpdateWorkflowCustomDescription).toHaveBeenCalledTimes(1);
      });
    });
  }));

  it("Editing a workflow description edits a description to the workflow", waitForAsync(() => {
    fixture.whenStable().then(() => {
      const workflowDescriptionLabel = fixture.debugElement.query(By.css(".workflow-description"));
      expect(workflowDescriptionLabel).toBeTruthy();
      workflowDescriptionLabel.triggerEventHandler("click", null);
      fixture.detectChanges();
      let editableDescriptionInput1 = fixture.debugElement.nativeElement.querySelector(
        ".workflow-editable-description"
      );
      expect(editableDescriptionInput1).toBeTruthy();
      vi.spyOn(component, "confirmUpdateWorkflowCustomDescription");
      sendInput(editableDescriptionInput1, "dummy description added by focusing out the input element.").then(() => {
        fixture.detectChanges();
        editableDescriptionInput1.dispatchEvent(new Event("focusout"));
        fixture.detectChanges();
        expect(component.confirmUpdateWorkflowCustomDescription).toHaveBeenCalledTimes(1);
      });
    });
  }));

  function sendInput(editableDescriptionInput: HTMLInputElement, text: string) {
    // Helper function to change the workflow description textbox.
    editableDescriptionInput.value = text;
    editableDescriptionInput.dispatchEvent(new Event("input"));
    fixture.detectChanges();
    return fixture.whenStable();
  }
});
