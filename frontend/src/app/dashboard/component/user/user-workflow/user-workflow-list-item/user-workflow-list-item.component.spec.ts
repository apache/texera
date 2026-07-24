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
import { testWorkflow1, testWorkflowEntries } from "../../../user-dashboard-test-fixtures";
import { By } from "@angular/platform-browser";
import { StubWorkflowPersistService } from "../../../../../common/service/workflow-persist/stub-workflow-persist.service";
import {
  DEFAULT_WORKFLOW_NAME,
  WorkflowPersistService,
} from "../../../../../common/service/workflow-persist/workflow-persist.service";
import { UserProjectService } from "../../../../service/user/project/user-project.service";
import { StubUserProjectService } from "../../../../service/user/project/stub-user-project.service";
import { DownloadService } from "../../../../service/user/download/download.service";
import { WorkflowExecutionHistoryComponent } from "../ngbd-modal-workflow-executions/workflow-execution-history.component";
import { Workflow } from "../../../../../common/type/workflow";
import { of } from "rxjs";
import { NzListComponent } from "ng-zorro-antd/list";
import { NzModalModule, NzModalService } from "ng-zorro-antd/modal";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { provideRouter } from "@angular/router";
import { DashboardEntry } from "../../../../type/dashboard-entry";
import { NzTooltipModule } from "ng-zorro-antd/tooltip";
import { commonTestProviders } from "../../../../../common/testing/test-utils";
import type { Mocked } from "vitest";

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

// A fresh DashboardEntry per call so methods that mutate the workflow (rename,
// remove-from-project) cannot leak into the shared testWorkflowEntries fixture.
function makeWorkflowEntry(workflowOverrides: Partial<Workflow> = {}, projectIDs: number[] = [1]): DashboardEntry {
  return new DashboardEntry({
    workflow: { ...testWorkflow1, ...workflowOverrides },
    isOwner: true,
    ownerName: "Texera",
    accessLevel: "Write",
    projectIDs: [...projectIDs],
    ownerId: 1,
    coverImage: null,
  });
}

describe("UserWorkflowListItemComponent", () => {
  let component: UserWorkflowListItemComponent;
  let fixture: ComponentFixture<TestHostComponent>;
  const fileSaverServiceSpy = { saveAs: vi.fn() } as unknown as Mocked<FileSaverService>;
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

  describe("method coverage", () => {
    afterEach(() => {
      vi.restoreAllMocks();
    });

    it("getProjectIds returns the set of the entry's project ids", () => {
      component.entry = makeWorkflowEntry({}, [1, 2, 3]);
      expect(component.getProjectIds()).toEqual(new Set([1, 2, 3]));
    });

    it("isLightColor reports light vs dark hex colors", () => {
      expect(component.isLightColor("ffffff")).toBe(true);
      expect(component.isLightColor("000000")).toBe(false);
    });

    describe("confirmUpdateWorkflowCustomName", () => {
      it("persists the new name, updates the workflow, and stops editing", () => {
        const persist = TestBed.inject(WorkflowPersistService);
        // The stub lacks updateWorkflowName; give this fresh-per-test instance a spy.
        const spy = ((persist as any).updateWorkflowName = vi.fn().mockReturnValue(of(undefined)));
        component.entry = makeWorkflowEntry({ wid: 5, name: "old" });
        component.editingName = true;

        component.confirmUpdateWorkflowCustomName("new name");

        expect(spy).toHaveBeenCalledWith(5, "new name");
        expect(component.workflow.name).toBe("new name");
        expect(component.editingName).toBe(false);
      });

      it("falls back to the default name when the input is empty", () => {
        const persist = TestBed.inject(WorkflowPersistService);
        const spy = ((persist as any).updateWorkflowName = vi.fn().mockReturnValue(of(undefined)));
        component.entry = makeWorkflowEntry({ wid: 5 });

        component.confirmUpdateWorkflowCustomName("");

        expect(spy).toHaveBeenCalledWith(5, DEFAULT_WORKFLOW_NAME);
        expect(component.workflow.name).toBe(DEFAULT_WORKFLOW_NAME);
      });

      it("is a no-op when the workflow has no id", () => {
        const persist = TestBed.inject(WorkflowPersistService);
        const spy = ((persist as any).updateWorkflowName = vi.fn());
        component.entry = makeWorkflowEntry({ wid: undefined });

        component.confirmUpdateWorkflowCustomName("x");

        expect(spy).not.toHaveBeenCalled();
      });
    });

    describe("confirmUpdateWorkflowCustomDescription", () => {
      it("persists the new description and stops editing", () => {
        const persist = TestBed.inject(WorkflowPersistService);
        const spy = ((persist as any).updateWorkflowDescription = vi.fn().mockReturnValue(of(undefined)));
        component.entry = makeWorkflowEntry({ wid: 5 });
        component.editingDescription = true;

        component.confirmUpdateWorkflowCustomDescription("new desc");

        expect(spy).toHaveBeenCalledWith(5, "new desc");
        expect(component.workflow.description).toBe("new desc");
        expect(component.editingDescription).toBe(false);
      });

      it("is a no-op when the workflow has no id", () => {
        const persist = TestBed.inject(WorkflowPersistService);
        const spy = ((persist as any).updateWorkflowDescription = vi.fn());
        component.entry = makeWorkflowEntry({ wid: undefined });

        component.confirmUpdateWorkflowCustomDescription("x");

        expect(spy).not.toHaveBeenCalled();
      });
    });

    it("removeWorkflowFromProject calls the service and prunes the entry's project ids", () => {
      const userProject = TestBed.inject(UserProjectService);
      const spy = vi.spyOn(userProject, "removeWorkflowFromProject").mockReturnValue(of({} as Response));
      component.entry = makeWorkflowEntry({ wid: 9 }, [1, 2]);

      component.removeWorkflowFromProject(1);

      expect(spy).toHaveBeenCalledWith(1, 9);
      expect([...component.entry.workflow.projectIDs]).toEqual([2]);
    });

    it("onClickGetWorkflowExecutions opens the execution-history modal for the workflow", () => {
      const modal = TestBed.inject(NzModalService);
      const spy = vi.spyOn(modal, "create").mockReturnValue({} as any);
      component.entry = makeWorkflowEntry({ wid: 9, name: "wf" });

      component.onClickGetWorkflowExecutions();

      expect(spy).toHaveBeenCalledWith(
        expect.objectContaining({
          nzContent: WorkflowExecutionHistoryComponent,
          nzData: { wid: 9 },
          nzTitle: "Execution results of Workflow: wf",
        })
      );
    });

    describe("onClickDownloadWorkfllow", () => {
      it("delegates to the download service with the workflow id and name", () => {
        const download = TestBed.inject(DownloadService);
        const spy = vi.spyOn(download, "downloadWorkflow").mockReturnValue(of(undefined) as any);
        component.entry = makeWorkflowEntry({ wid: 9, name: "wf" });

        component.onClickDownloadWorkfllow();

        expect(spy).toHaveBeenCalledWith(9, "wf");
      });

      it("does nothing when the workflow has no id", () => {
        const download = TestBed.inject(DownloadService);
        const spy = vi.spyOn(download, "downloadWorkflow");
        component.entry = makeWorkflowEntry({ wid: undefined });

        component.onClickDownloadWorkfllow();

        expect(spy).not.toHaveBeenCalled();
      });
    });
  });

  function sendInput(editableDescriptionInput: HTMLInputElement, text: string) {
    // Helper function to change the workflow description textbox.
    editableDescriptionInput.value = text;
    editableDescriptionInput.dispatchEvent(new Event("input"));
    fixture.detectChanges();
    return fixture.whenStable();
  }
});
