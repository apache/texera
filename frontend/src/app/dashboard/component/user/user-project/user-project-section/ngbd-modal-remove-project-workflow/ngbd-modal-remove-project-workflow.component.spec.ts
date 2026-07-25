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
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { Observable, of } from "rxjs";
import { NgbdModalRemoveProjectWorkflowComponent } from "./ngbd-modal-remove-project-workflow.component";
import { UserProjectService } from "../../../../../service/user/project/user-project.service";
import { DashboardWorkflow } from "../../../../../type/dashboard-workflow.interface";
import { commonTestProviders } from "../../../../../../common/testing/test-utils";

const PROJECT_ID = 7;

function makeWorkflow(wid: number): DashboardWorkflow {
  return {
    isOwner: true,
    ownerName: undefined,
    workflow: { wid, name: `wf-${wid}`, creationTime: 0, lastModifiedTime: 0 } as any,
    projectIDs: [],
    accessLevel: "WRITE",
    ownerId: 1,
    coverImage: null,
  };
}

describe("NgbdModalRemoveProjectWorkflowComponent", () => {
  let fixture: ComponentFixture<NgbdModalRemoveProjectWorkflowComponent>;
  let component: NgbdModalRemoveProjectWorkflowComponent;
  let userProjectService: {
    retrieveWorkflowsOfProject: ReturnType<typeof vi.fn>;
    removeWorkflowFromProject: ReturnType<typeof vi.fn>;
  };

  beforeEach(async () => {
    userProjectService = {
      retrieveWorkflowsOfProject: vi.fn().mockReturnValue(of([])),
      removeWorkflowFromProject: vi.fn().mockReturnValue(of({} as Response)),
    };

    await TestBed.configureTestingModule({
      imports: [NgbdModalRemoveProjectWorkflowComponent, HttpClientTestingModule],
      providers: [
        { provide: UserProjectService, useValue: userProjectService },
        { provide: NZ_MODAL_DATA, useValue: { projectId: PROJECT_ID } },
        ...commonTestProviders,
      ],
    }).compileComponents();
  });

  function createComponent(workflows: DashboardWorkflow[] = []): void {
    userProjectService.retrieveWorkflowsOfProject.mockReturnValue(of(workflows));
    fixture = TestBed.createComponent(NgbdModalRemoveProjectWorkflowComponent);
    component = fixture.componentInstance;
    fixture.detectChanges(); // triggers ngOnInit -> refreshProjectWorkflowEntries
  }

  it("should create", () => {
    createComponent();
    expect(component).toBeTruthy();
  });

  it("reads the project id from the modal data", () => {
    createComponent();
    expect(component.projectId).toBe(PROJECT_ID);
  });

  describe("ngOnInit", () => {
    it("loads the project's workflows and initializes the checkboxes as unchecked", () => {
      createComponent([makeWorkflow(10), makeWorkflow(20)]);
      expect(userProjectService.retrieveWorkflowsOfProject).toHaveBeenCalledWith(PROJECT_ID);
      expect(component.addedWorkflows).toHaveLength(2);
      expect(component.checkedWorkflows).toEqual([false, false]);
    });
  });

  describe("isAllChecked", () => {
    it("is false when there are no workflows", () => {
      createComponent();
      expect(component.isAllChecked()).toBe(false);
    });

    it("is true only when every checkbox is checked", () => {
      createComponent([makeWorkflow(10), makeWorkflow(20)]);
      component.checkedWorkflows = [true, true];
      expect(component.isAllChecked()).toBe(true);

      component.checkedWorkflows = [true, false];
      expect(component.isAllChecked()).toBe(false);
    });
  });

  describe("changeAll", () => {
    it("checks every box when not all are checked", () => {
      createComponent([makeWorkflow(10), makeWorkflow(20)]);
      component.checkedWorkflows = [true, false];
      component.changeAll();
      expect(component.checkedWorkflows).toEqual([true, true]);
    });

    it("unchecks every box when all are checked", () => {
      createComponent([makeWorkflow(10), makeWorkflow(20)]);
      component.checkedWorkflows = [true, true];
      component.changeAll();
      expect(component.checkedWorkflows).toEqual([false, false]);
    });

    it("is a safe no-op on an empty list", () => {
      createComponent();
      component.changeAll();
      expect(component.checkedWorkflows).toEqual([]);
    });
  });

  describe("submitForm", () => {
    it("removes only the checked workflows and drops them from the retained list", () => {
      createComponent([makeWorkflow(10), makeWorkflow(20), makeWorkflow(30)]);
      component.checkedWorkflows = [false, true, true];

      component.submitForm();

      expect(userProjectService.removeWorkflowFromProject).toHaveBeenCalledTimes(2);
      // Iterates high index -> low, so the calls land in wid order 30 then 20.
      expect(userProjectService.removeWorkflowFromProject).toHaveBeenNthCalledWith(1, PROJECT_ID, 30);
      expect(userProjectService.removeWorkflowFromProject).toHaveBeenNthCalledWith(2, PROJECT_ID, 20);
      expect(component.addedWorkflows.map(w => w.workflow.wid)).toEqual([10]);
    });

    it("removes every workflow when all are checked", () => {
      createComponent([makeWorkflow(10), makeWorkflow(20)]);
      component.checkedWorkflows = [true, true];

      component.submitForm();

      expect(userProjectService.removeWorkflowFromProject).toHaveBeenCalledTimes(2);
      expect(userProjectService.removeWorkflowFromProject).toHaveBeenNthCalledWith(1, PROJECT_ID, 20);
      expect(userProjectService.removeWorkflowFromProject).toHaveBeenNthCalledWith(2, PROJECT_ID, 10);
      expect(component.addedWorkflows).toEqual([]);
    });

    it("does nothing when no workflow is checked", () => {
      createComponent([makeWorkflow(10), makeWorkflow(20)]);
      component.checkedWorkflows = [false, false];

      component.submitForm();

      expect(userProjectService.removeWorkflowFromProject).not.toHaveBeenCalled();
      expect(component.addedWorkflows.map(w => w.workflow.wid)).toEqual([10, 20]);
    });

    it("subscribes to the removal requests so they are actually dispatched", () => {
      // Real removeWorkflowFromProject returns a cold HttpClient observable that only
      // fires the request on subscription; a cold double here proves submitForm's
      // forkJoin(...).subscribe() actually subscribes (not just builds the observables).
      createComponent([makeWorkflow(10), makeWorkflow(20)]);
      let subscriptions = 0;
      userProjectService.removeWorkflowFromProject.mockReturnValue(
        new Observable<Response>(observer => {
          subscriptions++;
          observer.next({} as Response);
          observer.complete();
        })
      );
      component.checkedWorkflows = [true, true];

      component.submitForm();

      expect(subscriptions).toBe(2);
    });
  });

  describe("template wiring", () => {
    it("renders a row per workflow and connects the select-all and Confirm controls", () => {
      createComponent([makeWorkflow(10), makeWorkflow(20)]);

      const rows = fixture.nativeElement.querySelectorAll("tbody tr");
      expect(rows.length).toBe(2);
      expect(rows[0].textContent).toContain("10");
      expect(rows[0].textContent).toContain("wf-10");

      // Header checkbox is bound to (change)="changeAll()".
      const selectAll = fixture.nativeElement.querySelector('thead input[type="checkbox"]') as HTMLInputElement;
      selectAll.click();
      fixture.detectChanges();
      expect(component.checkedWorkflows).toEqual([true, true]);

      // Confirm button is bound to (click)="submitForm()".
      (fixture.nativeElement.querySelector('button[aria-label="Confirm"]') as HTMLButtonElement).click();
      expect(userProjectService.removeWorkflowFromProject).toHaveBeenCalledTimes(2);
    });
  });
});
