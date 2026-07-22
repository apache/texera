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
import { of } from "rxjs";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { NgbdModalAddProjectWorkflowComponent } from "./ngbd-modal-add-project-workflow.component";
import { UserProjectService } from "../../../../../service/user/project/user-project.service";
import { WorkflowPersistService } from "../../../../../../common/service/workflow-persist/workflow-persist.service";
import { DashboardWorkflow } from "../../../../../type/dashboard-workflow.interface";

const PROJECT_ID = 42;

function workflowEntry(wid: number, name: string): DashboardWorkflow {
  return {
    workflow: { wid, name, creationTime: 0, lastModifiedTime: 0 },
  } as unknown as DashboardWorkflow;
}

const wf1 = workflowEntry(1, "alpha");
const wf2 = workflowEntry(2, "beta");

describe("NgbdModalAddProjectWorkflowComponent", () => {
  let component: NgbdModalAddProjectWorkflowComponent;
  let fixture: ComponentFixture<NgbdModalAddProjectWorkflowComponent>;
  let userProjectService: {
    retrieveWorkflowsOfProject: ReturnType<typeof vi.fn>;
    addWorkflowToProject: ReturnType<typeof vi.fn>;
  };
  let workflowPersistService: { retrieveWorkflowsBySessionUser: ReturnType<typeof vi.fn> };

  beforeEach(async () => {
    userProjectService = {
      // No workflows already in the project, so every session workflow is "unadded".
      retrieveWorkflowsOfProject: vi.fn().mockReturnValue(of([])),
      addWorkflowToProject: vi.fn().mockReturnValue(of({} as Response)),
    };
    workflowPersistService = {
      retrieveWorkflowsBySessionUser: vi.fn().mockReturnValue(of([wf1, wf2])),
    };

    await TestBed.configureTestingModule({
      imports: [NgbdModalAddProjectWorkflowComponent],
      providers: [
        { provide: NZ_MODAL_DATA, useValue: { projectId: PROJECT_ID } },
        { provide: UserProjectService, useValue: userProjectService },
        { provide: WorkflowPersistService, useValue: workflowPersistService },
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(NgbdModalAddProjectWorkflowComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("creates and renders one row per unadded workflow", () => {
    expect(component).toBeTruthy();
    expect(userProjectService.retrieveWorkflowsOfProject).toHaveBeenCalledWith(PROJECT_ID);
    expect(workflowPersistService.retrieveWorkflowsBySessionUser).toHaveBeenCalled();
    expect(component.unaddedWorkflows).toEqual([wf1, wf2]);
    expect(component.checkedWorkflows).toEqual([false, false]);

    const rows = fixture.nativeElement.querySelectorAll("tbody tr");
    expect(rows.length).toBe(2);
  });

  it("isAllChecked is true only when every workflow is checked", () => {
    expect(component.isAllChecked()).toBe(false); // freshly [false, false]
    component.checkedWorkflows = [true, true];
    expect(component.isAllChecked()).toBe(true);
    component.checkedWorkflows = [true, false];
    expect(component.isAllChecked()).toBe(false);
    component.checkedWorkflows = [];
    expect(component.isAllChecked()).toBe(false); // empty is not "all checked"
  });

  it("changeAll checks all when some are unchecked, and clears all when every one is checked", () => {
    // after init: [false, false] -> not all checked -> check all
    component.changeAll();
    expect(component.checkedWorkflows).toEqual([true, true]);
    // now all checked -> clear all
    component.changeAll();
    expect(component.checkedWorkflows).toEqual([false, false]);
  });

  it("submitForm adds only the checked workflows to the project and caches them", () => {
    component.checkedWorkflows = [true, false]; // check only wf1

    component.submitForm();

    expect(userProjectService.addWorkflowToProject).toHaveBeenCalledTimes(1);
    expect(userProjectService.addWorkflowToProject).toHaveBeenCalledWith(PROJECT_ID, 1);

    const cache = (component as unknown as { addedWorkflows: DashboardWorkflow[] }).addedWorkflows;
    expect(cache).toContain(wf1);
    expect(cache).not.toContain(wf2);
  });
});
