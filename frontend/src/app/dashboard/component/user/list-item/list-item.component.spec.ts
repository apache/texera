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
import { ListItemComponent } from "./list-item.component";
import { WorkflowPersistService } from "src/app/common/service/workflow-persist/workflow-persist.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzModalService } from "ng-zorro-antd/modal";
import { of, throwError } from "rxjs";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { RouterTestingModule } from "@angular/router/testing";
import { StubUserService } from "../../../../common/service/user/stub-user.service";
import { UserService } from "../../../../common/service/user/user.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import type { Mocked } from "vitest";
import { DashboardEntry } from "src/app/dashboard/type/dashboard-entry";
import {
  HUB_DATASET_RESULT_DETAIL,
  HUB_WORKFLOW_RESULT_DETAIL,
  USER_DATASET,
  USER_PROJECT,
  USER_WORKSPACE,
} from "../../../../app-routing.constant";

describe("ListItemComponent", () => {
  let component: ListItemComponent;
  let fixture: ComponentFixture<ListItemComponent>;
  let workflowPersistService: Mocked<WorkflowPersistService>;

  beforeEach(async () => {
    const workflowPersistServiceSpy = { updateWorkflowName: vi.fn(), updateWorkflowDescription: vi.fn() };

    await TestBed.configureTestingModule({
      imports: [ListItemComponent, HttpClientTestingModule, BrowserAnimationsModule, RouterTestingModule],
      providers: [
        { provide: WorkflowPersistService, useValue: workflowPersistServiceSpy },
        { provide: UserService, useClass: StubUserService },
        NzModalService,
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(ListItemComponent);
    component = fixture.componentInstance;
    workflowPersistService = TestBed.inject(WorkflowPersistService) as unknown as Mocked<WorkflowPersistService>;
    // initializeEntry() needs a fully-formed workflow entry to avoid throwing
    // when the template renders for the first time. Each test below overwrites
    // component.entry directly, which exercises confirm methods without going
    // back through change detection.
    component.entry = {
      id: 0,
      name: "default",
      description: "",
      type: "workflow",
      workflow: { isOwner: true },
      accessibleUserIds: [],
      likeCount: 0,
      viewCount: 0,
      isLiked: false,
      size: 0,
    } as unknown as DashboardEntry;
    fixture.detectChanges();
  });

  it("should update workflow name successfully", () => {
    const newName = "New Workflow Name";
    component.entry = { id: 1, name: "Old Name", type: "workflow" } as unknown as DashboardEntry;
    workflowPersistService.updateWorkflowName.mockReturnValue(of({} as Response));

    component.confirmUpdateCustomName(newName);

    expect(workflowPersistService.updateWorkflowName).toHaveBeenCalledWith(1, newName);
    expect(component.entry.name).toBe(newName);
    expect(component.editingName).toBe(false);
  });

  it("should handle error when updating workflow name", () => {
    const newName = "New Workflow Name";
    component.entry = { id: 1, name: "Old Name", type: "workflow" } as unknown as DashboardEntry;
    component.originalName = "Old Name";
    workflowPersistService.updateWorkflowName.mockReturnValue(throwError(() => new Error("Error")));

    component.confirmUpdateCustomName(newName);

    expect(workflowPersistService.updateWorkflowName).toHaveBeenCalledWith(1, newName);
    expect(component.entry.name).toBe("Old Name");
    expect(component.editingName).toBe(false);
  });

  it("should update workflow description successfully", () => {
    const newDescription = "New Description";
    component.entry = { id: 1, description: "Old Description", type: "workflow" } as unknown as DashboardEntry;
    workflowPersistService.updateWorkflowDescription.mockReturnValue(of({} as Response));

    component.confirmUpdateCustomDescription(newDescription);

    expect(workflowPersistService.updateWorkflowDescription).toHaveBeenCalledWith(1, newDescription);
    expect(component.entry.description).toBe(newDescription);
    expect(component.editingDescription).toBe(false);
  });

  it("should handle error when updating workflow description", () => {
    const newDescription = "New Description";
    component.entry = { id: 1, description: "Old Description", type: "workflow" } as unknown as DashboardEntry;
    component.originalDescription = "Old Description";
    workflowPersistService.updateWorkflowDescription.mockReturnValue(throwError(() => new Error("Error")));

    component.confirmUpdateCustomDescription(newDescription);

    expect(workflowPersistService.updateWorkflowDescription).toHaveBeenCalledWith(1, newDescription);
    expect(component.entry.description).toBe("Old Description");
    expect(component.editingDescription).toBe(false);
  });

  describe("initializeEntry routes", () => {
    const baseStats = { likeCount: 0, viewCount: 0, isLiked: false };

    it("routes owned workflows to the user workspace", () => {
      component.currentUid = 1;
      component.entry = {
        id: 100,
        type: "workflow",
        workflow: { isOwner: true },
        accessibleUserIds: [1],
        ...baseStats,
      } as unknown as DashboardEntry;
      component.initializeEntry();
      expect(component.entryLink).toEqual([USER_WORKSPACE, "100"]);
    });

    it("routes non-owned workflows to the hub workflow detail page", () => {
      component.currentUid = 1;
      component.entry = {
        id: 101,
        type: "workflow",
        workflow: { isOwner: false },
        accessibleUserIds: [2],
        ...baseStats,
      } as unknown as DashboardEntry;
      component.initializeEntry();
      expect(component.entryLink).toEqual([HUB_WORKFLOW_RESULT_DETAIL, "101"]);
    });

    it("routes projects to the user project page", () => {
      component.entry = { id: 200, type: "project", ...baseStats } as unknown as DashboardEntry;
      component.initializeEntry();
      expect(component.entryLink).toEqual([USER_PROJECT, "200"]);
    });

    it("routes owned datasets to the user dataset page", () => {
      component.currentUid = 1;
      component.entry = {
        id: 300,
        type: "dataset",
        dataset: { isOwner: true },
        accessibleUserIds: [1],
        ...baseStats,
      } as unknown as DashboardEntry;
      component.initializeEntry();
      expect(component.entryLink).toEqual([USER_DATASET, "300"]);
    });

    it("routes non-owned datasets to the hub dataset detail page", () => {
      component.currentUid = 1;
      component.entry = {
        id: 301,
        type: "dataset",
        dataset: { isOwner: false },
        accessibleUserIds: [2],
        ...baseStats,
      } as unknown as DashboardEntry;
      component.initializeEntry();
      expect(component.entryLink).toEqual([HUB_DATASET_RESULT_DETAIL, "301"]);
    });
  });
});
