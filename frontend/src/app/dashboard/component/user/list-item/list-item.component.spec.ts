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
import { of, Subject, throwError } from "rxjs";
import { ActionType, HubService } from "../../../../hub/service/hub.service";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { RouterTestingModule } from "@angular/router/testing";
import { StubUserService } from "../../../../common/service/user/stub-user.service";
import { UserService } from "../../../../common/service/user/user.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import type { Mocked } from "vitest";
import { DashboardEntry } from "src/app/dashboard/type/dashboard-entry";
import { DatasetService } from "../../../service/user/dataset/dataset.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
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
  let datasetService: Mocked<DatasetService>;
  let hubService: HubService;
  let modalService: NzModalService;

  beforeEach(async () => {
    const workflowPersistServiceSpy = { updateWorkflowName: vi.fn(), updateWorkflowDescription: vi.fn() };
    const datasetServiceSpy = { updateDatasetName: vi.fn() };

    await TestBed.configureTestingModule({
      imports: [ListItemComponent, HttpClientTestingModule, BrowserAnimationsModule, RouterTestingModule],
      providers: [
        { provide: WorkflowPersistService, useValue: workflowPersistServiceSpy },
        { provide: DatasetService, useValue: datasetServiceSpy },
        { provide: UserService, useClass: StubUserService },
        NzModalService,
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(ListItemComponent);
    component = fixture.componentInstance;
    workflowPersistService = TestBed.inject(WorkflowPersistService) as unknown as Mocked<WorkflowPersistService>;
    datasetService = TestBed.inject(DatasetService) as unknown as Mocked<DatasetService>;
    hubService = TestBed.inject(HubService);
    modalService = TestBed.inject(NzModalService);
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

  it("should reject an invalid dataset name, revert to original, and exit editing", () => {
    component.entry = {
      id: 5,
      name: "has space",
      type: "dataset",
    } as unknown as DashboardEntry;
    component.originalName = "original-name";
    component.editingName = true;
    const notificationService = TestBed.inject(NotificationService);
    const errorSpy = vi.spyOn(notificationService, "error");

    component.confirmUpdateCustomName("has space");

    expect(datasetService.updateDatasetName).not.toHaveBeenCalled();
    expect(errorSpy).toHaveBeenCalled();
    expect(component.entry.name).toBe("original-name");
    expect(component.editingName).toBe(false);
  });

  it("should call the dataset service for a valid dataset rename", () => {
    component.entry = {
      id: 5,
      name: "new-valid-name",
      type: "dataset",
    } as unknown as DashboardEntry;
    component.originalName = "old-name";
    datasetService.updateDatasetName.mockReturnValue(of({} as any));

    component.confirmUpdateCustomName("new-valid-name");

    expect(datasetService.updateDatasetName).toHaveBeenCalledWith(5, "new-valid-name");
  });

  it("should surface the error message and revert the name when a dataset rename fails", () => {
    component.entry = {
      id: 5,
      name: "new-valid-name",
      type: "dataset",
    } as unknown as DashboardEntry;
    component.originalName = "old-name";
    component.editingName = true;
    datasetService.updateDatasetName.mockReturnValue(throwError(() => new Error("boom")));
    const notificationService = TestBed.inject(NotificationService);
    const errorSpy = vi.spyOn(notificationService, "error");

    component.confirmUpdateCustomName("new-valid-name");

    expect(errorSpy).toHaveBeenCalledWith("boom");
    expect(component.entry.name).toBe("old-name");
    expect(component.editingName).toBe(false);
  });

  describe("edit + interaction handlers", () => {
    it("onEditName captures the original name and enters edit mode", () => {
      component.entry = { id: 1, name: "My Name", type: "workflow" } as unknown as DashboardEntry;

      component.onEditName();

      expect(component.editingName).toBe(true);
      expect(component.originalName).toBe("My Name");
    });

    it("onEditDescription opens the edit modal and applies the change, then closes it", () => {
      component.editable = true;
      component.entry = { id: 1, description: "old", type: "workflow" } as unknown as DashboardEntry;
      const descriptionChange = new Subject<string>();
      const modalRef = { componentInstance: { descriptionChange }, destroy: vi.fn() };
      const createSpy = vi.spyOn(modalService, "create").mockReturnValue(modalRef as any);
      const confirmSpy = vi.spyOn(component, "confirmUpdateCustomDescription").mockImplementation(() => {});

      component.onEditDescription();
      expect(createSpy).toHaveBeenCalled();

      descriptionChange.next("new description");
      expect(confirmSpy).toHaveBeenCalledWith("new description");
      expect(modalRef.destroy).toHaveBeenCalled();
    });

    it("onEditDescription is a no-op when the entry is not editable", () => {
      component.editable = false;
      const createSpy = vi.spyOn(modalService, "create");

      component.onEditDescription();

      expect(createSpy).not.toHaveBeenCalled();
    });

    it("onCheckboxChange toggles the entry's checked flag and emits the change", () => {
      const entry = { checked: false } as unknown as DashboardEntry;
      const emitSpy = vi.fn();
      component.checkboxChanged.subscribe(emitSpy);

      component.onCheckboxChange(entry);

      expect(entry.checked).toBe(true);
      expect(emitSpy).toHaveBeenCalled();
    });

    it("toggleLike likes the entry and refreshes the like count on success", () => {
      component.currentUid = 1;
      component.entry = { id: 5, type: "workflow" } as unknown as DashboardEntry;
      component.isLiked = false;
      vi.spyOn(hubService, "postLike").mockReturnValue(of(true));
      vi.spyOn(hubService, "getCounts").mockReturnValue(of([{ counts: { like: 3 } }] as any));

      component.toggleLike();

      expect(hubService.postLike).toHaveBeenCalledWith(5, "workflow");
      expect(component.isLiked).toBe(true);
      expect(component.likeCount).toBe(3);
    });

    it("toggleLike unlikes the entry and refreshes the like count on success", () => {
      component.currentUid = 1;
      component.entry = { id: 5, type: "workflow" } as unknown as DashboardEntry;
      component.isLiked = true;
      vi.spyOn(hubService, "postUnlike").mockReturnValue(of(true));
      vi.spyOn(hubService, "getCounts").mockReturnValue(of([{ counts: { like: 1 } }] as any));

      component.toggleLike();

      expect(hubService.postUnlike).toHaveBeenCalledWith(5, "workflow");
      expect(component.isLiked).toBe(false);
      expect(component.likeCount).toBe(1);
    });

    it("toggleLike does nothing when there is no current user", () => {
      component.currentUid = undefined;
      component.entry = { id: 5, type: "workflow" } as unknown as DashboardEntry;
      const postLikeSpy = vi.spyOn(hubService, "postLike");

      component.toggleLike();

      expect(postLikeSpy).not.toHaveBeenCalled();
    });

    it("openDetailModal opens the detail modal and increments the view count", () => {
      component.entry = { id: 9, type: "workflow" } as unknown as DashboardEntry;
      const modalRef = { componentInstance: {}, destroy: vi.fn() };
      vi.spyOn(modalService, "create").mockReturnValue(modalRef as any);
      vi.spyOn(hubService, "getCounts").mockReturnValue(of([{ counts: { view: 4 } }] as any));

      component.openDetailModal(9);

      expect(modalService.create).toHaveBeenCalled();
      expect(hubService.getCounts).toHaveBeenCalledWith(["workflow"], [9], [ActionType.View]);
      expect(component.viewCount).toBe(5); // 4 + 1
    });
  });
});
