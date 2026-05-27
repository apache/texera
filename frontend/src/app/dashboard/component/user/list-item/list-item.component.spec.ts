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
import { NO_ERRORS_SCHEMA } from "@angular/core";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { RouterTestingModule } from "@angular/router/testing";
import { StubUserService } from "../../../../common/service/user/stub-user.service";
import { UserService } from "../../../../common/service/user/user.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import type { Mocked } from "vitest";
import { DashboardEntry } from "src/app/dashboard/type/dashboard-entry";
import { DriveService } from "../../../service/user/google-drive/drive.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { DatasetService } from "../../../service/user/dataset/dataset.service";

describe("ListItemComponent", () => {
  let component: ListItemComponent;
  let fixture: ComponentFixture<ListItemComponent>;
  let workflowPersistService: Mocked<WorkflowPersistService>;
  let driveServiceMock: Mocked<DriveService>;
  let notificationServiceMock: { success: ReturnType<typeof vi.fn>; error: ReturnType<typeof vi.fn> };
  let connected$: Subject<void>;

  beforeEach(async () => {
    connected$ = new Subject<void>();

    const workflowPersistServiceSpy = {
      updateWorkflowName: vi.fn(),
      updateWorkflowDescription: vi.fn(),
      retrieveWorkflow: vi.fn().mockReturnValue(of({ content: { operators: [] } })),
    };

    driveServiceMock = {
      getToken: vi.fn().mockReturnValue(of({ status: "ok", accessToken: "token" })),
      onConnected: vi.fn().mockReturnValue(connected$.asObservable()),
      connect: vi.fn(),
      exportToDrive: vi.fn().mockReturnValue(of(undefined)),
    } as unknown as Mocked<DriveService>;

    notificationServiceMock = { success: vi.fn(), error: vi.fn() };

    await TestBed.configureTestingModule({
      imports: [ListItemComponent, HttpClientTestingModule, BrowserAnimationsModule, RouterTestingModule],
      providers: [
        { provide: WorkflowPersistService, useValue: workflowPersistServiceSpy },
        { provide: UserService, useClass: StubUserService },
        { provide: DriveService, useValue: driveServiceMock },
        { provide: NotificationService, useValue: notificationServiceMock },
        { provide: DatasetService, useValue: { retrieveDatasetVersionZip: vi.fn().mockReturnValue(of(new Blob())) } },
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

  describe("Drive integration", () => {
    it("should set isDriveConnected = true when token status is ok", () => {
      driveServiceMock.getToken.mockReturnValue(of({ status: "ok", accessToken: "token" }));
      component.ngOnInit();
      expect(component.isDriveConnected).toBe(true);
    });

    it("should set isDriveConnected = false when token status is not ok", () => {
      driveServiceMock.getToken.mockReturnValue(of({ status: "error", accessToken: "" }));
      component.ngOnInit();
      expect(component.isDriveConnected).toBe(false);
    });

    it("should set isDriveConnected = true and show toast when onConnected fires", () => {
      driveServiceMock.getToken.mockReturnValue(of({ status: "error", accessToken: "" }));
      component.ngOnInit();
      expect(component.isDriveConnected).toBe(false);

      connected$.next();

      expect(component.isDriveConnected).toBe(true);
      expect(notificationServiceMock.success).toHaveBeenCalledWith("Google Drive connected");
    });

    it("should call connect() when not connected and onClickDriveAction is called", () => {
      component.isDriveConnected = false;
      component.entry = { id: 1, name: "My Workflow", type: "workflow" } as unknown as DashboardEntry;

      component.onClickDriveAction();

      expect(driveServiceMock.connect).toHaveBeenCalled();
      expect(driveServiceMock.exportToDrive).not.toHaveBeenCalled();
    });

    it("should export workflow to Drive and show success toast", () => {
      component.isDriveConnected = true;
      component.entry = { id: 1, name: "My Workflow", type: "workflow" } as unknown as DashboardEntry;

      component.onClickDriveAction();

      expect(workflowPersistService.retrieveWorkflow).toHaveBeenCalledWith(1);
      expect(driveServiceMock.exportToDrive).toHaveBeenCalled();
      expect(notificationServiceMock.success).toHaveBeenCalledWith("Exported to Google Drive");
    });

    it("should export dataset to Drive and show success toast", () => {
      component.isDriveConnected = true;
      component.entry = { id: 2, name: "My Dataset", type: "dataset" } as unknown as DashboardEntry;

      component.onClickDriveAction();

      expect(driveServiceMock.exportToDrive).toHaveBeenCalledWith(expect.any(Blob), "My Dataset.zip");
      expect(notificationServiceMock.success).toHaveBeenCalledWith("Exported to Google Drive");
    });
  });
});
