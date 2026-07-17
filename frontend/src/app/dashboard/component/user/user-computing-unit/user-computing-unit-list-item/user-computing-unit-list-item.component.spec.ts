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

import { EventEmitter } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { By } from "@angular/platform-browser";
import { NzModalService } from "ng-zorro-antd/modal";
import { of, throwError } from "rxjs";
import type { Mocked } from "vitest";
import { UserComputingUnitListItemComponent } from "./user-computing-unit-list-item.component";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { WorkflowComputingUnitManagingService } from "../../../../../common/service/computing-unit/workflow-computing-unit/workflow-computing-unit-managing.service";
import { ComputingUnitStatusService } from "../../../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../../../../../common/service/computing-unit/computing-unit-status/mock-computing-unit-status.service";
import { ComputingUnitActionsService } from "../../../../../common/service/computing-unit/computing-unit-actions/computing-unit-actions.service";
import { DashboardWorkflowComputingUnit } from "../../../../../common/type/workflow-computing-unit";
import { commonTestProviders } from "../../../../../common/testing/test-utils";

function makeEntry(overrides: Partial<DashboardWorkflowComputingUnit> = {}): DashboardWorkflowComputingUnit {
  return {
    computingUnit: {
      cuid: 1,
      uid: 1,
      name: "unit-1",
      creationTime: 0,
      terminateTime: undefined,
      type: "local",
      uri: "",
      resource: {
        cpuLimit: "1",
        memoryLimit: "1Gi",
        gpuLimit: "0",
        jvmMemorySize: "0",
        shmSize: "0",
        nodeAddresses: [],
      },
    },
    status: "Running",
    metrics: { cpuUsage: "N/A", memoryUsage: "N/A" },
    isOwner: true,
    accessPrivilege: "WRITE",
    ownerGoogleAvatar: "",
    ownerName: "owner",
    ...overrides,
  };
}

describe("UserComputingUnitListItemComponent", () => {
  let fixture: ComponentFixture<UserComputingUnitListItemComponent>;
  let component: UserComputingUnitListItemComponent;
  let computingUnitService: Mocked<WorkflowComputingUnitManagingService>;
  let notificationService: Mocked<NotificationService>;
  let actionsService: Mocked<ComputingUnitActionsService>;
  let statusService: ComputingUnitStatusService;

  beforeEach(async () => {
    const computingUnitServiceSpy = {
      getComputingUnitLimitOptions: vi.fn(),
      renameComputingUnit: vi.fn(),
    } as unknown as Mocked<WorkflowComputingUnitManagingService>;
    computingUnitServiceSpy.getComputingUnitLimitOptions.mockReturnValue(
      of({ cpuLimitOptions: [], memoryLimitOptions: [], gpuLimitOptions: ["0", "1"] })
    );

    const notificationServiceSpy = {
      success: vi.fn(),
      error: vi.fn(),
      info: vi.fn(),
      warning: vi.fn(),
      loading: vi.fn(),
      blank: vi.fn(),
    };
    const actionsServiceSpy = { openShareAccessModal: vi.fn() };

    await TestBed.configureTestingModule({
      imports: [UserComputingUnitListItemComponent, HttpClientTestingModule],
      providers: [
        NzModalService,
        { provide: NotificationService, useValue: notificationServiceSpy },
        { provide: WorkflowComputingUnitManagingService, useValue: computingUnitServiceSpy },
        { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
        { provide: ComputingUnitActionsService, useValue: actionsServiceSpy },
        ...commonTestProviders,
      ],
    }).compileComponents();

    computingUnitService = TestBed.inject(
      WorkflowComputingUnitManagingService
    ) as unknown as Mocked<WorkflowComputingUnitManagingService>;
    notificationService = TestBed.inject(NotificationService) as unknown as Mocked<NotificationService>;
    actionsService = TestBed.inject(ComputingUnitActionsService) as unknown as Mocked<ComputingUnitActionsService>;
    statusService = TestBed.inject(ComputingUnitStatusService);
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UserComputingUnitListItemComponent);
    component = fixture.componentInstance;
    component.entry = makeEntry();
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("ngOnInit", () => {
    it("populates gpuOptions from the service", () => {
      expect(component.gpuOptions).toEqual(["0", "1"]);
    });

    it("notifies the user when the resource options request fails", () => {
      computingUnitService.getComputingUnitLimitOptions.mockReturnValue(throwError(() => new Error("boom")));
      const freshFixture = TestBed.createComponent(UserComputingUnitListItemComponent);
      freshFixture.componentInstance.entry = makeEntry();
      freshFixture.detectChanges();
      expect(notificationService.error).toHaveBeenCalledWith(
        expect.stringContaining("Failed to fetch resource options")
      );
    });
  });

  describe("entry and unit getters", () => {
    function bare(): UserComputingUnitListItemComponent {
      return new UserComputingUnitListItemComponent(
        {} as any,
        {} as any,
        {} as any,
        {} as any,
        {} as any,
        {} as any,
        {} as any
      );
    }

    it("throws when entry is accessed before being set", () => {
      expect(() => bare().entry).toThrowError("entry property must be provided to UserComputingUnitListItemComponent.");
    });

    it("returns the entry once set", () => {
      const entry = makeEntry();
      const item = bare();
      item.entry = entry;
      expect(item.entry).toBe(entry);
    });

    it("exposes the computing unit via the unit getter", () => {
      expect(component.unit).toBe(component.entry.computingUnit);
    });

    it("throws when the entry has no computing unit", () => {
      const item = bare();
      item.entry = {
        ...makeEntry(),
        computingUnit: undefined as unknown as DashboardWorkflowComputingUnit["computingUnit"],
      };
      expect(() => item.unit).toThrowError(
        "Incorrect type of DashboardEntry provided to UserComputingUnitListItemComponent. Entry must be computing unit."
      );
    });
  });

  describe("startEditingUnitName", () => {
    it("rejects renaming for non-owners", () => {
      const entry = makeEntry({ isOwner: false });
      component.startEditingUnitName(entry);
      expect(notificationService.error).toHaveBeenCalledWith("Only owners can rename computing units");
      expect(component.editingNameOfUnit).toBeNull();
    });

    it("enters editing mode for owners", () => {
      const entry = makeEntry();
      component.startEditingUnitName(entry);
      expect(component.editingNameOfUnit).toBe(entry.computingUnit.cuid);
      expect(component.editingUnitName).toBe(entry.computingUnit.name);
    });
  });

  describe("cancelEditingUnitName", () => {
    it("clears the editing state", () => {
      component.editingNameOfUnit = 1;
      component.editingUnitName = "half-typed";
      component.cancelEditingUnitName();
      expect(component.editingNameOfUnit).toBeNull();
      expect(component.editingUnitName).toBe("");
    });
  });

  describe("confirmUpdateUnitName", () => {
    it("rejects an empty name without calling the rename endpoint", () => {
      component.editingNameOfUnit = 1;
      component.confirmUpdateUnitName(1, "   ");
      expect(notificationService.error).toHaveBeenCalledWith("Computing unit name cannot be empty");
      expect(computingUnitService.renameComputingUnit).not.toHaveBeenCalled();
      expect(component.editingNameOfUnit).toBeNull();
    });

    it("renames with the trimmed name, updates the entry, refreshes, and resets editing on success", () => {
      const refreshSpy = vi.spyOn(statusService, "refreshComputingUnitList");
      computingUnitService.renameComputingUnit.mockReturnValue(of({} as Response));
      component.editingNameOfUnit = 1;

      component.confirmUpdateUnitName(1, "  new-name  ");

      expect(computingUnitService.renameComputingUnit).toHaveBeenCalledExactlyOnceWith(1, "new-name");
      expect(notificationService.success).toHaveBeenCalledWith("Successfully renamed computing unit");
      expect(component.entry.computingUnit.name).toBe("new-name");
      expect(refreshSpy).toHaveBeenCalledTimes(1);
      expect(component.editingNameOfUnit).toBeNull();
      expect(component.editingUnitName).toBe("");
    });

    it("does not rewrite the entry name when the cuid does not match", () => {
      computingUnitService.renameComputingUnit.mockReturnValue(of({} as Response));

      component.confirmUpdateUnitName(999, "other-name");

      expect(computingUnitService.renameComputingUnit).toHaveBeenCalledExactlyOnceWith(999, "other-name");
      expect(component.entry.computingUnit.name).toBe("unit-1");
    });

    it("notifies on failure, leaves the name unchanged, and resets editing", () => {
      const refreshSpy = vi.spyOn(statusService, "refreshComputingUnitList");
      computingUnitService.renameComputingUnit.mockReturnValue(throwError(() => new Error("boom")));
      component.editingNameOfUnit = 1;

      component.confirmUpdateUnitName(1, "new-name");

      expect(notificationService.error).toHaveBeenCalledWith(
        expect.stringContaining("Failed to rename computing unit:")
      );
      expect(component.entry.computingUnit.name).toBe("unit-1");
      expect(refreshSpy).not.toHaveBeenCalled();
      expect(component.editingNameOfUnit).toBeNull();
    });
  });

  describe("onClickOpenShareAccess", () => {
    it("delegates to the actions service with inWorkspace = false", async () => {
      await component.onClickOpenShareAccess(42);
      expect(actionsService.openShareAccessModal).toHaveBeenCalledExactlyOnceWith(42, false);
    });
  });

  describe("deleted output", () => {
    it("is an EventEmitter", () => {
      expect(component.deleted).toBeInstanceOf(EventEmitter);
    });

    it("emits when the delete button is clicked", () => {
      const emitted = vi.fn();
      component.deleted.subscribe(emitted);
      fixture.debugElement.query(By.css('button[title="Delete"]')).triggerEventHandler("click", null);
      expect(emitted).toHaveBeenCalledTimes(1);
    });
  });

  describe("resource getters", () => {
    it("reads resource limits straight from the unit", () => {
      expect(component.getGpuLimit()).toBe("0");
      expect(component.getJvmMemorySize()).toBe("0");
      expect(component.getSharedMemorySize()).toBe("0");
      expect(typeof component.getCpuLimit()).toBe("number");
      expect(typeof component.getMemoryLimit()).toBe("number");
    });

    it("returns zero usage values when metrics are unavailable", () => {
      expect(component.getCpuValue()).toBe(0);
      expect(component.getMemoryValue()).toBe(0);
    });

    it("shows GPU selection when more than one option is available", () => {
      expect(component.showGpuSelection()).toBe(true);
    });
  });
});
