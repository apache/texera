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
import { NzPopoverDirective } from "ng-zorro-antd/popover";
import { of, throwError } from "rxjs";
import type { Mocked } from "vitest";
import { UserComputingUnitListItemComponent } from "./user-computing-unit-list-item.component";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { WorkflowComputingUnitManagingService } from "../../../../../common/service/computing-unit/workflow-computing-unit/workflow-computing-unit-managing.service";
import { ComputingUnitStatusService } from "../../../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../../../../../common/service/computing-unit/computing-unit-status/mock-computing-unit-status.service";
import { ComputingUnitActionsService } from "../../../../../common/service/computing-unit/computing-unit-actions/computing-unit-actions.service";
import { DashboardWorkflowComputingUnit } from "../../../../../common/type/workflow-computing-unit";
import { GuiConfigService } from "../../../../../common/service/gui-config.service";
import { commonTestProviders } from "../../../../../common/testing/test-utils";
import { ComputingUnitMetadataComponent } from "../../../../../common/util/computing-unit.util";

function bareComponent(entry: DashboardWorkflowComputingUnit): UserComputingUnitListItemComponent {
  const item = new UserComputingUnitListItemComponent(
    {} as any,
    {} as any,
    {} as any,
    {} as any,
    {} as any,
    {} as any,
    {} as any
  );
  item.entry = entry;
  return item;
}

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

  afterEach(() => {
    vi.useRealTimers();
    fixture?.destroy();
    vi.restoreAllMocks();
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

  describe("ngOnInit gpuOptions fallback", () => {
    it("defaults gpuOptions to an empty array when the service omits gpuLimitOptions", () => {
      computingUnitService.getComputingUnitLimitOptions.mockReturnValue(
        of({ cpuLimitOptions: [], memoryLimitOptions: [], gpuLimitOptions: undefined as unknown as string[] })
      );
      const freshFixture = TestBed.createComponent(UserComputingUnitListItemComponent);
      freshFixture.componentInstance.entry = makeEntry();
      freshFixture.detectChanges();
      expect(freshFixture.componentInstance.gpuOptions).toEqual([]);
      freshFixture.destroy();
    });
  });

  describe("startEditingUnitName focus behavior", () => {
    it("focuses and selects the rendered input after the timeout fires", () => {
      const host = fixture.nativeElement as HTMLElement;
      document.body.appendChild(host);
      const focusSpy = vi.spyOn(HTMLInputElement.prototype, "focus").mockImplementation(() => {});
      const selectSpy = vi.spyOn(HTMLInputElement.prototype, "select").mockImplementation(() => {});
      vi.useFakeTimers();
      try {
        component.startEditingUnitName(makeEntry());
        // The editable input is rendered synchronously via cdr.detectChanges().
        // Scope the query to this fixture's host so it can't match another fixture's input.
        const input = (fixture.nativeElement as HTMLElement).querySelector(".unit-name-edit-input");
        expect(input).toBeTruthy();
        // The focus/select happen inside a setTimeout(0) callback.
        expect(focusSpy).not.toHaveBeenCalled();
        vi.advanceTimersByTime(1);
        expect(focusSpy).toHaveBeenCalledTimes(1);
        expect(selectSpy).toHaveBeenCalledTimes(1);
      } finally {
        host.remove();
      }
    });
  });

  describe("openComputingUnitMetadataModal", () => {
    it("opens the metadata modal with the expected configuration", () => {
      const modalService = TestBed.inject(NzModalService);
      const createSpy = vi.spyOn(modalService, "create").mockReturnValue({} as any);
      const entry = makeEntry();

      component.openComputingUnitMetadataModal(entry);

      expect(createSpy).toHaveBeenCalledTimes(1);
      expect(createSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          nzTitle: "Computing Unit Information",
          nzContent: ComputingUnitMetadataComponent,
          nzData: entry,
          nzFooter: null,
          nzMaskClosable: true,
          nzWidth: "600px",
        })
      );
    });
  });

  describe("status and label computations", () => {
    it("delegates badge color to the status util", () => {
      expect(component.getBadgeColor("Running")).toBe("green");
      expect(component.getBadgeColor("Pending")).toBe("gold");
      expect(component.getBadgeColor("Terminated")).toBe("red");
    });

    it("delegates the status tooltip to the status util", () => {
      expect(component.getUnitStatusTooltip(makeEntry({ status: "Running" }))).toBe("Ready to use");
      expect(component.getUnitStatusTooltip(makeEntry({ status: "Pending" }))).toBe("Computing unit is starting up");
      const terminated = {
        ...makeEntry(),
        status: "Terminated" as unknown as DashboardWorkflowComputingUnit["status"],
      };
      expect(component.getUnitStatusTooltip(terminated)).toBe("Terminated");
    });

    it("computes CPU/memory percentages and maps them to progress statuses under heavy load", () => {
      component.entry = makeEntry({
        metrics: { cpuUsage: "950m", memoryUsage: "950Mi" },
      });
      expect(component.getCpuPercentage()).toBeCloseTo(95, 1);
      expect(component.getMemoryPercentage()).toBeCloseTo(92.77, 1);
      expect(component.getCpuStatus()).toBe("exception");
      expect(component.getMemoryStatus()).toBe("exception");
    });

    it("reports 0% and success statuses for the default entry's N/A (unavailable) metrics", () => {
      expect(component.getCpuPercentage()).toBe(0);
      expect(component.getMemoryPercentage()).toBe(0);
      expect(component.getCpuStatus()).toBe("success");
      expect(component.getMemoryStatus()).toBe("success");
    });
  });

  describe("resource limit units", () => {
    it("returns CPU for a unitless core limit and passes through the raw unit otherwise", () => {
      const coreEntry = bareComponent(makeEntry());
      expect(coreEntry.getCpuLimitUnit()).toBe("CPU");

      const milliEntry = bareComponent(
        makeEntry({
          computingUnit: {
            ...makeEntry().computingUnit,
            resource: { ...makeEntry().computingUnit.resource, cpuLimit: "2000m" },
          },
        })
      );
      expect(milliEntry.getCpuLimitUnit()).toBe("m");
    });

    it("returns the memory limit unit parsed from the limit string", () => {
      expect(bareComponent(makeEntry()).getMemoryLimitUnit()).toBe("Gi");
    });
  });

  describe("getCpuValue / getMemoryValue with real usage", () => {
    function entryWith(cpuLimit: string, memoryLimit: string, cpuUsage: string, memoryUsage: string) {
      const base = makeEntry();
      return makeEntry({
        metrics: { cpuUsage, memoryUsage },
        computingUnit: {
          ...base.computingUnit,
          resource: { ...base.computingUnit.resource, cpuLimit, memoryLimit },
        },
      });
    }

    it("converts CPU usage to cores when the limit is expressed in cores", () => {
      component.entry = entryWith("1", "1Gi", "500m", "512Mi");
      expect(component.getCpuValue()).toBeCloseTo(0.5, 4);
      expect(component.getMemoryValue()).toBeCloseTo(0.5, 4);
    });

    it("converts CPU usage into the limit's unit when the limit carries a suffix", () => {
      component.entry = entryWith("2000m", "1Gi", "500m", "512Mi");
      // limit unit is "m", so usage is reported in millicores rather than cores
      expect(component.getCpuValue()).toBeCloseTo(500, 2);
    });
  });

  describe("fallback getters when metrics/resource are absent", () => {
    it("returns N/A for every usage and limit getter", () => {
      const base = makeEntry();
      const item = bareComponent(
        makeEntry({
          metrics: undefined,
          computingUnit: {
            ...base.computingUnit,
            resource: undefined as unknown as DashboardWorkflowComputingUnit["computingUnit"]["resource"],
          },
        })
      );

      expect(item.getCurrentComputingUnitCpuUsage()).toBe("N/A");
      expect(item.getCurrentComputingUnitMemoryUsage()).toBe("N/A");
      expect(item.getCurrentComputingUnitCpuLimit()).toBe("N/A");
      expect(item.getCurrentComputingUnitMemoryLimit()).toBe("N/A");
      expect(item.getCurrentComputingUnitGpuLimit()).toBe("N/A");
      expect(item.getCurrentComputingUnitJvmMemorySize()).toBe("N/A");
      expect(item.getCurrentSharedMemorySize()).toBe("N/A");
    });
  });

  describe("showGpuSelection edge cases", () => {
    it("hides the selection when the only option is the zero option", () => {
      component.gpuOptions = ["0"];
      expect(component.showGpuSelection()).toBe(false);
    });

    it("hides the selection when there are no options", () => {
      component.gpuOptions = [];
      expect(component.showGpuSelection()).toBe(false);
    });

    it("shows the selection when the single option is a non-zero value", () => {
      component.gpuOptions = ["2"];
      expect(component.showGpuSelection()).toBe(true);
    });
  });

  // ── Rendered-template interactions (the row's bindings) ──

  describe("template interactions", () => {
    /** The handlers that call `$event.stopPropagation()` need a real-ish event. */
    const clickEvent = () => ({ stopPropagation: vi.fn() }) as unknown as MouseEvent;

    /** Show the metrics popover synchronously and return its overlay text. */
    function openMetricsPopover(): { popover: NzPopoverDirective; text: string } {
      const popover = fixture.debugElement.query(By.css(".metrics-container")).injector.get(NzPopoverDirective);
      popover.show();
      fixture.detectChanges();
      return { popover, text: document.querySelector(".cdk-overlay-container")?.textContent ?? "" };
    }

    afterEach(() => {
      // Clear the overlay contents rather than removing the container element itself:
      // CDK's OverlayContainer caches that element, so removing it would make later
      // overlays render into a detached node.
      document.querySelectorAll(".cdk-overlay-container").forEach(el => (el.innerHTML = ""));
    });

    it("the rename button starts inline editing", () => {
      const renameButton = fixture.debugElement.query(By.css(".edit-button button"));
      expect(renameButton).toBeTruthy();

      renameButton.triggerEventHandler("click", clickEvent());

      expect(component.editingNameOfUnit).toBe(1);
      expect(component.editingUnitName).toBe("unit-1");
    });

    it("clicking the unit name opens the metadata modal", () => {
      const createSpy = vi
        .spyOn(TestBed.inject(NzModalService), "create")
        .mockReturnValue({} as ReturnType<NzModalService["create"]>);

      fixture.debugElement.query(By.css(".resource-name")).triggerEventHandler("click", null);

      expect(createSpy).toHaveBeenCalledWith(expect.objectContaining({ nzData: component.entry }));
    });

    it("escape on the rename input cancels editing", () => {
      component.editingNameOfUnit = 1;
      fixture.detectChanges();

      const input = fixture.debugElement.query(By.css("input.unit-name-edit-input"));
      expect(input).toBeTruthy();
      input.nativeElement.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));

      expect(component.editingNameOfUnit).toBeNull();
    });

    it("enter on the rename input confirms with the typed value", () => {
      computingUnitService.renameComputingUnit.mockReturnValue(of({} as Response));
      component.editingNameOfUnit = 1;
      fixture.detectChanges();

      const input = fixture.debugElement.query(By.css("input.unit-name-edit-input"));
      input.nativeElement.value = "  renamed  ";
      input.nativeElement.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));

      expect(computingUnitService.renameComputingUnit).toHaveBeenCalledExactlyOnceWith(1, "renamed");
    });

    it("a click inside the rename input does not bubble to the row", () => {
      component.editingNameOfUnit = 1;
      fixture.detectChanges();
      const stopPropagation = vi.fn();

      fixture.debugElement
        .query(By.css("input.unit-name-edit-input"))
        .triggerEventHandler("click", { stopPropagation } as unknown as MouseEvent);

      expect(stopPropagation).toHaveBeenCalledTimes(1);
    });

    it("the delete button emits the deleted output", () => {
      const deletedSpy = vi.fn();
      component.deleted.subscribe(deletedSpy);
      const deleteButton = fixture.debugElement.query(By.css(".button-group button[title='Delete']"));
      expect(deleteButton).toBeTruthy();

      deleteButton.triggerEventHandler("click", null);

      expect(deletedSpy).toHaveBeenCalledTimes(1);
    });

    it("omits the share button while sharing is disabled", () => {
      expect(fixture.debugElement.query(By.css("button[aria-label='Share computing unit']"))).toBeNull();
    });

    it("the share button opens the share-access modal when sharing is enabled", () => {
      // same DI instance the component holds
      TestBed.inject(GuiConfigService).env.sharingComputingUnitEnabled = true;
      fixture.detectChanges();

      const shareButton = fixture.debugElement.query(By.css("button[aria-label='Share computing unit']"));
      expect(shareButton).toBeTruthy();
      shareButton.triggerEventHandler("click", clickEvent());

      expect(actionsService.openShareAccessModal).toHaveBeenCalledWith(1, false);
    });

    it("the metrics popover renders the CPU and RAM rows", () => {
      const { popover, text } = openMetricsPopover();

      expect(text).toContain("CPU");
      expect(text).toContain("RAM");
      // the default entry has no GPU / JVM / shared-memory limits
      expect(text).not.toContain("GPU(s)");
      expect(text).not.toContain("JVM Memory Size");
      expect(text).not.toContain("Shared Memory Size");

      popover.hide();
      fixture.detectChanges();
    });

    it("the metrics popover adds the GPU, JVM and shared-memory rows when those limits are set", () => {
      const base = makeEntry();
      component.entry = makeEntry({
        computingUnit: {
          ...base.computingUnit,
          resource: { ...base.computingUnit.resource, gpuLimit: "2", jvmMemorySize: "2Gi", shmSize: "1Gi" },
        },
      });
      fixture.detectChanges();

      const { popover, text } = openMetricsPopover();

      expect(text).toContain("2 GPU(s)");
      expect(text).toContain("JVM Memory Size");
      expect(text).toContain("Shared Memory Size");

      popover.hide();
      fixture.detectChanges();
    });
  });
});
