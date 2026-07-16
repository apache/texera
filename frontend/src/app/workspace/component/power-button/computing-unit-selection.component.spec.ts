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
import { By } from "@angular/platform-browser";
import { ComputingUnitSelectionComponent } from "./computing-unit-selection.component";
import { NzButtonModule } from "ng-zorro-antd/button";
import { CommonModule } from "@angular/common";
import { NzIconModule } from "ng-zorro-antd/icon";
import { ActivatedRoute, ActivatedRouteSnapshot, convertToParamMap, Data, Params, UrlSegment } from "@angular/router";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { NzModalModule, NzModalService } from "ng-zorro-antd/modal";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { WorkflowComputingUnitManagingService } from "../../../common/service/computing-unit/workflow-computing-unit/workflow-computing-unit-managing.service";
import { MockComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/mock-computing-unit-status.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { UserService } from "../../../common/service/user/user.service";
import { StubUserService } from "../../../common/service/user/stub-user.service";
import { UserPveRecord } from "../../service/virtual-environment/virtual-environment.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { Subject, of, throwError } from "rxjs";
import {
  PackageResponse,
  PvePackageResponse,
  WorkflowPveService,
} from "../../service/virtual-environment/virtual-environment.service";
import {
  DashboardWorkflowComputingUnit,
  WorkflowComputingUnitType,
} from "../../../common/type/workflow-computing-unit";
import { ComputingUnitCreateModalComponent } from "../../../common/component/computing-unit-create-modal/computing-unit-create-modal.component";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { DEFAULT_WORKFLOW, WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { WorkflowExecutionsService } from "../../../dashboard/service/user/workflow-executions/workflow-executions.service";
import { WorkflowExecutionsEntry } from "../../../dashboard/type/workflow-executions-entry";
import { WorkflowMetadata } from "../../../dashboard/type/workflow-metadata.interface";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { ComputingUnitActionsService } from "../../../common/service/computing-unit/computing-unit-actions/computing-unit-actions.service";

/**
 * Builds a fully-populated DashboardWorkflowComputingUnit for driving the
 * status/selection code paths. `status` is widened to string so tests can pin
 * runtime-only states (e.g. "Terminating", "Failed") the template branches on.
 */
function makeComputingUnit(
  overrides: Partial<{
    cuid: number;
    name: string;
    uri: string;
    type: WorkflowComputingUnitType;
    status: string;
    isOwner: boolean;
  }> = {}
): DashboardWorkflowComputingUnit {
  const {
    cuid = 1,
    name = "unit",
    uri = `uri-${cuid}`,
    type = "kubernetes",
    status = "Running",
    isOwner = true,
  } = overrides;
  return {
    computingUnit: {
      cuid,
      uid: 1,
      name,
      creationTime: 0,
      terminateTime: undefined,
      type,
      uri,
      resource: {
        cpuLimit: "1",
        memoryLimit: "1Gi",
        gpuLimit: "0",
        jvmMemorySize: "1Gi",
        shmSize: "64Mi",
        nodeAddresses: [],
      },
    },
    status: status as DashboardWorkflowComputingUnit["status"],
    metrics: { cpuUsage: "N/A", memoryUsage: "N/A" },
    isOwner,
    accessPrivilege: "WRITE",
    ownerGoogleAvatar: "",
    ownerName: "owner",
  };
}

describe("PowerButtonComponent", () => {
  let component: ComputingUnitSelectionComponent;
  let fixture: ComponentFixture<ComputingUnitSelectionComponent>;

  let activatedRouteMock: Partial<ActivatedRoute>;
  const activatedRouteSnapshotMock: Partial<ActivatedRouteSnapshot> = {
    queryParams: {},
    url: [] as UrlSegment[],
    params: {} as Params,
    fragment: null,
    data: {} as Data,
    paramMap: convertToParamMap({}),
    queryParamMap: convertToParamMap({}),
    outlet: "",
    routeConfig: null,
    root: null as any,
    parent: null as any,
    firstChild: null as any,
    children: [],
    pathFromRoot: [],
  };
  activatedRouteMock = {
    snapshot: activatedRouteSnapshotMock as ActivatedRouteSnapshot,
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        ComputingUnitSelectionComponent,
        HttpClientTestingModule, // Use TestingModule instead of HttpClientModule
        CommonModule,
        NzButtonModule,
        NzIconModule,
        NzDropDownModule,
        NzModalModule, // Add NzModalModule for the NzModalService
        NoopAnimationsModule, // disable animations so overlay/modal content renders synchronously
      ],
      providers: [
        { provide: ActivatedRoute, useValue: activatedRouteMock },
        { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
        { provide: UserService, useClass: StubUserService },
        NzModalService, // Add NzModalService provider
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(ComputingUnitSelectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("showAddComputeUnitModalVisible", () => {
    it("forwards the seeded default name to the embedded create modal", () => {
      fixture.detectChanges();
      component.showAddComputeUnitModalVisible("wf's Computing Unit");
      const modal = fixture.debugElement.query(By.directive(ComputingUnitCreateModalComponent)).componentInstance;
      expect(modal.newComputingUnitName).toBe("wf's Computing Unit");
      expect(component.addComputeUnitModalVisible).toBe(true);
    });

    it("opens the modal without touching the name when no default is given", () => {
      fixture.detectChanges();
      const modal = fixture.debugElement.query(By.directive(ComputingUnitCreateModalComponent)).componentInstance;
      const nameBefore = modal.newComputingUnitName;
      component.showAddComputeUnitModalVisible();
      expect(component.addComputeUnitModalVisible).toBe(true);
      expect(modal.newComputingUnitName).toBe(nameBefore);
    });

    it("still opens when the embedded modal is not available", () => {
      fixture.detectChanges();
      // simulate the ViewChild not (yet) being resolved
      (component as any).computingUnitCreateModal = undefined;
      component.showAddComputeUnitModalVisible("Seed Name");
      expect(component.addComputeUnitModalVisible).toBe(true);
    });

    it("syncs visibility when the embedded modal closes itself", () => {
      fixture.detectChanges();
      component.showAddComputeUnitModalVisible();
      expect(component.addComputeUnitModalVisible).toBe(true);
      const modal = fixture.debugElement.query(By.directive(ComputingUnitCreateModalComponent)).componentInstance;
      modal.visibleChange.emit(false);
      expect(component.addComputeUnitModalVisible).toBe(false);
    });
  });

  describe("onComputingUnitCreated", () => {
    it("selects the created unit for the current workflow when the modal emits unitCreated", () => {
      fixture.detectChanges();
      component.workflowId = 7;
      const selectSpy = vi.spyOn(component, "selectComputingUnit").mockImplementation(() => {});
      const modal = fixture.debugElement.query(By.directive(ComputingUnitCreateModalComponent)).componentInstance;
      modal.unitCreated.emit({ computingUnit: { cuid: 42 } } as unknown as DashboardWorkflowComputingUnit);
      expect(selectSpy).toHaveBeenCalledWith(7, 42);
    });
  });

  describe("showGpuSelection", () => {
    it("reflects the fetched GPU limit options", () => {
      const managingService = TestBed.inject(WorkflowComputingUnitManagingService);
      vi.spyOn(managingService, "getComputingUnitLimitOptions").mockReturnValue(
        of({ cpuLimitOptions: [], memoryLimitOptions: [], gpuLimitOptions: ["0", "1"] })
      );
      const gpuFixture = TestBed.createComponent(ComputingUnitSelectionComponent);
      gpuFixture.detectChanges();
      expect(gpuFixture.componentInstance.showGpuSelection()).toBe(true);
    });

    it("hides the GPU row when the deployment reports no GPU support", () => {
      const managingService = TestBed.inject(WorkflowComputingUnitManagingService);
      vi.spyOn(managingService, "getComputingUnitLimitOptions").mockReturnValue(
        of({ cpuLimitOptions: [], memoryLimitOptions: [], gpuLimitOptions: ["0"] })
      );
      const gpuFixture = TestBed.createComponent(ComputingUnitSelectionComponent);
      gpuFixture.detectChanges();
      expect(gpuFixture.componentInstance.showGpuSelection()).toBe(false);
    });

    it("falls back to showing GPU metrics when the limit-options fetch fails", () => {
      const managingService = TestBed.inject(WorkflowComputingUnitManagingService);
      vi.spyOn(managingService, "getComputingUnitLimitOptions").mockReturnValue(throwError(() => new Error("boom")));
      const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
      const failedFixture = TestBed.createComponent(ComputingUnitSelectionComponent);
      failedFixture.detectChanges();
      expect(failedFixture.componentInstance.showGpuSelection()).toBe(true);
      expect(errorSpy).toHaveBeenCalled();
      errorSpy.mockRestore();
    });
  });

  describe("isSavedPveInstalledInCu", () => {
    it("returns true when a locked card with the same trimmed name exists", () => {
      component.pves = [
        { name: "  scanpyenv ", isLocked: true, userPackages: [] } as any,
        { name: "other", isLocked: false, userPackages: [] } as any,
      ];
      expect(component.isSavedPveInstalledInCu("scanpyenv")).toBe(true);
    });

    it("returns false when the same-name card is not locked (draft only)", () => {
      component.pves = [{ name: "scanpyenv", isLocked: false, userPackages: [] } as any];
      expect(component.isSavedPveInstalledInCu("scanpyenv")).toBe(false);
    });

    it("returns false when no card matches", () => {
      component.pves = [{ name: "other", isLocked: true, userPackages: [] } as any];
      expect(component.isSavedPveInstalledInCu("scanpyenv")).toBe(false);
    });
  });

  describe("installFromSavedPve", () => {
    const SAVED_VEID = 42;

    function makeSaved(packages: Record<string, string>, name = "scanpyenv"): UserPveRecord {
      return { veid: SAVED_VEID, name, packages };
    }

    beforeEach(() => {
      // Avoid triggering the real create/install pipeline; we only care about
      // routing + the state the method writes onto the cards.
      vi.spyOn(component, "createVirtualEnvironment").mockImplementation(() => {});
      vi.useFakeTimers();
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it("pushes a new unlocked card and schedules create when no matching locked card exists", () => {
      component.pves = [];
      component.availableDbPves = [makeSaved({ pandas: "==2.0.0" })];

      component.installFromSavedPve(SAVED_VEID);

      expect(component.pves.length).toBe(1);
      const card = component.pves[0];
      expect(card.isLocked).toBe(false);
      expect(card.name).toBe("scanpyenv");
      expect(card.newPackages).toEqual([{ name: "pandas", versionOp: "==", version: "2.0.0" }]);
      expect(card.deletingPackages).toEqual([]);

      vi.runAllTimers();
      expect(component.createVirtualEnvironment).toHaveBeenCalledWith(0);
    });

    it("routes to the locked card and installs only packages that are new in the DB", () => {
      component.pves = [
        {
          name: "scanpyenv",
          isLocked: true,
          userPackages: [{ name: "numpy", versionOp: "==", version: "1.26.0" }],
          newPackages: [],
          deletingPackages: [],
          pipOutput: "",
          prettyPipOutput: "",
          expanded: false,
          isInstalling: false,
        } as any,
      ];
      component.availableDbPves = [makeSaved({ numpy: "==1.26.0", pandas: "==2.0.0" })];

      component.installFromSavedPve(SAVED_VEID);

      const locked = component.pves[0];
      expect(locked.newPackages).toEqual([{ name: "pandas", versionOp: "==", version: "2.0.0" }]);
      expect(locked.deletingPackages).toEqual([]);
      expect(locked.userPackages).toEqual([{ name: "numpy", versionOp: "==", version: "1.26.0" }]);
      expect(locked.expanded).toBe(true);

      vi.runAllTimers();
      expect(component.createVirtualEnvironment).toHaveBeenCalledWith(0);
    });

    it("routes to the locked card and deletes packages that were removed from the DB", () => {
      component.pves = [
        {
          name: "scanpyenv",
          isLocked: true,
          userPackages: [
            { name: "numpy", versionOp: "==", version: "1.26.0" },
            { name: "pandas", versionOp: "==", version: "2.0.0" },
          ],
          newPackages: [],
          deletingPackages: [],
          pipOutput: "",
          prettyPipOutput: "",
          expanded: false,
          isInstalling: false,
        } as any,
      ];
      component.availableDbPves = [makeSaved({ numpy: "==1.26.0" })];

      component.installFromSavedPve(SAVED_VEID);

      const locked = component.pves[0];
      expect(locked.newPackages).toEqual([]);
      expect(locked.deletingPackages).toEqual([{ name: "pandas", version: "2.0.0" }]);
      // pandas should be dropped from userPackages so the install step won't
      // skip it later as "already installed".
      expect(locked.userPackages).toEqual([{ name: "numpy", versionOp: "==", version: "1.26.0" }]);

      vi.runAllTimers();
      expect(component.createVirtualEnvironment).toHaveBeenCalledWith(0);
    });

    it("treats a version change as delete-then-install on the locked card", () => {
      component.pves = [
        {
          name: "scanpyenv",
          isLocked: true,
          userPackages: [{ name: "scanpy", versionOp: "==", version: "1.11.1" }],
          newPackages: [],
          deletingPackages: [],
          pipOutput: "",
          prettyPipOutput: "",
          expanded: false,
          isInstalling: false,
        } as any,
      ];
      component.availableDbPves = [makeSaved({ scanpy: "==1.12.0" })];

      component.installFromSavedPve(SAVED_VEID);

      const locked = component.pves[0];
      expect(locked.deletingPackages).toEqual([{ name: "scanpy", version: "1.11.1" }]);
      expect(locked.newPackages).toEqual([{ name: "scanpy", versionOp: "==", version: "1.12.0" }]);
      // userPackages no longer contains scanpy so the install step won't drop it.
      expect(locked.userPackages).toEqual([]);

      vi.runAllTimers();
      expect(component.createVirtualEnvironment).toHaveBeenCalledWith(0);
    });

    it("is a no-op (with a success toast) when DB and CU contents match exactly", () => {
      const notificationService = TestBed.inject(NotificationService);
      const successSpy = vi.spyOn(notificationService, "success").mockImplementation(() => {});

      component.pves = [
        {
          name: "scanpyenv",
          isLocked: true,
          userPackages: [{ name: "numpy", versionOp: "==", version: "1.26.0" }],
          newPackages: [],
          deletingPackages: [],
          pipOutput: "",
          prettyPipOutput: "",
          expanded: false,
          isInstalling: false,
        } as any,
      ];
      component.availableDbPves = [makeSaved({ numpy: "==1.26.0" })];

      component.installFromSavedPve(SAVED_VEID);

      const locked = component.pves[0];
      expect(locked.newPackages).toEqual([]);
      expect(locked.deletingPackages).toEqual([]);
      expect(locked.userPackages).toEqual([{ name: "numpy", versionOp: "==", version: "1.26.0" }]);
      expect(successSpy).toHaveBeenCalled();

      vi.runAllTimers();
      expect(component.createVirtualEnvironment).not.toHaveBeenCalled();
    });

    it("is a no-op when the veid is not in availableDbPves", () => {
      component.pves = [];
      component.availableDbPves = [makeSaved({ pandas: "==2.0.0" })];

      component.installFromSavedPve(SAVED_VEID + 999);

      expect(component.pves).toEqual([]);
      vi.runAllTimers();
      expect(component.createVirtualEnvironment).not.toHaveBeenCalled();
    });

    it("matches an existing locked card when the saved name has surrounding whitespace", () => {
      component.pves = [
        {
          name: "scanpyenv",
          isLocked: true,
          userPackages: [{ name: "numpy", versionOp: "==", version: "1.26.0" }],
          newPackages: [],
          deletingPackages: [],
          pipOutput: "",
          prettyPipOutput: "",
          expanded: false,
          isInstalling: false,
        } as any,
      ];
      component.availableDbPves = [makeSaved({ numpy: "==1.26.0", pandas: "==2.0.0" }, "  scanpyenv  ")];

      component.installFromSavedPve(SAVED_VEID);

      // No new card pushed; the existing locked card is updated.
      expect(component.pves.length).toBe(1);
      const locked = component.pves[0];
      expect(locked.newPackages).toEqual([{ name: "pandas", versionOp: "==", version: "2.0.0" }]);

      vi.runAllTimers();
      expect(component.createVirtualEnvironment).toHaveBeenCalledWith(0);
    });

    it("matches an existing locked card when the locked card name has surrounding whitespace", () => {
      component.pves = [
        {
          name: "  scanpyenv  ",
          isLocked: true,
          userPackages: [],
          newPackages: [],
          deletingPackages: [],
          pipOutput: "",
          prettyPipOutput: "",
          expanded: false,
          isInstalling: false,
        } as any,
      ];
      component.availableDbPves = [makeSaved({ pandas: "==2.0.0" }, "scanpyenv")];

      component.installFromSavedPve(SAVED_VEID);

      expect(component.pves.length).toBe(1);
      const locked = component.pves[0];
      expect(locked.newPackages).toEqual([{ name: "pandas", versionOp: "==", version: "2.0.0" }]);

      vi.runAllTimers();
      expect(component.createVirtualEnvironment).toHaveBeenCalledWith(0);
    });

    it("treats names that differ only in case as different (case-sensitive match)", () => {
      component.pves = [
        {
          name: "ScanPyEnv",
          isLocked: true,
          userPackages: [],
          newPackages: [],
          deletingPackages: [],
          pipOutput: "",
          prettyPipOutput: "",
          expanded: false,
          isInstalling: false,
        } as any,
      ];
      component.availableDbPves = [makeSaved({ pandas: "==2.0.0" }, "scanpyenv")];

      component.installFromSavedPve(SAVED_VEID);

      // Saved name didn't match the existing locked card (different case),
      // so a new unlocked card is pushed.
      expect(component.pves.length).toBe(2);
      const pushed = component.pves[1];
      expect(pushed.name).toBe("scanpyenv");
      expect(pushed.isLocked).toBe(false);

      vi.runAllTimers();
      expect(component.createVirtualEnvironment).toHaveBeenCalledWith(1);
    });

    it("preserves the saved name verbatim (with whitespace) on the newly pushed card", () => {
      component.pves = [];
      component.availableDbPves = [makeSaved({ pandas: "==2.0.0" }, "  scanpyenv  ")];

      component.installFromSavedPve(SAVED_VEID);

      expect(component.pves.length).toBe(1);
      expect(component.pves[0].name).toBe("  scanpyenv  ");

      vi.runAllTimers();
      expect(component.createVirtualEnvironment).toHaveBeenCalledWith(0);
    });
  });

  describe("parseDbPackages", () => {
    it("returns an empty array for null packages", () => {
      const rows = (component as any).parseDbPackages(null);
      expect(rows).toEqual([]);
    });

    it("returns an empty array for undefined packages", () => {
      const rows = (component as any).parseDbPackages(undefined);
      expect(rows).toEqual([]);
    });

    it("parses ==X.Y.Z entries", () => {
      const rows = (component as any).parseDbPackages({ numpy: "==1.26.0" });
      expect(rows).toEqual([{ name: "numpy", versionOp: "==", version: "1.26.0" }]);
    });

    it("parses >= and <= operators", () => {
      const rows = (component as any).parseDbPackages({
        pandas: ">=2.0.0",
        scanpy: "<=1.10.5",
      });
      expect(rows).toEqual([
        { name: "pandas", versionOp: ">=", version: "2.0.0" },
        { name: "scanpy", versionOp: "<=", version: "1.10.5" },
      ]);
    });

    it("defaults to == when there is no recognized operator prefix", () => {
      const rows = (component as any).parseDbPackages({ numpy: "1.26.0" });
      expect(rows).toEqual([{ name: "numpy", versionOp: "==", version: "1.26.0" }]);
    });

    it("treats an empty string as no version, defaulting versionOp to ==", () => {
      const rows = (component as any).parseDbPackages({ numpy: "" });
      expect(rows).toEqual([{ name: "numpy", versionOp: "==", version: "" }]);
    });

    it("parses multiple packages and preserves the package name verbatim", () => {
      const rows = (component as any).parseDbPackages({
        "scikit-learn": "==1.3.0",
        numpy: "==1.26.0",
      });
      expect(rows).toContainEqual({ name: "scikit-learn", versionOp: "==", version: "1.3.0" });
      expect(rows).toContainEqual({ name: "numpy", versionOp: "==", version: "1.26.0" });
      expect(rows.length).toBe(2);
    });
  });

  describe("refreshAvailableDbPves", () => {
    let pveService: WorkflowPveService;

    beforeEach(() => {
      pveService = TestBed.inject(WorkflowPveService);
    });

    it("populates availableDbPves from listUserPves on success", () => {
      const records: UserPveRecord[] = [
        { veid: 1, name: "env1", packages: { numpy: "==1.26.0" } },
        { veid: 2, name: "env2", packages: {} },
      ];
      vi.spyOn(pveService, "listUserPves").mockReturnValue(of(records));

      component.availableDbPves = [{ veid: 999, name: "stale", packages: {} }];
      (component as any).refreshAvailableDbPves();

      expect(component.availableDbPves).toEqual(records);
    });

    it("clears availableDbPves and logs when listUserPves errors", () => {
      vi.spyOn(pveService, "listUserPves").mockReturnValue(throwError(() => new Error("fetch failed")));
      const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

      component.availableDbPves = [{ veid: 1, name: "stale", packages: {} }];
      (component as any).refreshAvailableDbPves();

      expect(component.availableDbPves).toEqual([]);
      expect(errorSpy).toHaveBeenCalled();
    });
  });

  describe("getPVEs() systemPackagesLoading flag", () => {
    const selectedUnit = {
      computingUnit: { cuid: 123 },
    } as unknown as DashboardWorkflowComputingUnit;

    let pveService: WorkflowPveService;

    beforeEach(() => {
      pveService = TestBed.inject(WorkflowPveService);
      component.selectedComputingUnit = selectedUnit;
      component.systemPackagesLoading = false;
      component.pves = [];
      component.systemPackages = [];
    });

    it("sets systemPackagesLoading=true immediately and keeps it true while /pve/system is in flight", () => {
      const pvesResp: PvePackageResponse[] = [{ pveName: "env1", userPackages: ["numpy==1.26.0"] }];
      const systemSubject = new Subject<PackageResponse>();
      vi.spyOn(pveService, "fetchPVEs").mockReturnValue(of(pvesResp));
      vi.spyOn(pveService, "getSystemPackages").mockReturnValue(systemSubject.asObservable());

      component.getPVEs();

      expect(component.systemPackagesLoading).toBe(true);
      expect(component.pves.length).toBe(1);
      expect(component.pves[0].name).toBe("env1");

      systemSubject.next({ system: ["pandas==2.0.0"] });
      systemSubject.complete();

      expect(component.systemPackagesLoading).toBe(false);
      expect(component.systemPackages).toEqual([{ name: "pandas", version: "2.0.0" }]);
    });

    it("clears systemPackagesLoading when /pve/system errors", () => {
      vi.spyOn(pveService, "fetchPVEs").mockReturnValue(of([] as PvePackageResponse[]));
      vi.spyOn(pveService, "getSystemPackages").mockReturnValue(throwError(() => new Error("system fetch failed")));
      vi.spyOn(console, "error").mockImplementation(() => {});

      component.getPVEs();

      expect(component.systemPackagesLoading).toBe(false);
      expect(component.systemPackages).toEqual([]);
    });

    it("clears systemPackagesLoading and resets state when /pve/pves errors", () => {
      const fetchPvesSpy = vi
        .spyOn(pveService, "fetchPVEs")
        .mockReturnValue(throwError(() => new Error("pves fetch failed")));
      const getSystemSpy = vi.spyOn(pveService, "getSystemPackages");
      vi.spyOn(console, "error").mockImplementation(() => {});

      component.pves = [{ name: "stale" }] as any;
      component.systemPackages = [{ name: "stale", version: "0.0.0" }];

      component.getPVEs();

      expect(fetchPvesSpy).toHaveBeenCalledWith(123);
      expect(getSystemSpy).not.toHaveBeenCalled();
      expect(component.systemPackagesLoading).toBe(false);
      expect(component.pves).toEqual([]);
      expect(component.systemPackages).toEqual([]);
    });
  });

  describe("createVirtualEnvironment name validation", () => {
    const VALIDATION_MSG = "Environment name must contain only letters and numbers.";

    let errorSpy: ReturnType<typeof vi.spyOn>;
    let runWsSpy: ReturnType<typeof vi.spyOn>;

    beforeEach(() => {
      errorSpy = vi.spyOn((component as any).notificationService, "error").mockImplementation(() => {});
      runWsSpy = vi.spyOn(component as any, "runPveWebSocket").mockImplementation(() => {});
    });

    function setSinglePve(name: string, isLocked = false): void {
      component.pves = [
        {
          name,
          isLocked,
          userPackages: [],
          newPackages: [],
          deletingPackages: [],
          pipOutput: "",
          prettyPipOutput: "",
          expanded: false,
          isInstalling: false,
        } as any,
      ];
    }

    const invalidCases: ReadonlyArray<readonly [string, string]> = [
      ["underscore", "my_env"],
      ["dash", "my-env"],
      ["internal whitespace", "my env"],
      ["dot", "my.env"],
      ["exclamation mark", "env!"],
      ["slash", "env/1"],
      ["only whitespace (empty after trim)", "   "],
      ["empty string", ""],
    ];

    invalidCases.forEach(([label, name]) => {
      it(`rejects ${label} (${JSON.stringify(name)}) and does not invoke the websocket`, () => {
        setSinglePve(name);

        component.createVirtualEnvironment(0);

        expect(errorSpy).toHaveBeenCalledWith(VALIDATION_MSG);
        expect(runWsSpy).not.toHaveBeenCalled();
      });
    });

    const validCases: ReadonlyArray<string> = ["env", "env1", "123", "ScanPyEnv", "abcXYZ0"];

    validCases.forEach(name => {
      it(`accepts ${JSON.stringify(name)} (alphanumeric only) and proceeds to create`, () => {
        setSinglePve(name);

        component.createVirtualEnvironment(0);

        expect(errorSpy).not.toHaveBeenCalledWith(VALIDATION_MSG);
        expect(runWsSpy).toHaveBeenCalled();
      });
    });

    it("trims surrounding whitespace before validating (valid name with padding passes)", () => {
      setSinglePve("  env1  ");

      component.createVirtualEnvironment(0);

      expect(errorSpy).not.toHaveBeenCalledWith(VALIDATION_MSG);
      expect(runWsSpy).toHaveBeenCalled();
    });

    it("rejects a name that is valid only before trimming (internal whitespace persists after trim)", () => {
      setSinglePve("  my env  ");

      component.createVirtualEnvironment(0);

      expect(errorSpy).toHaveBeenCalledWith(VALIDATION_MSG);
      expect(runWsSpy).not.toHaveBeenCalled();
    });

    it("rejects an invalid name on a locked card too (validation runs before the locked branch)", () => {
      const deleteSpy = vi.spyOn(component as any, "deleteUserPackages").mockImplementation(() => {});
      setSinglePve("my_env", true);

      component.createVirtualEnvironment(0);

      expect(errorSpy).toHaveBeenCalledWith(VALIDATION_MSG);
      expect(deleteSpy).not.toHaveBeenCalled();
      expect(runWsSpy).not.toHaveBeenCalled();
    });
  });

  describe("isCreateDisabled", () => {
    // Backs the per-environment OK button's [disabled]="isCreateDisabled(pve)":
    // the button stays disabled until the name has non-whitespace content.
    function pveWithName(name: string): any {
      return { name } as any;
    }

    it("disables when the name is empty", () => {
      expect(component.isCreateDisabled(pveWithName(""))).toBe(true);
    });

    it("disables when the name is only whitespace", () => {
      expect(component.isCreateDisabled(pveWithName("   "))).toBe(true);
    });

    it("enables when the name has non-whitespace content", () => {
      expect(component.isCreateDisabled(pveWithName("env1"))).toBe(false);
    });

    it("enables when a valid name is padded with whitespace", () => {
      expect(component.isCreateDisabled(pveWithName("  env1  "))).toBe(false);
    });
  });

  describe("selected computing unit stream (ngOnInit)", () => {
    // Boots a fresh component whose selected-unit stream is a Subject we drive.
    // Must run before the fresh fixture's first detectChanges so ngOnInit
    // subscribes to our subject rather than the mock's default of(null).
    function bootWithSelectedStream(): { selected$: Subject<DashboardWorkflowComputingUnit | null> } {
      const statusService = TestBed.inject(ComputingUnitStatusService);
      const selected$ = new Subject<DashboardWorkflowComputingUnit | null>();
      vi.spyOn(statusService, "getSelectedComputingUnit").mockReturnValue(selected$.asObservable());
      const freshFixture = TestBed.createComponent(ComputingUnitSelectionComponent);
      freshFixture.detectChanges();
      return { selected$ };
    }

    it("disables workflow modification and notifies when there are ongoing executions", () => {
      const actionService = TestBed.inject(WorkflowActionService);
      vi.spyOn(actionService, "getWorkflowMetadata").mockReturnValue({ ...DEFAULT_WORKFLOW, wid: 42 });
      const disableSpy = vi.spyOn(actionService, "disableWorkflowModification").mockImplementation(() => {});
      const enableSpy = vi.spyOn(actionService, "enableWorkflowModification").mockImplementation(() => {});
      const execService = TestBed.inject(WorkflowExecutionsService);
      const retrieveSpy = vi
        .spyOn(execService, "retrieveWorkflowExecutions")
        .mockReturnValue(of([{ eId: 1 }] as unknown as WorkflowExecutionsEntry[]));
      const infoSpy = vi.spyOn(TestBed.inject(NotificationService), "info").mockImplementation(() => {});

      const { selected$ } = bootWithSelectedStream();
      selected$.next(makeComputingUnit({ cuid: 7 }));

      expect(retrieveSpy).toHaveBeenCalledWith(42, [ExecutionState.Running, ExecutionState.Initializing]);
      expect(infoSpy).toHaveBeenCalled();
      expect(disableSpy).toHaveBeenCalledTimes(1);
      expect(enableSpy).not.toHaveBeenCalled();
    });

    it("enables workflow modification when there are no ongoing executions", () => {
      const actionService = TestBed.inject(WorkflowActionService);
      vi.spyOn(actionService, "getWorkflowMetadata").mockReturnValue({ ...DEFAULT_WORKFLOW, wid: 42 });
      const disableSpy = vi.spyOn(actionService, "disableWorkflowModification").mockImplementation(() => {});
      const enableSpy = vi.spyOn(actionService, "enableWorkflowModification").mockImplementation(() => {});
      const execService = TestBed.inject(WorkflowExecutionsService);
      vi.spyOn(execService, "retrieveWorkflowExecutions").mockReturnValue(of([] as WorkflowExecutionsEntry[]));
      const infoSpy = vi.spyOn(TestBed.inject(NotificationService), "info").mockImplementation(() => {});

      const { selected$ } = bootWithSelectedStream();
      selected$.next(makeComputingUnit({ cuid: 7 }));

      expect(enableSpy).toHaveBeenCalledTimes(1);
      expect(disableSpy).not.toHaveBeenCalled();
      expect(infoSpy).not.toHaveBeenCalled();
    });

    it("does not re-check executions when the same cuid re-emits (lastSelectedCuid guard)", () => {
      const actionService = TestBed.inject(WorkflowActionService);
      vi.spyOn(actionService, "getWorkflowMetadata").mockReturnValue({ ...DEFAULT_WORKFLOW, wid: 42 });
      vi.spyOn(actionService, "enableWorkflowModification").mockImplementation(() => {});
      const execService = TestBed.inject(WorkflowExecutionsService);
      const retrieveSpy = vi
        .spyOn(execService, "retrieveWorkflowExecutions")
        .mockReturnValue(of([] as WorkflowExecutionsEntry[]));

      const { selected$ } = bootWithSelectedStream();
      selected$.next(makeComputingUnit({ cuid: 9 }));
      selected$.next(makeComputingUnit({ cuid: 9 }));

      expect(retrieveSpy).toHaveBeenCalledTimes(1);
    });
  });

  describe("registerWorkflowMetadataSubscription (ngOnInit)", () => {
    // Boots a fresh component with a controlled metadata-change stream and a
    // mutable metadata object so we can flip the wid and re-emit at will.
    function bootWithMetaStream(): {
      comp: ComputingUnitSelectionComponent;
      emit: (wid: number | undefined) => void;
    } {
      const actionService = TestBed.inject(WorkflowActionService);
      const meta$ = new Subject<WorkflowMetadata>();
      vi.spyOn(actionService, "workflowMetaDataChanged").mockReturnValue(meta$.asObservable());
      let currentMeta: WorkflowMetadata = { ...DEFAULT_WORKFLOW };
      vi.spyOn(actionService, "getWorkflowMetadata").mockImplementation(() => currentMeta);
      const freshFixture = TestBed.createComponent(ComputingUnitSelectionComponent);
      freshFixture.detectChanges();
      const emit = (wid: number | undefined) => {
        currentMeta = { ...DEFAULT_WORKFLOW, wid };
        meta$.next(currentMeta);
      };
      return { comp: freshFixture.componentInstance, emit };
    }

    it("selects the computing unit from the latest execution when the workflow id changes", () => {
      const execService = TestBed.inject(WorkflowExecutionsService);
      vi.spyOn(execService, "retrieveLatestWorkflowExecution").mockReturnValue(
        of({ cuId: 55 } as unknown as WorkflowExecutionsEntry)
      );
      const { comp, emit } = bootWithMetaStream();
      const selectSpy = vi.spyOn(comp, "selectComputingUnit").mockImplementation(() => {});

      emit(100);

      expect(comp.workflowId).toBe(100);
      expect(selectSpy).toHaveBeenCalledWith(100, 55);
    });

    it("falls back to the first Running unit when retrieving the latest execution fails", () => {
      const execService = TestBed.inject(WorkflowExecutionsService);
      vi.spyOn(execService, "retrieveLatestWorkflowExecution").mockReturnValue(
        throwError(() => new Error("no execution"))
      );
      const { comp, emit } = bootWithMetaStream();
      comp.allComputingUnits = [
        makeComputingUnit({ cuid: 1, status: "Pending" }),
        makeComputingUnit({ cuid: 2, status: "Running" }),
      ];
      const selectSpy = vi.spyOn(comp, "selectComputingUnit").mockImplementation(() => {});

      emit(100);

      expect(selectSpy).toHaveBeenCalledWith(100, 2);
    });

    it("does not fetch again when the workflow id is unchanged", () => {
      const execService = TestBed.inject(WorkflowExecutionsService);
      const latestSpy = vi
        .spyOn(execService, "retrieveLatestWorkflowExecution")
        .mockReturnValue(of({ cuId: 1 } as unknown as WorkflowExecutionsEntry));
      const { emit } = bootWithMetaStream();

      emit(100);
      latestSpy.mockClear();
      emit(100);

      expect(latestSpy).not.toHaveBeenCalled();
    });
  });

  describe("selectComputingUnit guards", () => {
    it("does nothing when the cuid is undefined", () => {
      const selectSpy = vi.spyOn(TestBed.inject(ComputingUnitStatusService), "selectComputingUnit");
      component.selectComputingUnit(5, undefined);
      expect(selectSpy).not.toHaveBeenCalled();
    });

    it("does nothing when the wid is the default (unsaved) workflow id", () => {
      const selectSpy = vi.spyOn(TestBed.inject(ComputingUnitStatusService), "selectComputingUnit");
      component.selectComputingUnit(DEFAULT_WORKFLOW.wid, 3);
      expect(selectSpy).not.toHaveBeenCalled();
    });

    it("selects for a valid wid/cuid pair", () => {
      const selectSpy = vi.spyOn(TestBed.inject(ComputingUnitStatusService), "selectComputingUnit");
      component.selectComputingUnit(5, 3);
      expect(selectSpy).toHaveBeenCalledWith(5, 3);
    });
  });

  describe("status helpers", () => {
    it("getButtonText returns 'Connect' with no selection and the unit name otherwise", () => {
      component.selectedComputingUnit = null;
      expect(component.getButtonText()).toBe("Connect");
      component.selectedComputingUnit = makeComputingUnit({ name: "My Unit" });
      expect(component.getButtonText()).toBe("My Unit");
    });

    it("computeStatus maps selection + status onto badge states", () => {
      component.selectedComputingUnit = null;
      expect(component.computeStatus()).toBe("processing");
      component.selectedComputingUnit = makeComputingUnit({ status: "Running" });
      expect(component.computeStatus()).toBe("success");
      component.selectedComputingUnit = makeComputingUnit({ status: "Pending" });
      expect(component.computeStatus()).toBe("warning");
      component.selectedComputingUnit = makeComputingUnit({ status: "Terminating" });
      expect(component.computeStatus()).toBe("warning");
      component.selectedComputingUnit = makeComputingUnit({ status: "Failed" });
      expect(component.computeStatus()).toBe("error");
    });

    it("cannotSelectUnit only allows Running units", () => {
      expect(component.cannotSelectUnit(makeComputingUnit({ status: "Running" }))).toBe(false);
      expect(component.cannotSelectUnit(makeComputingUnit({ status: "Pending" }))).toBe(true);
    });

    it("isSelectedUnit matches on the computing unit uri", () => {
      component.selectedComputingUnit = makeComputingUnit({ cuid: 1, uri: "uri-1" });
      expect(component.isSelectedUnit(makeComputingUnit({ cuid: 1, uri: "uri-1" }))).toBe(true);
      expect(component.isSelectedUnit(makeComputingUnit({ cuid: 2, uri: "uri-2" }))).toBe(false);
    });

    it("isComputingUnitRunning reflects the selected unit status", () => {
      component.selectedComputingUnit = null;
      expect(component.isComputingUnitRunning()).toBe(false);
      component.selectedComputingUnit = makeComputingUnit({ status: "Running" });
      expect(component.isComputingUnitRunning()).toBe(true);
      component.selectedComputingUnit = makeComputingUnit({ status: "Pending" });
      expect(component.isComputingUnitRunning()).toBe(false);
    });
  });

  describe("terminateComputingUnit", () => {
    it("errors and does nothing when the cuid is not a known unit", () => {
      component.allComputingUnits = [];
      const errorSpy = vi.spyOn(TestBed.inject(NotificationService), "error").mockImplementation(() => {});
      const confirmSpy = vi
        .spyOn(TestBed.inject(ComputingUnitActionsService), "confirmAndTerminate")
        .mockImplementation(() => {});

      component.terminateComputingUnit(999);

      expect(errorSpy).toHaveBeenCalledWith("Invalid computing unit.");
      expect(confirmSpy).not.toHaveBeenCalled();
    });

    it("delegates to confirmAndTerminate for a known non-local unit without deleting environments", () => {
      const unit = makeComputingUnit({ cuid: 5, type: "kubernetes" });
      component.allComputingUnits = [unit];
      component.selectedComputingUnit = makeComputingUnit({ cuid: 5, type: "kubernetes" });
      const confirmSpy = vi
        .spyOn(TestBed.inject(ComputingUnitActionsService), "confirmAndTerminate")
        .mockImplementation(() => {});
      const deleteEnvSpy = vi.spyOn(TestBed.inject(WorkflowPveService), "deleteEnvironments");

      component.terminateComputingUnit(5);

      expect(confirmSpy).toHaveBeenCalledWith(5, unit);
      expect(deleteEnvSpy).not.toHaveBeenCalled();
    });

    it("also deletes PVE environments for a local selected unit, swallowing errors", () => {
      const unit = makeComputingUnit({ cuid: 5, type: "local" });
      component.allComputingUnits = [unit];
      component.selectedComputingUnit = makeComputingUnit({ cuid: 5, type: "local" });
      vi.spyOn(TestBed.inject(ComputingUnitActionsService), "confirmAndTerminate").mockImplementation(() => {});
      const deleteEnvSpy = vi
        .spyOn(TestBed.inject(WorkflowPveService), "deleteEnvironments")
        .mockReturnValue(throwError(() => new Error("boom")));
      const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});

      component.terminateComputingUnit(5);

      expect(deleteEnvSpy).toHaveBeenCalledWith(5);
      expect(consoleSpy).toHaveBeenCalled();
      consoleSpy.mockRestore();
    });
  });

  describe("startEditingUnitName", () => {
    it("blocks non-owners and leaves the editing state untouched", () => {
      const errorSpy = vi.spyOn(TestBed.inject(NotificationService), "error").mockImplementation(() => {});
      component.editingNameOfUnit = null;

      component.startEditingUnitName(makeComputingUnit({ cuid: 3, isOwner: false }));

      expect(errorSpy).toHaveBeenCalledWith("Only owners can rename computing units");
      expect(component.editingNameOfUnit).toBeNull();
    });

    it("enters the edit state for an owned unit", () => {
      vi.useFakeTimers();
      try {
        component.startEditingUnitName(makeComputingUnit({ cuid: 3, name: "Env", isOwner: true }));
        expect(component.editingNameOfUnit).toBe(3);
        expect(component.editingUnitName).toBe("Env");
        // Flush the deferred focus() setTimeout; the input is not in the main DOM
        // tree here, so the guarded focus call is a no-op.
        vi.runAllTimers();
      } finally {
        vi.useRealTimers();
      }
    });
  });

  describe("confirmUpdateUnitName", () => {
    it("rejects an invalid (empty) name and cancels editing", () => {
      const errorSpy = vi.spyOn(TestBed.inject(NotificationService), "error").mockImplementation(() => {});
      const renameSpy = vi.spyOn(TestBed.inject(WorkflowComputingUnitManagingService), "renameComputingUnit");
      component.editingNameOfUnit = 3;
      component.editingUnitName = "old";

      component.confirmUpdateUnitName(3, "   ");

      expect(errorSpy).toHaveBeenCalled();
      expect(renameSpy).not.toHaveBeenCalled();
      expect(component.editingNameOfUnit).toBeNull();
      expect(component.editingUnitName).toBe("");
    });

    it("renames, patches local + selected caches, refreshes the list and clears edit state on success", () => {
      const listUnit = makeComputingUnit({ cuid: 3, name: "old" });
      component.allComputingUnits = [listUnit];
      component.selectedComputingUnit = makeComputingUnit({ cuid: 3, name: "old" });
      const renameSpy = vi
        .spyOn(TestBed.inject(WorkflowComputingUnitManagingService), "renameComputingUnit")
        .mockReturnValue(of({} as Response));
      const refreshSpy = vi.spyOn(TestBed.inject(ComputingUnitStatusService), "refreshComputingUnitList");
      vi.spyOn(TestBed.inject(NotificationService), "success").mockImplementation(() => {});
      component.editingNameOfUnit = 3;

      component.confirmUpdateUnitName(3, "  NewName  ");

      expect(renameSpy).toHaveBeenCalledWith(3, "NewName");
      expect(component.allComputingUnits[0].computingUnit.name).toBe("NewName");
      expect(component.selectedComputingUnit!.computingUnit.name).toBe("NewName");
      expect(refreshSpy).toHaveBeenCalled();
      expect(component.editingNameOfUnit).toBeNull();
      expect(component.editingUnitName).toBe("");
    });

    it("shows a failure toast and still clears the edit state when rename errors", () => {
      component.allComputingUnits = [makeComputingUnit({ cuid: 3, name: "old" })];
      vi.spyOn(TestBed.inject(WorkflowComputingUnitManagingService), "renameComputingUnit").mockReturnValue(
        throwError(() => new Error("nope"))
      );
      const errorSpy = vi.spyOn(TestBed.inject(NotificationService), "error").mockImplementation(() => {});
      component.editingNameOfUnit = 3;
      component.editingUnitName = "old";

      component.confirmUpdateUnitName(3, "NewName");

      expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining("Failed to rename"));
      expect(component.editingNameOfUnit).toBeNull();
      expect(component.editingUnitName).toBe("");
    });
  });

  describe("onDropdownVisibilityChange", () => {
    it("refreshes the computing unit list when the dropdown opens", () => {
      const refreshSpy = vi.spyOn(TestBed.inject(ComputingUnitStatusService), "refreshComputingUnitList");
      component.onDropdownVisibilityChange(true);
      expect(refreshSpy).toHaveBeenCalledTimes(1);
    });

    it("does nothing when the dropdown closes", () => {
      const refreshSpy = vi.spyOn(TestBed.inject(ComputingUnitStatusService), "refreshComputingUnitList");
      component.onDropdownVisibilityChange(false);
      expect(refreshSpy).not.toHaveBeenCalled();
    });
  });

  describe("PVE modal open/close", () => {
    it("showPVEmodalVisible opens the modal and refreshes packages + saved environments", () => {
      component.selectedComputingUnit = makeComputingUnit({ cuid: 12 });
      const getPvesSpy = vi.spyOn(component, "getPVEs").mockImplementation(() => {});
      const refreshDbSpy = vi.spyOn(component as any, "refreshAvailableDbPves").mockImplementation(() => {});

      component.showPVEmodalVisible();

      expect(component.pveModalVisible).toBe(true);
      expect(getPvesSpy).toHaveBeenCalledTimes(1);
      expect(refreshDbSpy).toHaveBeenCalledTimes(1);
    });

    it("closePveModal tears down sockets, clears draft state and hides the modal", () => {
      const closeMock = vi.fn();
      component.pves = [{ name: "a", socket: { close: closeMock } as any, isInstalling: true } as any];
      component.availableDbPves = [{ veid: 1, name: "x", packages: {} }];
      component.pveModalVisible = true;

      component.closePveModal();

      expect(closeMock).toHaveBeenCalled();
      expect(component.pves[0].socket).toBeUndefined();
      expect(component.pves[0].isInstalling).toBe(false);
      expect(component.availableDbPves).toEqual([]);
      expect(component.pveModalVisible).toBe(false);
    });
  });

  describe("package row editing", () => {
    it("addPackage appends an empty new-package row to the indexed environment", () => {
      component.pves = [{ name: "env", newPackages: [] } as any];
      component.addPackage(0);
      expect(component.pves[0].newPackages).toEqual([
        { name: "", version: "", versionOp: undefined, deleteToggle: false },
      ]);
    });

    it("togglePackageDelete moves a package into deletingPackages and back out", () => {
      component.pves = [{ name: "env", deletingPackages: [] } as any];
      const pkg = { name: "numpy", version: "1.26.0", deleteToggle: false } as any;

      component.togglePackageDelete(0, pkg);
      expect(pkg.deleteToggle).toBe(true);
      expect(component.pves[0].deletingPackages).toEqual([{ name: "numpy", version: "1.26.0" }]);

      component.togglePackageDelete(0, pkg);
      expect(pkg.deleteToggle).toBe(false);
      expect(component.pves[0].deletingPackages).toEqual([]);
    });
  });

  describe("parsePackageRows / updatePrettyPipOutput", () => {
    it("parsePackageRows splits '=='-pinned entries and trims name + version", () => {
      const rows = (component as any).parsePackageRows(["numpy==1.26.0", "  scipy == 1.11.0 ", "flask"]);
      expect(rows).toEqual([
        { name: "numpy", versionOp: "==", version: "1.26.0" },
        { name: "scipy", versionOp: "==", version: "1.11.0" },
        { name: "flask", versionOp: "==", version: "" },
      ]);
    });

    it("updatePrettyPipOutput escapes html, styles success lines and converts newlines to <br/>", () => {
      component.pves = [
        { name: "env", pipOutput: "[pip] Successfully installed numpy\n<script>&\n", prettyPipOutput: "" } as any,
      ];

      component.updatePrettyPipOutput(0);

      const out = component.pves[0].prettyPipOutput;
      expect(out).toContain('<span class="pip-exit ok"><strong>[pip] Successfully installed numpy</strong></span>');
      expect(out).toContain("&lt;script&gt;");
      expect(out).toContain("&amp;");
      expect(out).toContain("<br/>");
    });
  });

  describe("template rendering (overlays)", () => {
    afterEach(() => {
      // Clear the overlay contents rather than removing the container element itself:
      // CDK's OverlayContainer caches that element, so removing it would make later
      // overlays render into a detached node.
      document.querySelectorAll(".cdk-overlay-container").forEach(el => (el.innerHTML = ""));
    });

    it("renders the PVE modal body: system section, saved environments and env cards", () => {
      component.selectedComputingUnit = makeComputingUnit({ cuid: 12 });
      component.systemPackagesLoading = false;
      component.systemPackages = [{ name: "numpy", version: "1.26.0" }];
      component.availableDbPves = [{ veid: 1, name: "scanpyenv", packages: {} }];
      component.pves = [
        {
          name: "envA",
          isLocked: true,
          userPackages: [{ name: "numpy", versionOp: "==", version: "1.26.0" }],
          newPackages: [{ name: "pandas", versionOp: "==", version: "2.0.0" }],
          deletingPackages: [],
          pipOutput: "",
          prettyPipOutput: "log line",
          expanded: true,
          isInstalling: false,
        } as any,
      ];
      component.pveModalVisible = true;
      fixture.detectChanges();

      expect(document.querySelector(".system-section")).toBeTruthy();
      expect(document.querySelector(".saved-pves-section")).toBeTruthy();
      expect(document.querySelector(".saved-pve-row")).toBeTruthy();
      expect(document.querySelector(".saved-pve-name")?.textContent).toContain("scanpyenv");
      expect(document.querySelector(".ve-form")).toBeTruthy();
      expect(document.querySelector(".pip-panel")).toBeTruthy();
      expect(document.querySelectorAll(".package-row").length).toBeGreaterThan(0);
    });

    // The dropdown-menu rows live inside <nz-dropdown-menu>'s deferred
    // <ng-template>, which ng-zorro only stamps into a CDK overlay on open —
    // that overlay does not attach under jsdom, so the rows are not queryable
    // in the document. Coverage of the menu template still comes from the
    // detectChanges below: Angular instantiates projected content eagerly, so
    // the *ngFor rows and their per-unit bindings run even while parked. The
    // assertions here target the trigger button, which renders in the main DOM.
    it("renders the trigger button with the selected unit's name and a status badge", () => {
      component.allComputingUnits = [
        makeComputingUnit({ cuid: 1, name: "Running Unit", status: "Running", isOwner: true }),
        makeComputingUnit({ cuid: 2, name: "Pending Unit", status: "Pending", isOwner: true }),
      ];
      component.selectedComputingUnit = component.allComputingUnits[0];
      fixture.detectChanges();

      const host = fixture.nativeElement as HTMLElement;
      expect(host.querySelector(".unit-name-text")?.textContent).toContain("Running Unit");
      expect(host.querySelector("nz-badge")).toBeTruthy();
      expect(host.querySelector(".connect-text")).toBeNull();
    });

    it("renders the 'Connect' label on the trigger when no unit is selected", () => {
      component.selectedComputingUnit = null;
      component.allComputingUnits = [];
      fixture.detectChanges();

      const host = fixture.nativeElement as HTMLElement;
      expect(host.querySelector(".connect-text")?.textContent).toContain("Connect");
      expect(host.querySelector(".unit-name-text")).toBeNull();
    });
  });
});
