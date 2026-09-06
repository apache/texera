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
import { ComputingUnitMetadataComponent } from "../../../common/util/computing-unit.util";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { NzPopoverDirective } from "ng-zorro-antd/popover";

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
    statusReason: string;
    isOwner: boolean;
  }> = {}
): DashboardWorkflowComputingUnit {
  const {
    cuid = 1,
    name = "unit",
    uri = `uri-${cuid}`,
    type = "kubernetes",
    status = "Running",
    statusReason = undefined,
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
    statusReason,
    metrics: { cpuUsage: "N/A", memoryUsage: "N/A" },
    isOwner,
    accessPrivilege: "WRITE",
    ownerAvatar: "",
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

    /** Builds one locked card whose single package is `pkg`. */
    function lockedCardWith(pkg: Record<string, unknown>): void {
      component.pves = [
        {
          name: "scanpyenv",
          isLocked: true,
          userPackages: [pkg],
          newPackages: [],
          deletingPackages: [],
          pipOutput: "",
          prettyPipOutput: "",
          expanded: false,
          isInstalling: false,
        } as any,
      ];
    }

    it("treats a card package with no version and a blank database version as unchanged", () => {
      const successSpy = vi.spyOn(TestBed.inject(NotificationService), "success").mockImplementation(() => {});
      lockedCardWith({ name: "numpy", versionOp: "==", version: undefined });
      component.availableDbPves = [makeSaved({ numpy: "" })];

      component.installFromSavedPve(SAVED_VEID);

      // Both sides fall back to "" through `?? ""`, so the diff is empty.
      expect(successSpy).toHaveBeenCalledWith(expect.stringContaining("already up to date"));
      expect(component.pves[0].newPackages).toEqual([]);
      expect(component.pves[0].deletingPackages).toEqual([]);
    });

    it("replaces a card package with no version when the database pins one", () => {
      lockedCardWith({ name: "numpy", versionOp: "==", version: undefined });
      component.availableDbPves = [makeSaved({ numpy: "==1.26.0" })];

      component.installFromSavedPve(SAVED_VEID);

      const locked = component.pves[0];
      expect(locked.deletingPackages).toEqual([{ name: "numpy", version: "" }]);
      expect(locked.newPackages).toEqual([{ name: "numpy", versionOp: "==", version: "1.26.0" }]);
      expect(locked.userPackages).toEqual([]);
    });

    it("deletes a versionless card package the database no longer lists", () => {
      lockedCardWith({ name: "numpy", versionOp: "==", version: undefined });
      component.availableDbPves = [makeSaved({ pandas: "==2.0.0" })];

      component.installFromSavedPve(SAVED_VEID);

      const locked = component.pves[0];
      expect(locked.deletingPackages).toEqual([{ name: "numpy", version: "" }]);
      expect(locked.newPackages).toEqual([{ name: "pandas", versionOp: "==", version: "2.0.0" }]);
    });

    it("reads a database row that carries no version at all through the same fallback", () => {
      // parseDbPackages always produces a string, so the `db.version ?? ""` side of the
      // comparison is only reachable by handing applySavedPveAsUpdate the row directly.
      const successSpy = vi.spyOn(TestBed.inject(NotificationService), "success").mockImplementation(() => {});
      lockedCardWith({ name: "numpy", versionOp: "==", version: "" });

      (component as any).applySavedPveAsUpdate(0, "scanpyenv", [{ name: "numpy", versionOp: "==" }]);

      expect(successSpy).toHaveBeenCalledWith(expect.stringContaining("already up to date"));
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

    it("treats a null version as no version", () => {
      // A row the database stored without a spec: `raw?.match?.()` short-circuits and the
      // `?? ""` fallback keeps the row usable instead of writing `null` into the version.
      const rows = (component as any).parseDbPackages({ numpy: null });
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

    it("treats a system package with no pinned version as versionless", () => {
      vi.spyOn(pveService, "fetchPVEs").mockReturnValue(of([] as PvePackageResponse[]));
      vi.spyOn(pveService, "getSystemPackages").mockReturnValue(of({ system: ["  numpy  "] }));

      component.getPVEs();

      // Splitting on "==" yields no second element, so the `?? ""` fallback runs.
      expect(component.systemPackages).toEqual([{ name: "numpy", version: "" }]);
    });
  });

  /**
   * The PVE block is the largest uncovered region in the component, and the suite above stops
   * exactly at its seams: it stubs out `runPveWebSocket` and `deleteUserPackages` so it can assert
   * name validation without opening a socket. These drive the other side of those seams.
   *
   * The only environmental need is a stand-in for `WebSocket`, so `onmessage` / `onerror` can be
   * fired by hand — jsdom has no WebSocket and the component never inspects anything but the
   * handlers it assigns.
   */
  describe("the virtual-environment socket", () => {
    /** The sockets opened during a test, newest last. */
    let sockets: FakeSocket[];
    /** The notification channel every guard in this block reports through. */
    let errorSpy: ReturnType<typeof vi.spyOn>;

    class FakeSocket {
      static opened: FakeSocket[] = [];
      onmessage: ((e: { data: string }) => void) | null = null;
      onerror: (() => void) | null = null;
      close = vi.fn();
      constructor(public url: string) {
        FakeSocket.opened.push(this);
      }
      /** Delivers one server line to the component. */
      say(data: string): void {
        this.onmessage?.({ data });
      }
    }

    /** Installs one unlocked environment and returns the component's view of it. */
    function setPve(over: Record<string, unknown> = {}): void {
      component.pves = [
        {
          name: "envone",
          isLocked: false,
          isInstalling: false,
          userPackages: [],
          newPackages: [],
          deletingPackages: [],
          pipOutput: "",
          prettyPipOutput: "",
          expanded: false,
          ...over,
        } as any,
      ];
    }

    beforeEach(() => {
      FakeSocket.opened = [];
      sockets = FakeSocket.opened;
      vi.stubGlobal("WebSocket", FakeSocket as unknown as typeof WebSocket);
      errorSpy = vi.spyOn((component as any).notificationService, "error").mockImplementation(() => {});
      // The socket URL is built from the selected unit's cuid, so a unit has to be selected.
      component.selectedComputingUnit = makeComputingUnit({ name: "unit" });
    });

    afterEach(() => {
      vi.unstubAllGlobals();
      // console is global, and the suite has no blanket restore, so a spy installed on it here
      // would otherwise outlive the test that made it and swallow later output.
      vi.restoreAllMocks();
    });

    it("opens a socket for the environment and locks the card while it runs", () => {
      setPve();

      component.createVirtualEnvironment(0);

      expect(sockets).toHaveLength(1);
      expect(component.pves[0].isInstalling).toBe(true);
      expect(component.pves[0].isLocked).toBe(true);
      expect(component.pves[0].pipOutput).toContain("Creating virtual environment");
    });

    it("trims the environment name before it reaches the socket URL", () => {
      // The name is user input from an inline editor; an untrimmed name produces a URL for an
      // environment the backend does not have.
      const urlSpy = vi.spyOn(TestBed.inject(WorkflowPveService), "getPveWebSocketUrl");
      setPve({ name: "  spaced  " });

      component.createVirtualEnvironment(0);

      expect(component.pves[0].name).toBe("spaced");
      expect(urlSpy).toHaveBeenCalledWith(expect.anything(), "spaced", "create", []);
    });

    it("appends each line the server sends to the pip output", () => {
      setPve();
      component.createVirtualEnvironment(0);

      sockets[0].say("collecting numpy");
      sockets[0].say("installed");

      expect(component.pves[0].pipOutput).toContain("collecting numpy");
      expect(component.pves[0].pipOutput).toContain("installed");
      // Still running: the card stays locked until the sentinel arrives.
      expect(component.pves[0].isInstalling).toBe(true);
    });

    it("closes the socket and unlocks installing on the __DONE__ sentinel", () => {
      // Without this the spinner never stops and the card is stuck mid-install.
      // The continuation is stubbed out deliberately: letting the real delete/install chain run
      // would reset isInstalling downstream, so the assertion would pass even if this branch
      // never cleared it.
      vi.spyOn(component as any, "deleteUserPackages").mockImplementation(() => {});
      setPve();
      component.createVirtualEnvironment(0);

      sockets[0].say("__DONE__");

      expect(component.pves[0].isInstalling).toBe(false);
      expect(component.pves[0].socket).toBeUndefined();
      expect(sockets[0].close).toHaveBeenCalled();
    });

    it("does not print the sentinel as though it were output", () => {
      setPve();
      component.createVirtualEnvironment(0);

      sockets[0].say("__DONE__");

      expect(component.pves[0].pipOutput).not.toContain("__DONE__");
    });

    it("reports a dropped connection in the output rather than hanging", () => {
      setPve();
      component.createVirtualEnvironment(0);

      sockets[0].onerror?.();

      expect(component.pves[0].pipOutput).toContain("[WebSocket error]");
      expect(component.pves[0].isInstalling).toBe(false);
      expect(component.pves[0].socket).toBeUndefined();
    });

    it("closes any socket still open on the card before starting another", () => {
      // Re-running a create on a card that is already streaming would otherwise leave the first
      // socket writing into the same pipOutput.
      setPve();
      component.createVirtualEnvironment(0);
      const first = sockets[0];

      component.pves[0] = { ...component.pves[0], isLocked: false } as any;
      component.createVirtualEnvironment(0);

      expect(first.close).toHaveBeenCalled();
      expect(sockets).toHaveLength(2);
    });

    it("reinstalls the recorded packages once the environment is created", () => {
      // The create socket's completion chains delete-then-install, which is how a rebuilt
      // environment gets its packages back.
      const deleteSpy = vi
        .spyOn(component as any, "deleteUserPackages")
        .mockImplementation((...args: unknown[]) => (args[1] as (() => void) | undefined)?.());
      const installSpy = vi.spyOn(component as any, "installUserPackages").mockImplementation(() => {});
      setPve();

      component.createVirtualEnvironment(0);
      sockets[0].say("__DONE__");

      expect(deleteSpy).toHaveBeenCalled();
      expect(installSpy).toHaveBeenCalled();
    });

    it("skips creation entirely for a locked card and only syncs its packages", () => {
      const deleteSpy = vi
        .spyOn(component as any, "deleteUserPackages")
        .mockImplementation((...args: unknown[]) => (args[1] as (() => void) | undefined)?.());
      const installSpy = vi.spyOn(component as any, "installUserPackages").mockImplementation(() => {});
      setPve({ isLocked: true });

      component.createVirtualEnvironment(0);

      expect(sockets).toHaveLength(0);
      expect(deleteSpy).toHaveBeenCalled();
      expect(installSpy).toHaveBeenCalled();
    });

    it("refuses to create a second environment under a name another card already uses", () => {
      setPve();
      // The nameless card is scanned first, so the duplicate check has to survive it before it
      // reaches the card that actually clashes.
      const card = component.pves[0];
      component.pves = [{ ...card, name: undefined }, { ...card }, { ...card, name: " envone " }] as any;

      component.createVirtualEnvironment(2);

      expect(errorSpy).toHaveBeenCalledWith("An environment with this name already exists.");
      expect(sockets).toHaveLength(0);
      expect(component.pves[2].isInstalling).toBeFalsy();
    });

    it("starts the log from empty when the card has produced no output yet", () => {
      setPve();
      component.createVirtualEnvironment(0);
      // runPveWebSocket seeds the card with its banner; clearing it models a card whose log
      // was reset while the socket was still attached, and exercises the handler's fallback.
      (component.pves[0] as { pipOutput?: string }).pipOutput = undefined;

      sockets[0].say("collecting numpy");

      expect(component.pves[0].pipOutput).toBe("collecting numpy\n");
    });

    it("records a dropped connection even when the card has produced no output yet", () => {
      setPve();
      component.createVirtualEnvironment(0);
      (component.pves[0] as { pipOutput?: string }).pipOutput = undefined;

      sockets[0].onerror?.();

      expect(component.pves[0].pipOutput).toBe("\n[WebSocket error]\n");
      expect(component.pves[0].isInstalling).toBe(false);
    });

    describe("installUserPackages", () => {
      it("refuses a package that is missing its operator or its version", () => {
        setPve({ newPackages: [{ name: "numpy", versionOp: undefined, version: "1.26.0" }] });
        (component as any).installUserPackages(0);
        expect(errorSpy).toHaveBeenLastCalledWith("Please specify an operator and version for each package.");

        setPve({ newPackages: [{ name: "numpy", versionOp: "==", version: "   " }] });
        (component as any).installUserPackages(0);
        expect(errorSpy).toHaveBeenLastCalledWith("Please specify an operator and version for each package.");

        expect(sockets).toHaveLength(0);
      });

      it("skips a package already present as a system package or in the environment", () => {
        vi.spyOn(TestBed.inject(WorkflowPveService), "getUserPackages").mockReturnValue(of([]));
        // The system list is matched case-insensitively, so "NumPy" must block "numpy".
        component.systemPackages = [{ name: "NumPy", version: "1.26.0" }];
        setPve({
          userPackages: [{ name: "pandas", versionOp: "==", version: "2.0.0" }],
          newPackages: [
            { name: "   ", versionOp: "==", version: "1.0.0" },
            { name: "numpy", versionOp: "==", version: "1.26.0" },
            { name: "Pandas", versionOp: "==", version: "2.0.0" },
          ],
        });

        (component as any).installUserPackages(0);

        expect(errorSpy).toHaveBeenCalledWith("Skipped numpy: already installed as a system package.");
        expect(errorSpy).toHaveBeenCalledWith("Skipped Pandas: already installed in this environment.");
        // Nothing survives the filters, so no socket is opened and the draft rows are cleared.
        expect(sockets).toHaveLength(0);
        expect(component.pves[0].newPackages).toEqual([]);
        expect(component.pves[0].isInstalling).toBe(false);
      });

      it("has nothing to install when the card carries no draft rows at all", () => {
        const refreshSpy = vi.spyOn(TestBed.inject(WorkflowPveService), "getUserPackages").mockReturnValue(of([]));
        setPve({ newPackages: undefined });

        (component as any).installUserPackages(0);

        expect(sockets).toHaveLength(0);
        expect(refreshSpy).toHaveBeenCalledWith(1, "envone");
      });

      it("opens an install socket for the surviving packages and reloads the list when it finishes", () => {
        const urlSpy = vi.spyOn(TestBed.inject(WorkflowPveService), "getPveWebSocketUrl");
        vi.spyOn(TestBed.inject(WorkflowPveService), "getUserPackages").mockReturnValue(of(["numpy==1.26.0"]));
        component.systemPackages = [];
        setPve({ newPackages: [{ name: "  numpy  ", versionOp: "==", version: "  1.26.0  " }] });

        (component as any).installUserPackages(0);

        expect(urlSpy).toHaveBeenCalledWith(1, "envone", "install", ["numpy==1.26.0"]);
        expect(sockets).toHaveLength(1);

        sockets[0].say("__DONE__");

        expect(component.pves[0].newPackages).toEqual([]);
        expect(component.pves[0].userPackages).toEqual([{ name: "numpy", versionOp: "==", version: "1.26.0" }]);
      });

      it("logs and keeps the current list when reloading the packages fails", () => {
        const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
        vi.spyOn(TestBed.inject(WorkflowPveService), "getUserPackages").mockReturnValue(
          throwError(() => new Error("nope"))
        );
        setPve({ userPackages: [{ name: "numpy", versionOp: "==", version: "1.26.0" }] });

        (component as any).refreshUserPackages(0);

        expect(consoleSpy).toHaveBeenCalledWith("Failed to refresh user packages", expect.any(Error));
        expect(component.pves[0].userPackages).toEqual([{ name: "numpy", versionOp: "==", version: "1.26.0" }]);
      });
    });

    describe("deleteUserPackages", () => {
      it("deletes the queued packages one at a time and reports a failure without stopping", () => {
        const deleteSpy = vi
          .spyOn(TestBed.inject(WorkflowPveService), "deletePackage")
          .mockReturnValueOnce(of(["removed numpy"]))
          .mockReturnValueOnce(throwError(() => new Error("boom")));
        vi.spyOn(TestBed.inject(WorkflowPveService), "getUserPackages").mockReturnValue(of([]));
        setPve({
          pipOutput: undefined,
          deletingPackages: [
            { name: "numpy", version: "1.26.0" },
            { name: "pandas", version: "2.0.0" },
          ],
        });
        const onDone = vi.fn();

        (component as any).deleteUserPackages(0, onDone);

        expect(deleteSpy).toHaveBeenNthCalledWith(1, 1, "envone", "numpy");
        expect(deleteSpy).toHaveBeenNthCalledWith(2, 1, "envone", "pandas");
        expect(component.pves[0].pipOutput).toContain("removed numpy");
        expect(component.pves[0].pipOutput).toContain("[PVE][ERR] Failed to delete package: pandas");
        // The queue is drained and the caller resumes exactly once.
        expect(component.pves[0].deletingPackages).toEqual([]);
        expect(onDone).toHaveBeenCalledTimes(1);
      });
    });
  });

  describe("onClickOpenShareAccess", () => {
    it("opens the share dialog for the given computing unit", async () => {
      const openSpy = vi
        .spyOn(TestBed.inject(ComputingUnitActionsService), "openShareAccessModal")
        .mockImplementation(() => {});

      await component.onClickOpenShareAccess(7);

      expect(openSpy).toHaveBeenCalledWith(7, true);
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

    it("does not look up an execution for an unsaved workflow", () => {
      const execService = TestBed.inject(WorkflowExecutionsService);
      const latestSpy = vi.spyOn(execService, "retrieveLatestWorkflowExecution");
      const { comp, emit } = bootWithMetaStream();

      // The placeholder id an unsaved workflow carries: recorded, but never queried for.
      emit(DEFAULT_WORKFLOW.wid);

      expect(comp.workflowId).toBe(DEFAULT_WORKFLOW.wid);
      expect(latestSpy).not.toHaveBeenCalled();
    });

    it("selects nothing when the fetch fails and no unit is Running", () => {
      const execService = TestBed.inject(WorkflowExecutionsService);
      vi.spyOn(execService, "retrieveLatestWorkflowExecution").mockReturnValue(
        throwError(() => new Error("no execution"))
      );
      const { comp, emit } = bootWithMetaStream();
      comp.allComputingUnits = [makeComputingUnit({ cuid: 1, status: "Pending" })];
      const selectSpy = vi.spyOn(comp, "selectComputingUnit").mockImplementation(() => {});

      emit(100);

      expect(selectSpy).not.toHaveBeenCalled();
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

    it("focuses and selects the rename box once it is in the document", () => {
      vi.useFakeTimers();
      const input = document.createElement("input");
      input.className = "unit-name-edit-input";
      document.body.appendChild(input);
      const focus = vi.spyOn(input, "focus").mockImplementation(() => {});
      const select = vi.spyOn(input, "select").mockImplementation(() => {});
      try {
        component.startEditingUnitName(makeComputingUnit({ cuid: 3, name: "Env", isOwner: true }));

        vi.runAllTimers();

        expect(focus).toHaveBeenCalledTimes(1);
        expect(select).toHaveBeenCalledTimes(1);
      } finally {
        input.remove();
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

    it("leaves the caches alone when the renamed unit is neither listed nor selected", () => {
      component.allComputingUnits = [makeComputingUnit({ cuid: 9, name: "other" })];
      component.selectedComputingUnit = makeComputingUnit({ cuid: 9, name: "other" });
      vi.spyOn(TestBed.inject(WorkflowComputingUnitManagingService), "renameComputingUnit").mockReturnValue(
        of({} as Response)
      );
      const refreshSpy = vi.spyOn(TestBed.inject(ComputingUnitStatusService), "refreshComputingUnitList");
      vi.spyOn(TestBed.inject(NotificationService), "success").mockImplementation(() => {});
      component.editingNameOfUnit = 3;

      component.confirmUpdateUnitName(3, "NewName");

      expect(component.allComputingUnits[0].computingUnit.name).toBe("other");
      expect(component.selectedComputingUnit!.computingUnit.name).toBe("other");
      expect(refreshSpy).toHaveBeenCalled();
      expect(component.editingNameOfUnit).toBeNull();
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

    it("togglePackageDelete records a versionless package with an empty version", () => {
      component.pves = [{ name: "env", deletingPackages: [] } as any];
      const pkg = { name: "numpy", versionOp: "==", version: undefined, deleteToggle: false } as any;

      component.togglePackageDelete(0, pkg);

      expect(component.pves[0].deletingPackages).toEqual([{ name: "numpy", version: "" }]);
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

    it("updatePrettyPipOutput renders an environment that has produced no output as empty", () => {
      component.pves = [{ name: "env", pipOutput: undefined, prettyPipOutput: "stale" } as any];

      component.updatePrettyPipOutput(0);

      expect(component.pves[0].prettyPipOutput).toBe("");
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

  describe("resource display accessors", () => {
    beforeEach(() => {
      component.selectedComputingUnit = makeComputingUnit();
    });

    it("reads the selected unit's raw resource limits and usage", () => {
      expect(component.getCurrentComputingUnitCpuLimit()).toBe("1");
      expect(component.getCurrentComputingUnitMemoryLimit()).toBe("1Gi");
      expect(component.getCurrentComputingUnitGpuLimit()).toBe("0");
      expect(component.getCurrentComputingUnitJvmMemorySize()).toBe("1Gi");
      expect(component.getCurrentSharedMemorySize()).toBe("64Mi");
      expect(component.getCurrentComputingUnitCpuUsage()).toBe("N/A");
      expect(component.getCurrentComputingUnitMemoryUsage()).toBe("N/A");
    });

    it("returns 'NaN' for every accessor when no unit is selected", () => {
      component.selectedComputingUnit = null;
      expect(component.getCurrentComputingUnitCpuLimit()).toBe("NaN");
      expect(component.getCurrentComputingUnitMemoryLimit()).toBe("NaN");
      expect(component.getCurrentComputingUnitGpuLimit()).toBe("NaN");
      expect(component.getCurrentComputingUnitJvmMemorySize()).toBe("NaN");
      expect(component.getCurrentSharedMemorySize()).toBe("NaN");
      expect(component.getCurrentComputingUnitCpuUsage()).toBe("NaN");
      expect(component.getCurrentComputingUnitMemoryUsage()).toBe("NaN");
    });
  });

  describe("resource display getters", () => {
    beforeEach(() => {
      component.selectedComputingUnit = makeComputingUnit();
    });

    it("derives the display limits and units from the raw values", () => {
      expect(component.getCpuLimit()).toBe(1);
      expect(component.getMemoryLimit()).toBe(1);
      expect(component.getGpuLimit()).toBe("0");
      expect(component.getJvmMemorySize()).toBe("1Gi");
      expect(component.getSharedMemorySize()).toBe("64Mi");
      expect(component.getCpuLimitUnit()).toBe("CPU");
      expect(component.getMemoryLimitUnit()).toBe("Gi");
    });

    it("returns zero usage values and percentages when metrics are unavailable", () => {
      expect(component.getCpuValue()).toBe(0);
      expect(component.getMemoryValue()).toBe(0);
      expect(component.getCpuPercentage()).toBe(0);
      expect(component.getMemoryPercentage()).toBe(0);
    });

    it("maps a zero percentage to the 'success' progress status", () => {
      expect(component.getCpuStatus()).toBe("success");
      expect(component.getMemoryStatus()).toBe("success");
    });

    it("maps a status to a badge color", () => {
      expect(component.getBadgeColor("Running")).toBe("green");
      expect(component.getBadgeColor("Pending")).toBe("gold");
      expect(component.getBadgeColor("Terminating")).toBe("gold");
      expect(component.getBadgeColor("Failed")).toBe("red");
      expect(component.getBadgeColor("Unknown")).toBe("red");
    });
  });

  describe("trackBy helpers", () => {
    it("trackByCuid keys a unit by its cuid", () => {
      expect(component.trackByCuid(0, makeComputingUnit({ cuid: 77 }))).toBe(77);
    });

    it("trackByIndex returns the index", () => {
      expect(component.trackByIndex(4)).toBe(4);
    });
  });

  describe("cancelEditingUnitName", () => {
    it("clears the editing state", () => {
      component.editingNameOfUnit = 3;
      component.editingUnitName = "half-typed";
      component.cancelEditingUnitName();
      expect(component.editingNameOfUnit).toBeNull();
      expect(component.editingUnitName).toBe("");
    });
  });

  describe("openComputingUnitMetadataModal", () => {
    it("opens the metadata modal with the unit as nzData", () => {
      const unit = makeComputingUnit({ cuid: 9 });
      const createSpy = vi.spyOn(TestBed.inject(NzModalService), "create").mockReturnValue({} as any);
      try {
        component.openComputingUnitMetadataModal(unit);

        expect(createSpy).toHaveBeenCalledWith(
          expect.objectContaining({
            nzTitle: "Computing Unit Information",
            nzContent: ComputingUnitMetadataComponent,
            nzData: unit,
            nzFooter: null,
          })
        );
      } finally {
        createSpy.mockRestore();
      }
    });
  });

  describe("scrollToBottomOfPipModal", () => {
    // The handler defers a scroll via setTimeout and reads scrollHeight, which
    // jsdom reports as 0 — drive it with fake timers and a plain stand-in element
    // (not a real node) so the assertion is deterministic and layout-independent.
    it("scrolls the pip log element to the bottom on the deferred tick", () => {
      vi.useFakeTimers();
      const el = { scrollTop: 0, scrollHeight: 500 } as unknown as HTMLElement;
      const getByIdSpy = vi.spyOn(document, "getElementById").mockReturnValue(el);
      try {
        component.scrollToBottomOfPipModal(2);
        expect(el.scrollTop).toBe(0); // deferred; not applied yet
        vi.runAllTimers();
        expect(getByIdSpy).toHaveBeenCalledWith("pip-log-2");
        expect(el.scrollTop).toBe(500);
      } finally {
        getByIdSpy.mockRestore();
        vi.useRealTimers();
      }
    });

    it("is a safe no-op when the pip log element is absent", () => {
      vi.useFakeTimers();
      const getByIdSpy = vi.spyOn(document, "getElementById").mockReturnValue(null);
      try {
        expect(() => {
          component.scrollToBottomOfPipModal(9);
          vi.runAllTimers();
        }).not.toThrow();
      } finally {
        getByIdSpy.mockRestore();
        vi.useRealTimers();
      }
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // Rows of the dropdown menu, driven through the markup ng-zorro stamps into
  // the CDK overlay rather than by calling the handlers on the instance.
  // ──────────────────────────────────────────────────────────────────────────
  describe("dropdown menu rows (rendered)", () => {
    /**
     * ng-zorro gates the dropdown's overlay behind an `auditTime(150)` that it
     * schedules outside the Angular zone, so neither `fakeAsync`'s `tick()` nor
     * Vitest's fake timers reach it — zone.js captured the native timer before
     * either could patch it. Poll for the rows instead of sleeping a fixed
     * amount, so a slow runner costs extra iterations rather than a failure.
     */
    async function openDropdown(): Promise<HTMLElement[]> {
      fixture.debugElement.query(By.css(".computing-units-dropdown-button")).nativeElement.click();
      for (let i = 0; i < 80 && document.querySelectorAll(".computing-unit-option").length === 0; i++) {
        await new Promise(resolve => setTimeout(resolve, 25));
        fixture.detectChanges();
      }
      fixture.detectChanges();
      const rows = Array.from(document.querySelectorAll<HTMLElement>(".computing-unit-option"));
      expect(rows.length).toBeGreaterThan(0);
      return rows;
    }

    function click(element: Element | null | undefined): void {
      expect(element).toBeTruthy();
      element!.dispatchEvent(new MouseEvent("click", { bubbles: true }));
      fixture.detectChanges();
    }

    let selectSpy: ReturnType<typeof vi.spyOn>;
    let sharingDefault: boolean;

    beforeEach(() => {
      sharingDefault = TestBed.inject(GuiConfigService).env.sharingComputingUnitEnabled;
      component.workflowId = 5;
      component.allComputingUnits = [
        makeComputingUnit({ cuid: 1, name: "Alpha" }),
        makeComputingUnit({ cuid: 2, name: "Beta" }),
      ];
      selectSpy = vi
        .spyOn(TestBed.inject(ComputingUnitStatusService), "selectComputingUnit")
        .mockImplementation(() => {});
      fixture.detectChanges();
    });

    afterEach(() => {
      // The share test flips this flag. TestBed hands every test its own
      // MockGuiConfigService, so it cannot leak today, but restore it anyway so the
      // block stays correct if these tests ever share one config instance.
      TestBed.inject(GuiConfigService).env.sharingComputingUnitEnabled = sharingDefault;
      // Clear the overlay contents rather than the container itself, which CDK caches.
      document.querySelectorAll(".cdk-overlay-container").forEach(el => (el.innerHTML = ""));
      vi.restoreAllMocks();
    });

    it("selects the unit whose row is clicked", async () => {
      const rows = await openDropdown();

      click(rows[1]);

      expect(component.selectedComputingUnit?.computingUnit.cuid).toBe(2);
      expect(selectSpy).toHaveBeenCalledWith(5, 2);
    });

    it("commits a rename when the inline editor loses focus", async () => {
      const renameSpy = vi
        .spyOn(TestBed.inject(WorkflowComputingUnitManagingService), "renameComputingUnit")
        .mockReturnValue(of({} as any));
      component.editingNameOfUnit = 1;
      component.editingUnitName = "Alpha";
      const rows = await openDropdown();

      const editor = rows[0].querySelector<HTMLInputElement>(".unit-name-edit-input");
      expect(editor?.value).toBe("Alpha");
      editor!.value = "Renamed";
      editor!.dispatchEvent(new FocusEvent("focusout", { bubbles: true }));
      fixture.detectChanges();

      expect(renameSpy).toHaveBeenCalledWith(1, "Renamed");
    });

    it("commits a rename when Enter is pressed in the inline editor", async () => {
      const renameSpy = vi
        .spyOn(TestBed.inject(WorkflowComputingUnitManagingService), "renameComputingUnit")
        .mockReturnValue(of({} as any));
      component.editingNameOfUnit = 1;
      component.editingUnitName = "Alpha";
      const rows = await openDropdown();

      const editor = rows[0].querySelector<HTMLInputElement>(".unit-name-edit-input");
      editor!.value = "Entered";
      editor!.dispatchEvent(new KeyboardEvent("keyup", { key: "Enter", bubbles: true }));
      fixture.detectChanges();

      expect(renameSpy).toHaveBeenCalledWith(1, "Entered");
    });

    it("abandons the rename when Escape is pressed in the inline editor", async () => {
      const renameSpy = vi.spyOn(TestBed.inject(WorkflowComputingUnitManagingService), "renameComputingUnit");
      component.editingNameOfUnit = 1;
      component.editingUnitName = "Alpha";
      const rows = await openDropdown();

      rows[0]
        .querySelector<HTMLInputElement>(".unit-name-edit-input")!
        .dispatchEvent(new KeyboardEvent("keyup", { key: "Escape", bubbles: true }));
      fixture.detectChanges();

      expect(component.editingNameOfUnit).toBeNull();
      expect(renameSpy).not.toHaveBeenCalled();
    });

    it("swallows a click inside the inline editor instead of selecting the row", async () => {
      component.editingNameOfUnit = 1;
      component.editingUnitName = "Alpha";
      const rows = await openDropdown();

      click(rows[0].querySelector(".unit-name-edit-input"));

      expect(selectSpy).not.toHaveBeenCalled();
      expect(component.editingNameOfUnit).toBe(1);
    });
    it("opens the inline editor from the pencil without selecting the row", async () => {
      const rows = await openDropdown();

      click(rows[0].querySelector(".anticon-edit"));

      expect(component.editingNameOfUnit).toBe(1);
      expect(component.editingUnitName).toBe("Alpha");
      expect(selectSpy).not.toHaveBeenCalled();
    });

    it("opens the python-environment modal from the plus icon without selecting the row", async () => {
      const rows = await openDropdown();

      click(rows[1].querySelector(".anticon-plus"));

      expect(component.pveModalVisible).toBe(true);
      expect(component.selectedComputingUnit?.computingUnit.cuid).toBe(2);
      expect(selectSpy).not.toHaveBeenCalled();
    });

    it("opens share access from the share icon without selecting the row", async () => {
      TestBed.inject(GuiConfigService).env.sharingComputingUnitEnabled = true;
      const shareSpy = vi
        .spyOn(TestBed.inject(ComputingUnitActionsService), "openShareAccessModal")
        .mockImplementation(() => {});
      fixture.detectChanges();
      const rows = await openDropdown();

      click(rows[0].querySelector(".anticon-share-alt"));

      expect(shareSpy).toHaveBeenCalledWith(1, true);
      expect(selectSpy).not.toHaveBeenCalled();
    });

    it("opens the metadata modal from the eye icon without selecting the row", async () => {
      const createSpy = vi.spyOn(TestBed.inject(NzModalService), "create").mockReturnValue({} as any);
      const rows = await openDropdown();

      click(rows[1].querySelector(".anticon-eye"));

      expect(createSpy).toHaveBeenCalledTimes(1);
      expect(createSpy.mock.calls[0][0].nzData).toBe(component.allComputingUnits[1]);
      expect(selectSpy).not.toHaveBeenCalled();
    });

    it("terminates from the delete icon without selecting the row", async () => {
      const terminateSpy = vi
        .spyOn(TestBed.inject(ComputingUnitActionsService), "confirmAndTerminate")
        .mockImplementation(() => {});
      const rows = await openDropdown();

      click(rows[0].querySelector(".anticon-delete"));

      expect(terminateSpy).toHaveBeenCalledWith(1, component.allComputingUnits[0]);
      expect(selectSpy).not.toHaveBeenCalled();
    });

    it("opens the create-unit modal from the row below the divider", async () => {
      await openDropdown();

      click(document.querySelector(".create-computing-unit"));

      expect(component.addComputeUnitModalVisible).toBe(true);
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // The metrics popover's own template, and the PVE modal's close controls.
  // ──────────────────────────────────────────────────────────────────────────
  describe("metrics popover (rendered)", () => {
    /** A running kubernetes unit whose resource limits the popover reads. */
    function unitWithResources(resource: Partial<Record<string, string>>): DashboardWorkflowComputingUnit {
      const unit = makeComputingUnit({ cuid: 7, name: "Metrics" });
      unit.computingUnit.resource = { ...unit.computingUnit.resource, ...(resource as any) };
      return unit;
    }

    /** Opens the metrics popover and returns the metric names it rendered, in order. */
    async function openMetrics(): Promise<{ names: string[]; overlay: HTMLElement }> {
      const container = fixture.debugElement.query(By.css("#metrics-container-id"));
      expect(container).toBeTruthy();
      container.injector.get(NzPopoverDirective).show();
      fixture.detectChanges();
      // the tooltip base positions its overlay in a microtask
      await Promise.resolve();
      fixture.detectChanges();
      const overlay = document.querySelector<HTMLElement>(".cdk-overlay-container")!;
      const names = Array.from(overlay.querySelectorAll(".resource-metrics .general-metric .metric-name")).map(
        el => el.textContent?.trim() ?? ""
      );
      return { names, overlay };
    }

    afterEach(() => {
      document.querySelectorAll(".cdk-overlay-container").forEach(el => (el.innerHTML = ""));
      vi.restoreAllMocks();
    });

    it("renders the CPU and RAM rows with their value, limit and percentage", async () => {
      component.selectedComputingUnit = unitWithResources({ cpuLimit: "2", memoryLimit: "4Gi" });
      fixture.detectChanges();

      const { names, overlay } = await openMetrics();

      expect(names).toContain("CPU");
      expect(names).toContain("RAM");
      const cpu = overlay.querySelector(".cpu-metric .metric-value")!;
      expect(cpu.querySelector(".metric-unit")?.textContent).toContain("2");
      expect(cpu.querySelector(".metric-percentage")?.textContent).toContain("%");
      const memory = overlay.querySelector(".memory-metric .metric-value")!;
      expect(memory.querySelector(".metric-unit")?.textContent).toContain("4");
      expect(memory.querySelector(".metric-percentage")?.textContent).toContain("%");
    });

    it("adds the GPU, JVM and shared-memory rows when the unit declares those limits", async () => {
      component.gpuOptions = ["0", "1"];
      component.selectedComputingUnit = unitWithResources({
        gpuLimit: "2",
        jvmMemorySize: "1Gi",
        shmSize: "64Mi",
      });
      fixture.detectChanges();

      const { names, overlay } = await openMetrics();

      expect(names).toEqual(["CPU", "RAM", "GPU", "JVM Memory Size", "Shared Memory Size"]);
      expect(overlay.textContent).toContain("2 GPU(s)");
    });

    it("drops the GPU row when the deployment offers no GPU at all", async () => {
      component.gpuOptions = ["0"];
      component.selectedComputingUnit = unitWithResources({ gpuLimit: "2" });
      fixture.detectChanges();

      expect((await openMetrics()).names).not.toContain("GPU");
    });

    it("drops each optional row whose limit reads as zero or NaN", async () => {
      component.gpuOptions = ["0", "1"];
      component.selectedComputingUnit = unitWithResources({
        gpuLimit: "0",
        jvmMemorySize: "NaN",
        shmSize: "0",
      });
      fixture.detectChanges();

      expect((await openMetrics()).names).toEqual(["CPU", "RAM"]);
    });

    it("has no popover content to show while no unit is selected", () => {
      component.selectedComputingUnit = null;
      fixture.detectChanges();

      expect(fixture.debugElement.query(By.css("#metrics-container-id"))).toBeNull();
    });

    it("closes the python-environment modal from its footer button and its cancel", () => {
      component.pveModalVisible = true;
      fixture.detectChanges();

      document.querySelector<HTMLButtonElement>(".footer-all button")!.click();
      fixture.detectChanges();

      expect(component.pveModalVisible).toBe(false);

      component.pveModalVisible = true;
      fixture.detectChanges();
      // the create-unit child renders an nz-modal too, so pick this one by its title
      const pveModal = fixture.debugElement
        .queryAll(By.css("nz-modal"))
        .find(modal => modal.componentInstance?.nzTitle === "Python Environments");
      pveModal!.triggerEventHandler("nzOnCancel", undefined);

      expect(component.pveModalVisible).toBe(false);
    });
  });
});
