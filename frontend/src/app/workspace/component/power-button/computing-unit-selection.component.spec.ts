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
import { ComputingUnitSelectionComponent } from "./computing-unit-selection.component";
import { NzButtonModule } from "ng-zorro-antd/button";
import { CommonModule } from "@angular/common";
import { NzIconModule } from "ng-zorro-antd/icon";
import { ActivatedRoute, ActivatedRouteSnapshot, convertToParamMap, Data, Params, UrlSegment } from "@angular/router";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { NzModalModule, NzModalService } from "ng-zorro-antd/modal";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/mock-computing-unit-status.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { UserPveRecord } from "../../service/virtual-environment/virtual-environment.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { Subject, of, throwError } from "rxjs";
import {
  PackageResponse,
  PvePackageResponse,
  WorkflowPveService,
} from "../../service/virtual-environment/virtual-environment.service";
import { DashboardWorkflowComputingUnit } from "../../../common/type/workflow-computing-unit";

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
      ],
      providers: [
        { provide: ActivatedRoute, useValue: activatedRouteMock },
        { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
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
});
