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
        { name: "  scanpy_env ", isLocked: true, userPackages: [] } as any,
        { name: "other", isLocked: false, userPackages: [] } as any,
      ];
      expect(component.isSavedPveInstalledInCu("scanpy_env")).toBe(true);
    });

    it("returns false when the same-name card is not locked (draft only)", () => {
      component.pves = [{ name: "scanpy_env", isLocked: false, userPackages: [] } as any];
      expect(component.isSavedPveInstalledInCu("scanpy_env")).toBe(false);
    });

    it("returns false when no card matches", () => {
      component.pves = [{ name: "other", isLocked: true, userPackages: [] } as any];
      expect(component.isSavedPveInstalledInCu("scanpy_env")).toBe(false);
    });
  });

  describe("installFromSavedPve", () => {
    const SAVED_VEID = 42;

    function makeSaved(packages: Record<string, string>, name = "scanpy_env"): UserPveRecord {
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
      expect(card.name).toBe("scanpy_env");
      expect(card.newPackages).toEqual([{ name: "pandas", versionOp: "==", version: "2.0.0" }]);
      expect(card.deletingPackages).toEqual([]);

      vi.runAllTimers();
      expect(component.createVirtualEnvironment).toHaveBeenCalledWith(0);
    });

    it("routes to the locked card and installs only packages that are new in the DB", () => {
      component.pves = [
        {
          name: "scanpy_env",
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
          name: "scanpy_env",
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
          name: "scanpy_env",
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
          name: "scanpy_env",
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
      const pvesResp: PvePackageResponse[] = [{ pveName: "env-a", userPackages: ["numpy==1.26.0"] }];
      const systemSubject = new Subject<PackageResponse>();
      vi.spyOn(pveService, "fetchPVEs").mockReturnValue(of(pvesResp));
      vi.spyOn(pveService, "getSystemPackages").mockReturnValue(systemSubject.asObservable());

      component.getPVEs();

      expect(component.systemPackagesLoading).toBe(true);
      expect(component.pves.length).toBe(1);
      expect(component.pves[0].name).toBe("env-a");

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
});
