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
import { of, throwError } from "rxjs";
import { NzMessageModule, NzMessageService } from "ng-zorro-antd/message";
import { AdminComputingUnitComponent } from "./admin-computing-unit.component";
import { WorkflowComputingUnitManagingService } from "../../../../common/service/computing-unit/workflow-computing-unit/workflow-computing-unit-managing.service";
import { DashboardWorkflowComputingUnit } from "../../../../common/type/workflow-computing-unit";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { UserService } from "../../../../common/service/user/user.service";
import { StubUserService } from "../../../../common/service/user/stub-user.service";

function makeUnit(over: Partial<DashboardWorkflowComputingUnit> = {}): DashboardWorkflowComputingUnit {
  return {
    computingUnit: {
      cuid: 1,
      uid: 100,
      name: "cu",
      creationTime: 1_700_000_000_000,
      terminateTime: undefined,
      type: "kubernetes",
      uri: "uri",
      resource: {
        cpuLimit: "2",
        memoryLimit: "4Gi",
        gpuLimit: "0",
        jvmMemorySize: "2G",
        shmSize: "64Mi",
        nodeAddresses: [],
      },
    },
    status: "Running",
    metrics: { cpuUsage: "NaN", memoryUsage: "NaN" },
    isOwner: false,
    accessPrivilege: "WRITE",
    ownerGoogleAvatar: "",
    ownerName: "alice",
    ...over,
  };
}

function localUnit(): DashboardWorkflowComputingUnit {
  return makeUnit({
    computingUnit: {
      ...makeUnit().computingUnit,
      type: "local",
      resource: {
        cpuLimit: "NaN",
        memoryLimit: "NaN",
        gpuLimit: "NaN",
        jvmMemorySize: "NaN",
        shmSize: "NaN",
        nodeAddresses: [],
      },
    },
  });
}

describe("AdminComputingUnitComponent", () => {
  let component: AdminComputingUnitComponent;
  let fixture: ComponentFixture<AdminComputingUnitComponent>;
  let service: WorkflowComputingUnitManagingService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      providers: [
        WorkflowComputingUnitManagingService,
        { provide: UserService, useClass: StubUserService },
        ...commonTestProviders,
      ],
      imports: [AdminComputingUnitComponent, HttpClientTestingModule, NzMessageModule],
    }).compileComponents();

    fixture = TestBed.createComponent(AdminComputingUnitComponent);
    component = fixture.componentInstance;
    service = TestBed.inject(WorkflowComputingUnitManagingService);
    // Keep the fetch inert/synchronous; deliberately no detectChanges() so ngOnInit's poll never starts.
    vi.spyOn(service, "listAllComputingUnits").mockReturnValue(of([]));
  });

  afterEach(() => {
    vi.restoreAllMocks();
    fixture.destroy();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  // The only test that renders the template. Every binding a row uses — including the
  // `date` pipe on the creation-time tooltip — resolves here and nowhere else, so a pipe or
  // directive missing from the component's `imports` fails this test instead of only the
  // AOT app build (which the other specs, deliberately render-free, never exercise).
  it("renders a row per unit", () => {
    vi.mocked(service.listAllComputingUnits).mockReturnValue(of([makeUnit()]));

    fixture.detectChanges();

    // nz-table adds a hidden measure row to tbody, so match on the owner cell every data row has.
    const dataRows = Array.from<HTMLElement>(fixture.nativeElement.querySelectorAll("tbody tr")).filter(
      row => row.querySelector("texera-user-avatar") !== null
    );
    expect(dataRows.length).toBe(1);
    expect(dataRows[0].textContent).toContain("alice");
  });

  it("fetchData loads all units and clears the loading flag", () => {
    const units = [makeUnit(), makeUnit({ computingUnit: { ...makeUnit().computingUnit, cuid: 2 } })];
    vi.mocked(service.listAllComputingUnits).mockReturnValue(of(units));

    component.fetchData();

    expect(component.computingUnits).toEqual(units);
    expect(component.isLoading).toBe(false);
  });

  it("fetchData clears the loading flag and shows a message when the fetch fails", () => {
    const errorSpy = vi.spyOn(TestBed.inject(NzMessageService), "error").mockReturnValue({} as any);
    vi.mocked(service.listAllComputingUnits).mockReturnValue(throwError(() => ({ error: { message: "boom" } })));

    component.fetchData();

    // On error the spinner must stop rather than spin forever, and the failure is surfaced.
    expect(component.isLoading).toBe(false);
    expect(errorSpy).toHaveBeenCalledWith("boom");
  });

  describe("resourceSummary", () => {
    it("joins CPU, memory and GPU with a middot and labels", () => {
      const unit = makeUnit({
        computingUnit: {
          ...makeUnit().computingUnit,
          resource: { ...makeUnit().computingUnit.resource, gpuLimit: "1" },
        },
      });
      expect(component.resourceSummary(unit)).toBe("2 CPU · 4Gi · 1 GPU");
    });

    it("omits GPU when there is none", () => {
      expect(component.resourceSummary(makeUnit())).toBe("2 CPU · 4Gi");
    });

    it("shows a no-limits message for local units", () => {
      expect(component.resourceSummary(localUnit())).toBe("Local — no limits");
    });
  });

  describe("displaySpec", () => {
    it("renders a real value unchanged", () => {
      expect(component.displaySpec("2Gi")).toBe("2Gi");
    });

    it("renders NaN and empty as an em dash", () => {
      expect(component.displaySpec("NaN")).toBe("—");
      expect(component.displaySpec("")).toBe("—");
    });
  });

  describe("isLocal", () => {
    it("is true only for local units", () => {
      expect(component.isLocal(localUnit())).toBe(true);
      expect(component.isLocal(makeUnit())).toBe(false);
    });
  });

  describe("onExpandChange", () => {
    it("adds and removes a cuid from the expanded set", () => {
      component.onExpandChange(7, true);
      expect(component.expandedCuids.has(7)).toBe(true);

      component.onExpandChange(7, false);
      expect(component.expandedCuids.has(7)).toBe(false);
    });
  });

  describe("client-side sort and filter", () => {
    it("sorts by name", () => {
      const a = makeUnit({ computingUnit: { ...makeUnit().computingUnit, name: "a" } });
      const b = makeUnit({ computingUnit: { ...makeUnit().computingUnit, name: "b" } });
      expect(component.sortByName(a, b)).toBeLessThan(0);
      expect(component.sortByName(b, a)).toBeGreaterThan(0);
    });

    it("sorts by creation time numerically", () => {
      const older = makeUnit({ computingUnit: { ...makeUnit().computingUnit, creationTime: 1 } });
      const newer = makeUnit({ computingUnit: { ...makeUnit().computingUnit, creationTime: 2 } });
      expect(component.sortByCreated(older, newer)).toBeLessThan(0);
    });

    it("filters by type", () => {
      expect(component.filterByType(["local"], localUnit())).toBe(true);
      expect(component.filterByType(["local"], makeUnit())).toBe(false);
    });

    it("filters by status", () => {
      const pending = makeUnit({ status: "Pending" });
      expect(component.filterByStatus(["Pending"], pending)).toBe(true);
      expect(component.filterByStatus(["Pending"], makeUnit())).toBe(false);
    });
  });
});
