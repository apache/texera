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

import { HttpErrorResponse } from "@angular/common/http";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { of, Subject, throwError } from "rxjs";
import { NzMessageService } from "ng-zorro-antd/message";
import { NzModalService } from "ng-zorro-antd/modal";
import { AdminComputingUnitComponent } from "./admin-computing-unit.component";
import { WorkflowComputingUnitManagingService } from "../../../../common/service/computing-unit/workflow-computing-unit/workflow-computing-unit-managing.service";
import { ComputingUnitActionsService } from "../../../../common/service/computing-unit/computing-unit-actions/computing-unit-actions.service";
import {
  DashboardWorkflowComputingUnit,
  WorkflowComputingUnit,
  WorkflowComputingUnitResourceLimit,
} from "../../../../common/type/workflow-computing-unit";
import { ComputingUnitState } from "../../../../common/type/computing-unit-connection.interface";
import { commonTestImports, commonTestProviders } from "../../../../common/testing/test-utils";
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
    ownerAvatar: "",
    ownerName: "alice",
    ...over,
  };
}

// Overrides fields of the nested `computingUnit`, so a test need not respread it.
function withCu(over: Partial<WorkflowComputingUnit>): Partial<DashboardWorkflowComputingUnit> {
  return { computingUnit: { ...makeUnit().computingUnit, ...over } };
}

// The all-placeholder resource a local unit carries.
const NAN_RESOURCE: WorkflowComputingUnitResourceLimit = {
  cpuLimit: "NaN",
  memoryLimit: "NaN",
  gpuLimit: "NaN",
  jvmMemorySize: "NaN",
  shmSize: "NaN",
  nodeAddresses: [],
};

function withResource(over: Partial<WorkflowComputingUnitResourceLimit>): Partial<DashboardWorkflowComputingUnit> {
  return withCu({ resource: { ...makeUnit().computingUnit.resource, ...over } });
}

function localUnit(cuid = 1): DashboardWorkflowComputingUnit {
  return makeUnit(withCu({ cuid, type: "local", resource: NAN_RESOURCE }));
}

describe("AdminComputingUnitComponent", () => {
  let component: AdminComputingUnitComponent;
  let fixture: ComponentFixture<AdminComputingUnitComponent>;
  let service: WorkflowComputingUnitManagingService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      providers: [NzModalService, { provide: UserService, useClass: StubUserService }, ...commonTestProviders],
      imports: [AdminComputingUnitComponent, ...commonTestImports],
    }).compileComponents();

    fixture = TestBed.createComponent(AdminComputingUnitComponent);
    component = fixture.componentInstance;
    service = TestBed.inject(WorkflowComputingUnitManagingService);
    // No `detectChanges()` here, so the poll starts only in the tests that ask for it.
    vi.spyOn(service, "listAllComputingUnits").mockReturnValue(of([]));
  });

  afterEach(() => {
    vi.restoreAllMocks();
    fixture.destroy();
  });

  // `nz-table` adds a hidden measure row, so match on the owner cell every data row has.
  const dataRows = () =>
    Array.from<HTMLElement>(fixture.nativeElement.querySelectorAll("tbody tr")).filter(
      row => row.querySelector("texera-user-avatar") !== null
    );

  // Stubs the shared confirmation, so no modal opens. `confirmed` stands for the admin confirming it and the
  // termination succeeding.
  const stubConfirm = (confirmed = false) =>
    vi
      .spyOn(TestBed.inject(ComputingUnitActionsService), "confirmAndTerminate")
      .mockImplementation((_cuid, _unit, onTerminated) => {
        if (confirmed) {
          onTerminated?.();
        }
      });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  // Renders the template, so a pipe or directive missing from `imports` (say `date`) fails here,
  // not only in the AOT build.
  it("renders a row per unit", () => {
    vi.mocked(service.listAllComputingUnits).mockReturnValue(of([makeUnit()]));

    fixture.detectChanges();

    const rows = dataRows();
    expect(rows.length).toBe(1);
    expect(rows[0].textContent).toContain("alice");
  });

  describe("loading and polling", () => {
    // Fake timers go in before `ngOnInit`, so the poll's interval is fake too.
    beforeEach(() => {
      vi.useFakeTimers();
      vi.spyOn(TestBed.inject(NzMessageService), "error").mockReturnValue({} as any);
    });
    afterEach(() => vi.useRealTimers());

    const failWith = (message: string) => throwError(() => new HttpErrorResponse({ error: { message }, status: 500 }));
    const shownError = () => TestBed.inject(NzMessageService).error;

    it("loads every unit on init and clears the loading flag", () => {
      const units = [makeUnit(), makeUnit(withCu({ cuid: 2 }))];
      vi.mocked(service.listAllComputingUnits).mockReturnValue(of(units));

      component.ngOnInit();

      expect(component.computingUnits).toEqual(units);
      expect(component.isLoading).toBe(false);
    });

    it("shows the loading spinner while the first load is pending and hides it once it answers", () => {
      const first = new Subject<DashboardWorkflowComputingUnit[]>();
      vi.mocked(service.listAllComputingUnits).mockReturnValue(first);
      const spinner = () => fixture.nativeElement.querySelector(".loading-container");

      fixture.detectChanges();
      vi.advanceTimersByTime(1);
      fixture.detectChanges();
      expect(spinner()).not.toBeNull();

      first.next([makeUnit()]);
      first.complete();
      fixture.detectChanges();
      vi.advanceTimersByTime(1);
      fixture.detectChanges();
      expect(spinner()).toBeNull();
    });

    it("clears the loading flag and shows a message when the first load fails", () => {
      vi.mocked(service.listAllComputingUnits).mockReturnValue(failWith("boom"));

      component.ngOnInit();

      expect(component.isLoading).toBe(false);
      expect(shownError()).toHaveBeenCalledWith("boom");
    });

    it("refreshes on each tick, and a failed poll keeps the last data and does not stop the polling", () => {
      const first = [makeUnit()];
      const second = [makeUnit(withCu({ cuid: 2 }))];
      vi.mocked(service.listAllComputingUnits)
        .mockReturnValueOnce(of(first))
        .mockReturnValueOnce(failWith("boom"))
        .mockReturnValueOnce(of(second));

      component.ngOnInit();
      expect(component.computingUnits).toEqual(first);

      vi.advanceTimersByTime(5000);
      expect(shownError()).toHaveBeenCalledWith("boom");
      expect(component.computingUnits).toEqual(first);

      vi.advanceTimersByTime(5000);
      expect(component.computingUnits).toEqual(second);
    });

    it("toasts once while polling keeps failing, and again when it fails after a recovery", () => {
      vi.mocked(service.listAllComputingUnits)
        .mockReturnValueOnce(failWith("first"))
        .mockReturnValueOnce(failWith("second"))
        .mockReturnValueOnce(of([makeUnit()]))
        .mockReturnValueOnce(failWith("third"));

      component.ngOnInit();
      expect(shownError()).toHaveBeenCalledTimes(1);
      expect(shownError()).toHaveBeenLastCalledWith("first");

      // Still failing: no second toast.
      vi.advanceTimersByTime(5000);
      expect(shownError()).toHaveBeenCalledTimes(1);

      // Recovers, then fails again: the next failure is announced afresh.
      vi.advanceTimersByTime(5000);
      expect(shownError()).toHaveBeenCalledTimes(1);
      vi.advanceTimersByTime(5000);
      expect(shownError()).toHaveBeenCalledTimes(2);
      expect(shownError()).toHaveBeenLastCalledWith("third");
    });

    describe("stale-data warning", () => {
      const warning = () => fixture.nativeElement.querySelector("nz-alert") as HTMLElement | null;
      const tickAndRender = () => {
        vi.advanceTimersByTime(5000);
        fixture.detectChanges();
      };

      it("appears with the time of the last good load while a refresh fails, and clears on recovery", () => {
        vi.setSystemTime(new Date(2026, 9, 10, 15, 4, 5));
        vi.mocked(service.listAllComputingUnits)
          .mockReturnValueOnce(of([makeUnit()]))
          .mockReturnValueOnce(failWith("boom"))
          .mockReturnValueOnce(failWith("boom"))
          .mockReturnValueOnce(of([makeUnit()]));

        fixture.detectChanges();
        expect(warning()).toBeNull();

        // The warning stays up for the whole outage, even after the toast has gone.
        tickAndRender();
        expect(warning()?.textContent).toContain("Showing data from 3:04:05");
        tickAndRender();
        expect(warning()?.textContent).toContain("Showing data from 3:04:05");

        tickAndRender();
        expect(warning()).toBeNull();
      });

      it("says the units could not be loaded when the first load fails, rather than showing an empty table as is", () => {
        vi.mocked(service.listAllComputingUnits).mockReturnValue(failWith("boom"));

        fixture.detectChanges();
        vi.advanceTimersByTime(1);
        fixture.detectChanges();

        expect(warning()?.textContent).toContain("Could not load computing units");
        expect(warning()?.textContent).not.toContain("Showing data from");
      });
    });

    // A poll that started before a termination can answer after it, with a list that still has the unit.
    it("does not bring a terminated row back when a poll that was in flight answers", () => {
      const first = makeUnit();
      const second = makeUnit(withCu({ cuid: 2 }));
      const inFlight = new Subject<DashboardWorkflowComputingUnit[]>();
      vi.mocked(service.listAllComputingUnits)
        .mockReturnValueOnce(of([first, second]))
        .mockReturnValueOnce(inFlight);
      stubConfirm(true);
      const cuids = () => component.computingUnits.map(u => u.computingUnit.cuid);

      component.ngOnInit();
      // The next poll is now waiting on the backend.
      vi.advanceTimersByTime(5000);
      component.terminate(first);
      expect(cuids()).toEqual([2]);

      inFlight.next([first, second]);
      inFlight.complete();
      expect(cuids()).toEqual([2]);
    });

    // `switchMap` would cancel the slow response on every tick, so the table would never refresh.
    it("lets a response slower than the interval finish instead of cancelling it", () => {
      const requests: Subject<DashboardWorkflowComputingUnit[]>[] = [];
      vi.mocked(service.listAllComputingUnits).mockImplementation(() => {
        requests.push(new Subject());
        return requests[requests.length - 1];
      });

      component.ngOnInit();
      vi.advanceTimersByTime(15000);
      // Three ticks passed with the first request pending, and none added a request.
      expect(requests).toHaveLength(1);

      const units = [makeUnit()];
      requests[0].next(units);
      requests[0].complete();
      expect(component.computingUnits).toEqual(units);
    });

    it("stops polling once the component is destroyed", () => {
      component.ngOnInit();
      fixture.destroy();
      vi.advanceTimersByTime(15000);

      expect(service.listAllComputingUnits).toHaveBeenCalledTimes(1);
    });
  });

  it("labels a unit whose owner has no name as Unknown", () => {
    vi.mocked(service.listAllComputingUnits).mockReturnValue(of([makeUnit({ ownerName: "" })]));

    fixture.detectChanges();

    expect(fixture.nativeElement.querySelector(".owner-cell")?.textContent).toContain("Unknown");
  });

  const specDetail = () => fixture.nativeElement.querySelector("dl.spec-detail") as HTMLElement | null;
  const expander = () => fixture.nativeElement.querySelector("button.ant-table-row-expand-icon") as HTMLElement | null;

  it("shows every resource spec when a row is expanded, with the NaN placeholder as a dash", () => {
    const unit = makeUnit(withResource({ jvmMemorySize: "NaN" }));
    vi.mocked(service.listAllComputingUnits).mockReturnValue(of([unit]));
    component.expandedCuids.add(unit.computingUnit.cuid);

    fixture.detectChanges();

    const pairs = Array.from(specDetail()!.querySelectorAll("div")).map(d => [
      d.querySelector("dt")?.textContent,
      d.querySelector("dd")?.textContent?.trim(),
    ]);
    expect(pairs).toEqual([
      ["CPU", "2"],
      ["Memory", "4Gi"],
      ["GPU", "0"],
      ["JVM Memory", "—"],
      ["Shared Memory", "64Mi"],
    ]);
  });

  // Clicks the real expander, so the `nzExpandChange` wiring is covered. Local units have no limits and get no
  // expander.
  it("expands and collapses a Kubernetes row through its expander, and gives a local row none", () => {
    vi.mocked(service.listAllComputingUnits).mockReturnValue(of([makeUnit()]));
    fixture.detectChanges();

    expect(specDetail()).toBeNull();
    expander()!.click();
    fixture.detectChanges();
    expect(specDetail()).not.toBeNull();
    expander()!.click();
    fixture.detectChanges();
    expect(specDetail()).toBeNull();

    component.computingUnits = [localUnit()];
    fixture.detectChanges();
    expect(expander()).toBeNull();
  });

  // A poll replaces every row object, so the expanded state is keyed by `cuid`, not held on the row.
  it("keeps a row expanded when a poll replaces the rows with fresh objects", () => {
    vi.mocked(service.listAllComputingUnits).mockReturnValue(of([makeUnit()]));
    fixture.detectChanges();
    expander()!.click();
    fixture.detectChanges();

    component.computingUnits = [makeUnit()];
    fixture.detectChanges();

    expect(specDetail()).not.toBeNull();
  });

  describe("terminating a unit", () => {
    // The row's only `nz-button`: the expander is a plain button.
    const terminateButtons = () =>
      Array.from<HTMLButtonElement>(fixture.nativeElement.querySelectorAll("button[nz-button]"));
    const first = makeUnit();
    const second = makeUnit(withCu({ cuid: 2, name: "second" }));

    it("gives every row a terminate button, labelled for its unit type", () => {
      vi.mocked(service.listAllComputingUnits).mockReturnValue(of([first, localUnit(2)]));

      fixture.detectChanges();

      expect(terminateButtons().map(b => b.getAttribute("aria-label"))).toEqual([
        "Terminate this computing unit",
        "Disconnect from this computing unit",
      ]);
    });

    it("asks to confirm the termination of the unit on the clicked row", () => {
      const confirm = stubConfirm();
      vi.mocked(service.listAllComputingUnits).mockReturnValue(of([first, second]));
      fixture.detectChanges();

      terminateButtons()[1].click();

      expect(confirm).toHaveBeenCalledTimes(1);
      expect(confirm).toHaveBeenCalledWith(2, second, expect.any(Function));
    });

    it("drops only that row once the termination succeeds, without waiting for the next poll", () => {
      stubConfirm(true);
      vi.mocked(service.listAllComputingUnits).mockReturnValue(of([first, second]));
      fixture.detectChanges();

      terminateButtons()[0].click();
      fixture.detectChanges();

      expect(component.computingUnits.map(u => u.computingUnit.cuid)).toEqual([2]);
      expect(dataRows()).toHaveLength(1);
    });

    // The modal can be cancelled and the request can fail, and in both the unit lives on.
    it("keeps the row until the termination has succeeded", () => {
      stubConfirm();
      vi.mocked(service.listAllComputingUnits).mockReturnValue(of([first, second]));
      fixture.detectChanges();

      terminateButtons()[0].click();
      fixture.detectChanges();

      expect(dataRows()).toHaveLength(2);
    });

    // The actions column is the seventh after the expander, and the detail row must span all of them.
    it("spans the expanded detail row across every column, the actions one included", () => {
      vi.mocked(service.listAllComputingUnits).mockReturnValue(of([first]));
      component.expandedCuids.add(first.computingUnit.cuid);

      fixture.detectChanges();

      const detailCell = specDetail()!.closest("td")!;
      expect(detailCell.getAttribute("colspan")).toBe("7");
    });
  });

  describe("resourceSummary", () => {
    it("joins CPU, memory and GPU with a middot and labels", () => {
      const unit = makeUnit(withResource({ gpuLimit: "1" }));
      expect(component.resourceSummary(unit)).toBe("2 CPU · 4Gi · 1 GPU");
    });

    it("omits GPU when there is none", () => {
      expect(component.resourceSummary(makeUnit())).toBe("2 CPU · 4Gi");
    });

    it("falls back to a dash when a non-local unit reports no usable spec", () => {
      const blank = makeUnit(withCu({ resource: { ...NAN_RESOURCE, memoryLimit: "" } }));
      expect(component.resourceSummary(blank)).toBe("—");
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

  describe("client-side sort and filter", () => {
    it("sorts by name", () => {
      const a = makeUnit(withCu({ name: "a" }));
      const b = makeUnit(withCu({ name: "b" }));
      expect(component.sortByName(a, b)).toBeLessThan(0);
      expect(component.sortByName(b, a)).toBeGreaterThan(0);
    });

    it("sorts by creation time numerically", () => {
      const older = makeUnit(withCu({ creationTime: 1 }));
      const newer = makeUnit(withCu({ creationTime: 2 }));
      expect(component.sortByCreated(older, newer)).toBeLessThan(0);
    });

    it("sorts a missing name or owner as empty instead of throwing", () => {
      const noName = makeUnit(withCu({ name: undefined as unknown as string }));
      const noOwner = makeUnit({ ownerName: undefined as unknown as string });
      expect(component.sortByName(noName, makeUnit())).toBeLessThan(0);
      expect(component.sortByName(makeUnit(), noName)).toBeGreaterThan(0);
      expect(component.sortByOwner(noOwner, makeUnit())).toBeLessThan(0);
      expect(component.sortByOwner(makeUnit(), noOwner)).toBeGreaterThan(0);
    });

    it("sorts by owner, type and status", () => {
      expect(component.sortByOwner(makeUnit({ ownerName: "a" }), makeUnit({ ownerName: "b" }))).toBeLessThan(0);
      expect(component.sortByType(makeUnit(), localUnit())).toBeLessThan(0);
      expect(component.sortByStatus(makeUnit({ status: "Failed" }), makeUnit({ status: "Running" }))).toBeLessThan(0);
    });

    it("filters by type", () => {
      expect(component.filterByType(["local"], localUnit())).toBe(true);
      expect(component.filterByType(["local"], makeUnit())).toBe(false);
    });

    // Failed units are what an admin opens this page to reclaim, so every status must be selectable.
    it("filters by each offered status, and does not offer the no-unit sentinel", () => {
      const offered = component.statusFilters.map(f => f.value);
      expect(offered).toContain("Failed");
      expect(offered).not.toContain(ComputingUnitState.NoComputingUnit);
      for (const status of offered) {
        const unit = makeUnit({ status: status as DashboardWorkflowComputingUnit["status"] });
        expect(component.filterByStatus([status], unit)).toBe(true);
        expect(component.filterByStatus(["not-a-status"], unit)).toBe(false);
      }
    });
  });
});
