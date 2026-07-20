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

import { ComponentFixture, inject, TestBed } from "@angular/core/testing";
import { By } from "@angular/platform-browser";
import { AdminExecutionComponent } from "./admin-execution.component";
import { AdminExecutionService } from "../../../service/admin/execution/admin-execution.service";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { NzModalModule } from "ng-zorro-antd/modal";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { Execution } from "../../../../common/type/execution";
import { NzTableQueryParams } from "ng-zorro-antd/table";
import { NzModalService } from "ng-zorro-antd/modal";
import { WorkflowWebsocketService } from "../../../../workspace/service/workflow-websocket/workflow-websocket.service";
import { NO_SORT } from "./admin-execution.component";
import { of } from "rxjs";

describe("AdminDashboardComponent", () => {
  let component: AdminExecutionComponent;
  let fixture: ComponentFixture<AdminExecutionComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      providers: [AdminExecutionService, ...commonTestProviders],
      imports: [AdminExecutionComponent, HttpClientTestingModule, NzDropDownModule, NzModalModule],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AdminExecutionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  afterEach(() => fixture.destroy());

  it("should create", inject([HttpTestingController], () => {
    expect(component).toBeTruthy();
  }));

  it("renders the workflow link to /user/workflow/<id> when the admin has access", () => {
    component.listOfExecutions = [
      {
        access: true,
        workflowId: 42,
        workflowName: "demo workflow",
        executionId: 1,
        executionName: "exec",
        userName: "alice",
        executionStatus: "COMPLETED",
      } as unknown as Execution,
    ];
    component.isLoading = false;
    fixture.detectChanges();

    const anchor = fixture.debugElement.query(By.css('a[href="/user/workflow/42"]'));
    expect(anchor).toBeTruthy();
  });
});

describe("AdminExecutionComponent pagination (#3586)", () => {
  let component: AdminExecutionComponent;
  let fixture: ComponentFixture<AdminExecutionComponent>;
  let service: AdminExecutionService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      providers: [AdminExecutionService, ...commonTestProviders],
      imports: [AdminExecutionComponent, HttpClientTestingModule, NzDropDownModule, NzModalModule],
    }).compileComponents();

    fixture = TestBed.createComponent(AdminExecutionComponent);
    component = fixture.componentInstance;
    service = TestBed.inject(AdminExecutionService);
    // Keep any data fetch inert and synchronous so the page-bar state can be asserted directly.
    // Note: we deliberately do NOT call fixture.detectChanges() (no ngOnInit/auto-refresh) and
    // drive onQueryParamsChange() directly, the way ng-zorro's nz-table does on a page click.
    vi.spyOn(service, "getExecutionList").mockReturnValue(of([]));
    vi.spyOn(service, "getTotalWorkflows").mockImplementation(() => of(component.totalWorkflows));
  });

  afterEach(() => fixture.destroy());

  function changeParams(pageSize: number, pageIndex: number): void {
    component.onQueryParamsChange({ pageSize, pageIndex, sort: [], filter: [] } as NzTableQueryParams);
  }

  it("moves to the page the user clicks (page 1 -> page 5)", () => {
    component.totalWorkflows = 65; // 13 pages at size 5
    component.pageSize = 5;
    component.currentPageIndex = 0;

    changeParams(5, 5);

    expect(component.pageSize).toBe(5);
    expect(component.currentPageIndex).toBe(4); // 0-indexed page 5
  });

  it("follows the emitted page index when the page size changes (reset to first page)", () => {
    // On a page-size change ng-zorro emits the new size together with pageIndex=1.
    component.totalWorkflows = 65;
    component.pageSize = 5;
    component.currentPageIndex = 4; // currently on page 5

    changeParams(20, 1);

    expect(component.pageSize).toBe(20);
    expect(component.currentPageIndex).toBe(0); // must follow pageIndex=1, not stay on page 5
  });

  it("syncs both page size and page index from a single event, order-independently", () => {
    component.totalWorkflows = 65;
    component.pageSize = 5;
    component.currentPageIndex = 4;

    changeParams(20, 3); // size and index change together

    expect(component.pageSize).toBe(20);
    expect(component.currentPageIndex).toBe(2); // page 3 at the new size
  });

  it("clamps to the last existing page when a larger page size removes pages", () => {
    component.totalWorkflows = 65; // 2 pages at size 50
    component.pageSize = 5;
    component.currentPageIndex = 12; // last page at size 5

    changeParams(50, 13);

    expect(component.pageSize).toBe(50);
    expect(component.currentPageIndex).toBe(1); // clamp to page 2 (the last page)
  });

  it("handles single-page results (stays on the only page)", () => {
    component.totalWorkflows = 3; // 1 page
    component.pageSize = 5;
    component.currentPageIndex = 0;

    changeParams(5, 1);

    expect(component.currentPageIndex).toBe(0);
  });

  it("clamps to the first page for empty results even if a later page is requested", () => {
    component.totalWorkflows = 0;
    component.pageSize = 5;
    component.currentPageIndex = 0;

    changeParams(5, 5); // requesting page 5 with no data at all

    expect(component.currentPageIndex).toBe(0);
  });

  it("does not refetch when neither page size nor page index changed (sort/filter handled elsewhere)", () => {
    component.totalWorkflows = 65;
    component.pageSize = 5;
    component.currentPageIndex = 4; // page 5
    vi.mocked(service.getExecutionList).mockClear();

    changeParams(5, 5); // page 5 again, same size -> no-op for pagination

    expect(service.getExecutionList).not.toHaveBeenCalled();
  });

  it("fetches exactly once for a real page change", () => {
    component.totalWorkflows = 65;
    component.pageSize = 5;
    component.currentPageIndex = 0;
    vi.mocked(service.getExecutionList).mockClear();

    changeParams(5, 5);

    expect(service.getExecutionList).toHaveBeenCalledTimes(1);
  });
});

describe("AdminExecutionComponent methods (#6550)", () => {
  let component: AdminExecutionComponent;
  let fixture: ComponentFixture<AdminExecutionComponent>;
  let service: AdminExecutionService;

  const NOW = 1_700_000_000_000; // fixed clock (ms) for Date.now()-based logic

  function makeExecution(over: Partial<Execution> = {}): Execution {
    return {
      access: true,
      workflowId: 1,
      workflowName: "wf",
      executionId: 1,
      executionName: "exec",
      userName: "alice",
      executionStatus: "COMPLETED",
      startTime: 0,
      endTime: 0,
      executionTime: 0,
      ...over,
    } as unknown as Execution;
  }

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      providers: [AdminExecutionService, ...commonTestProviders],
      imports: [AdminExecutionComponent, HttpClientTestingModule, NzDropDownModule, NzModalModule],
    }).compileComponents();

    fixture = TestBed.createComponent(AdminExecutionComponent);
    component = fixture.componentInstance;
    service = TestBed.inject(AdminExecutionService);
    // Keep data fetches inert and synchronous. We deliberately do NOT call
    // detectChanges(), so ngOnInit's pollers never start.
    vi.spyOn(service, "getExecutionList").mockReturnValue(of([]));
    vi.spyOn(service, "getTotalWorkflows").mockReturnValue(of(0));
    // Fixed clock makes the Date.now()-based methods deterministic and also keeps the
    // websocket heartbeat interval (created in the action handlers) from ever firing.
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    fixture.destroy();
  });

  describe("pure helpers", () => {
    it("padZero pads single digits to two characters", () => {
      expect(component.padZero(5)).toBe("05");
      expect(component.padZero(12)).toBe("12");
    });

    it("convertSecondsToTime formats seconds as HH:MM:SS", () => {
      expect(component.convertSecondsToTime(0)).toBe("00:00:00");
      expect(component.convertSecondsToTime(45)).toBe("00:00:45");
      expect(component.convertSecondsToTime(3661)).toBe("01:01:01");
    });

    it("maxStringLength truncates only when longer than the limit", () => {
      expect(component.maxStringLength("hello world", 5)).toBe("hello . . . ");
      expect(component.maxStringLength("hi", 5)).toBe("hi");
    });

    it("getStatusColor maps statuses to colors and falls back to black", () => {
      expect(component.getStatusColor("RUNNING")).toBe("orange");
      expect(component.getStatusColor("COMPLETED")).toBe("green");
      expect(component.getStatusColor("KILLED")).toBe("red");
      expect(component.getStatusColor("SOMETHING_ELSE")).toBe("black");
    });

    it("convertTimeToTimestamp renders the timestamp via toLocaleString", () => {
      // Assert against the same locale call so the test is timezone-independent.
      const expected = new Date(NOW).toLocaleString("en-US", { timeZoneName: "short" });
      expect(component.convertTimeToTimestamp("COMPLETED", NOW)).toBe(expected);
    });

    it("calculateTime uses the fixed final duration for a completed execution", () => {
      expect(component.calculateTime(10000, 4000, "COMPLETED", "w")).toBe(6);
    });

    it("calculateTime uses the live elapsed time for a running execution", () => {
      // now = NOW/1000 seconds; start = NOW - 5000 ms -> elapsed 5 s.
      expect(component.calculateTime(0, NOW - 5000, "RUNNING", "w")).toBe(5);
    });
  });

  describe("time status", () => {
    it("specifyCompletedStatus flips COMPLETED to JUST COMPLETED within the 5s window", () => {
      const exec = makeExecution({ executionStatus: "COMPLETED", endTime: NOW - 2000 });
      component.listOfExecutions = [exec];

      component.specifyCompletedStatus();

      expect(exec.executionStatus).toBe("JUST COMPLETED");
    });

    it("specifyCompletedStatus reverts JUST COMPLETED to COMPLETED after 5s", () => {
      const exec = makeExecution({ executionStatus: "JUST COMPLETED", endTime: NOW - 10000 });
      component.listOfExecutions = [exec];

      component.specifyCompletedStatus();

      expect(exec.executionStatus).toBe("COMPLETED");
    });

    it("updateTimeDifferences assigns the elapsed time for each execution", () => {
      const exec = makeExecution({ executionStatus: "COMPLETED", startTime: 4000, endTime: 10000 });
      component.listOfExecutions = [exec];

      component.updateTimeDifferences();

      expect(exec.executionTime).toBe(6);
    });

    it("updateTimeStatus delegates to specifyCompletedStatus and updateTimeDifferences", () => {
      const specify = vi.spyOn(component, "specifyCompletedStatus");
      const diffs = vi.spyOn(component, "updateTimeDifferences");

      component.updateTimeStatus();

      expect(specify).toHaveBeenCalledTimes(1);
      expect(diffs).toHaveBeenCalledTimes(1);
    });

    it("dataCheck flags a status change and ignores a fresh JUST COMPLETED", () => {
      const oldRunning = makeExecution({ executionStatus: "RUNNING" });
      const newCompleted = makeExecution({ executionStatus: "COMPLETED" });
      expect(component.dataCheck(oldRunning, newCompleted)).toBe(true);

      const oldJustCompleted = makeExecution({ executionStatus: "JUST COMPLETED" });
      const newFresh = makeExecution({ executionStatus: "COMPLETED", endTime: NOW - 2000 });
      expect(component.dataCheck(oldJustCompleted, newFresh)).toBe(false);
    });
  });

  describe("data + table", () => {
    it("fetchData populates the list, total and loading flag", () => {
      const exec = makeExecution({ workflowId: 7 });
      vi.mocked(service.getExecutionList).mockReturnValue(of([exec]));
      vi.mocked(service.getTotalWorkflows).mockReturnValue(of(3));

      component.fetchData();

      expect(component.listOfExecutions).toEqual([exec]);
      expect(component.totalWorkflows).toBe(3);
      expect(component.isLoading).toBe(false);
    });

    it("onFilterChange stringifies the filter and refetches with it", () => {
      vi.mocked(service.getExecutionList).mockClear();

      component.onFilterChange(["RUNNING", "COMPLETED"]);

      expect(component.filter).toEqual(["RUNNING", "COMPLETED"]);
      expect(vi.mocked(service.getExecutionList).mock.calls[0][4]).toEqual(["RUNNING", "COMPLETED"]);
    });

    it("onSortChange sets the field/direction and refetches", () => {
      vi.mocked(service.getExecutionList).mockClear();

      component.onSortChange("executionName", "ascend");

      expect(component.sortField).toBe("executionName");
      expect(component.sortDirection).toBe("asc");
      expect(service.getExecutionList).toHaveBeenCalledTimes(1);
    });

    it("onSortChange resets to NO_SORT when the active field is cleared", () => {
      component.sortField = "executionName";
      component.sortDirection = "asc";

      component.onSortChange("executionName", null);

      expect(component.sortField).toBe(NO_SORT);
      expect(component.sortDirection).toBe(NO_SORT);
    });
  });

  describe("execution actions", () => {
    let openWebsocket: ReturnType<typeof vi.fn>;
    let send: ReturnType<typeof vi.fn>;

    beforeEach(() => {
      openWebsocket = vi.spyOn(WorkflowWebsocketService.prototype, "openWebsocket").mockImplementation(() => {});
      send = vi.spyOn(WorkflowWebsocketService.prototype, "send").mockImplementation(() => {});
    });

    it("killExecution opens the socket for the workflow, sends a kill request and refreshes", () => {
      vi.mocked(service.getExecutionList).mockClear();

      component.killExecution(42);

      expect(openWebsocket).toHaveBeenCalledWith(42);
      expect(send).toHaveBeenCalledWith("WorkflowKillRequest", {});
      expect(service.getExecutionList).toHaveBeenCalledTimes(1);
    });

    it("pauseExecution sends a pause request", () => {
      component.pauseExecution(9);

      expect(openWebsocket).toHaveBeenCalledWith(9);
      expect(send).toHaveBeenCalledWith("WorkflowPauseRequest", {});
    });

    it("resumeExecution sends a resume request", () => {
      component.resumeExecution(9);

      expect(openWebsocket).toHaveBeenCalledWith(9);
      expect(send).toHaveBeenCalledWith("WorkflowResumeRequest", {});
    });

    it("clickToViewHistory opens the history modal for the workflow", () => {
      const modal = TestBed.inject(NzModalService);
      const create = vi.spyOn(modal, "create").mockReturnValue({} as ReturnType<NzModalService["create"]>);

      component.clickToViewHistory(7, "My Workflow");

      expect(create).toHaveBeenCalledTimes(1);
      expect(create.mock.calls[0][0]).toMatchObject({
        nzData: { wid: 7 },
        nzTitle: "Execution results of Workflow: My Workflow",
      });
    });
  });
});
