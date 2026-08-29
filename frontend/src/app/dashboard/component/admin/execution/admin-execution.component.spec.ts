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
import { NzTableComponent, NzTableQueryParams, NzThAddOnComponent } from "ng-zorro-antd/table";
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

    it("getStatusColor maps the remaining statuses", () => {
      expect(component.getStatusColor("READY")).toBe("lightgreen");
      expect(component.getStatusColor("PAUSED")).toBe("purple");
      expect(component.getStatusColor("FAILED")).toBe("gray");
      expect(component.getStatusColor("JUST COMPLETED")).toBe("blue");
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

    it("dataCheck flags execution-name and workflow-name changes when the status is unchanged", () => {
      const base = makeExecution({ executionStatus: "RUNNING", executionName: "e1", workflowName: "w1" });

      const renamedExecution = makeExecution({ executionStatus: "RUNNING", executionName: "e2", workflowName: "w1" });
      expect(component.dataCheck(base, renamedExecution)).toBe(true);

      const renamedWorkflow = makeExecution({ executionStatus: "RUNNING", executionName: "e1", workflowName: "w2" });
      expect(component.dataCheck(base, renamedWorkflow)).toBe(true);
    });

    it("dataCheck returns false when status, execution name and workflow name are all unchanged", () => {
      const base = makeExecution({ executionStatus: "RUNNING", executionName: "e1", workflowName: "w1" });
      const identical = makeExecution({ executionStatus: "RUNNING", executionName: "e1", workflowName: "w1" });

      expect(component.dataCheck(base, identical)).toBe(false);
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

    it("onSortChange maps a descend order to the 'desc' direction", () => {
      vi.mocked(service.getExecutionList).mockClear();

      component.onSortChange("workflowName", "descend");

      expect(component.sortField).toBe("workflowName");
      expect(component.sortDirection).toBe("desc");
      expect(service.getExecutionList).toHaveBeenCalledTimes(1);
    });

    it("onSortChange is a no-op when a non-active field is cleared", () => {
      component.sortField = "executionName";
      component.sortDirection = "asc";
      vi.mocked(service.getExecutionList).mockClear();

      // Clearing (sortOrder null) a field that is not the active sort field neither
      // resets the sort nor refetches.
      component.onSortChange("workflowName", null);

      expect(component.sortField).toBe("executionName");
      expect(component.sortDirection).toBe("asc");
      expect(service.getExecutionList).not.toHaveBeenCalled();
    });

    it("filterByStatus matches only executions whose status contains a selected value", () => {
      const running = makeExecution({ executionStatus: "RUNNING" });

      expect(component.filterByStatus(["RUN"], running)).toBe(true);
      expect(component.filterByStatus(["FAILED"], running)).toBe(false);
      expect(component.filterByStatus(["FAILED", "RUNNING"], running)).toBe(true);
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

  describe("lifecycle polling", () => {
    // The clock tick and background-refresh intervals are hard-coded in the component
    // (1s and 5s respectively); mirror them here to drive the fake timers.
    const TICK_MS = 1000;
    const REFRESH_MS = 5000;

    it("ngOnInit loads the current page and starts the 1s clock tick", () => {
      const firstExec = makeExecution({ workflowId: 5 });
      vi.mocked(service.getExecutionList).mockReturnValue(of([firstExec]));
      vi.mocked(service.getTotalWorkflows).mockReturnValue(of(2));
      const updateSpy = vi.spyOn(component, "updateTimeStatus");

      component.ngOnInit();

      // The initial fetch resolves synchronously (of(...)) and populates the view.
      expect(component.listOfExecutions).toEqual([firstExec]);
      expect(component.totalWorkflows).toBe(2);
      expect(component.isLoading).toBe(false);

      updateSpy.mockClear();
      vi.mocked(service.getExecutionList).mockClear();

      // A clock tick recomputes elapsed time client-side without hitting the service.
      vi.advanceTimersByTime(TICK_MS);

      expect(updateSpy).toHaveBeenCalled();
      expect(service.getExecutionList).not.toHaveBeenCalled();
    });

    it("ngOnInit polls the current page every 5s and leaves the total untouched when it did not change", () => {
      vi.mocked(service.getExecutionList).mockReturnValue(of([makeExecution({ workflowId: 5 })]));
      vi.mocked(service.getTotalWorkflows).mockReturnValue(of(2));

      component.ngOnInit();
      expect(component.totalWorkflows).toBe(2);

      // Next poll returns fresh rows but the same total.
      const polledExec = makeExecution({ workflowId: 9, executionStatus: "RUNNING" });
      vi.mocked(service.getExecutionList).mockClear();
      vi.mocked(service.getExecutionList).mockReturnValue(of([polledExec]));
      vi.mocked(service.getTotalWorkflows).mockReturnValue(of(2));

      vi.advanceTimersByTime(REFRESH_MS);

      expect(service.getExecutionList).toHaveBeenCalledTimes(1);
      expect(component.listOfExecutions).toEqual([polledExec]);
      // The total is unchanged, so applyCurrentPage must not reassign/churn it.
      expect(component.totalWorkflows).toBe(2);
    });

    it("ngOnDestroy clears the clock interval so it stops ticking", () => {
      vi.mocked(service.getExecutionList).mockReturnValue(of([]));
      vi.mocked(service.getTotalWorkflows).mockReturnValue(of(0));
      component.ngOnInit();

      const updateSpy = vi.spyOn(component, "updateTimeStatus");
      component.ngOnDestroy();

      vi.advanceTimersByTime(TICK_MS * 3);

      expect(updateSpy).not.toHaveBeenCalled();
    });
  });
});

describe("AdminExecutionComponent template rendering", () => {
  let component: AdminExecutionComponent;
  let fixture: ComponentFixture<AdminExecutionComponent>;
  let service: AdminExecutionService;

  const makeExecution = (overrides: Partial<Execution> = {}): Execution => ({
    workflowName: "a-very-long-workflow-name-to-truncate",
    workflowId: 11,
    userName: "alice",
    userId: 1,
    executionId: 21,
    executionStatus: "COMPLETED",
    executionTime: 3661_000,
    executionName: "run-1",
    startTime: 1_700_000_000_000,
    endTime: 1_700_000_100_000,
    access: true,
    ...overrides,
  });

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      providers: [AdminExecutionService, ...commonTestProviders],
      imports: [AdminExecutionComponent, HttpClientTestingModule, NzDropDownModule, NzModalModule],
    }).compileComponents();

    fixture = TestBed.createComponent(AdminExecutionComponent);
    component = fixture.componentInstance;
    service = TestBed.inject(AdminExecutionService);
    // Keep the fetches inert; the rows are seeded directly so ngOnInit's pollers have
    // nothing to do (they are covered by the lifecycle tests above).
    vi.spyOn(service, "getExecutionList").mockReturnValue(of([]));
    vi.spyOn(service, "getTotalWorkflows").mockReturnValue(of(0));
  });

  afterEach(() => fixture.destroy());

  const renderRows = (executions: Execution[]): void => {
    // ngOnInit refills listOfExecutions from the service, so supply the rows through the
    // stub rather than assigning them (which the fetch would overwrite).
    vi.mocked(service.getExecutionList).mockReturnValue(of(executions));
    fixture.detectChanges();
    fixture.detectChanges();
  };

  // nz-table renders an internal measure row with empty cells; keep only real data rows.
  const dataRows = () =>
    fixture.debugElement
      .queryAll(By.css("tbody tr"))
      .filter(row => !row.nativeElement.hasAttribute("nz-table-measure-row"));

  const cellsOf = (rowIndex: number): string[] =>
    dataRows()
      [rowIndex].queryAll(By.css("td"))
      .map(td => (td.nativeElement.textContent ?? "").trim());

  // The four action buttons carry no text and no distinguishing attribute: nz-tooltip is a
  // directive input and never reaches the DOM. nz-icon does render [nzType] as an
  // `anticon-<type>` class, so the icon is the only way to tell them apart.
  const actionButton = (icon: string) =>
    fixture.debugElement
      .queryAll(By.css("tbody tr button"))
      .find(btn => (btn.nativeElement as HTMLElement).querySelector(`i.anticon-${icon}`));

  // Whether the control is greyed out. Read in both directions on every row that is rendered:
  // asserting only that the one relevant control is enabled leaves every widening of a
  // [disabled] predicate (and every exchange between two of them) invisible.
  const isDisabled = (icon: string): boolean => {
    const button = actionButton(icon);
    expect(button, `no action button with the ${icon} icon`).toBeTruthy();
    return (button!.nativeElement as HTMLButtonElement).disabled;
  };

  it("renders one row per execution with the truncated name and formatted duration", () => {
    renderRows([makeExecution()]);

    expect(dataRows()).toHaveLength(1);
    const cells = cellsOf(0);
    // Hard literals rather than the component helpers the template itself calls, so the
    // expectations cannot collapse into the production code they are meant to pin. Every column
    // is read: without that, two <td>s can be exchanged without a test noticing.
    expect(cells[0]).toContain("a-very-long-work . . . "); // 37-char name cut to 16
    expect(cells[0]).not.toContain("truncate"); // the tail is really gone
    expect(cells[0]).toContain("(11)");
    expect(cells[1]).toBe("run-1 (21)");
    expect(cells[2]).toBe("alice");
    expect(cells[3]).toBe("COMPLETED");
    // startTime/endTime are 100 s apart, and a finished execution's duration is that fixed span.
    expect(cells[4]).toBe("00:01:40");
  });

  it("renders the Not Available arm when the execution time is negative", () => {
    renderRows([makeExecution({ executionTime: -1, endTime: 0 })]);

    // Per cell rather than on the row's joined text: with both fallbacks firing at once, joined
    // text cannot tell which of the two cells produced the string.
    const cells = cellsOf(0);
    expect(cells[4]).toBe("Not Available");
    expect(cells[5]).toBe("Not Available");
  });

  it("kills the execution with its workflow id when the kill control is clicked", () => {
    const killSpy = vi.spyOn(component, "killExecution").mockImplementation(() => {});
    renderRows([makeExecution({ executionStatus: "RUNNING", workflowId: 42 })]);

    const killButton = actionButton("stop");
    expect(killButton).toBeTruthy();

    killButton!.triggerEventHandler("click", null);

    expect(killSpy).toHaveBeenCalledWith(42);
  });

  it("renders the workflow name as plain text when the admin has no access", () => {
    // Without access the admin must not be handed a link into the workflow editor, but the
    // name and id still have to be readable so the row can be identified. The name is
    // deliberately longer than the 16-character limit: this branch has to truncate too, and a
    // short name would make an untruncated render indistinguishable from a truncated one.
    renderRows([makeExecution({ access: false, workflowId: 77, workflowName: "secret-workflow-name-too-long" })]);

    expect(fixture.debugElement.query(By.css('a[href="/user/workflow/77"]'))).toBeNull();
    expect(cellsOf(0)[0]).toContain("secret-workflow- . . . ");
    expect(cellsOf(0)[0]).not.toContain("name-too-long");
    expect(cellsOf(0)[0]).toContain("(77)");
  });

  it("pauses the execution when the pause control is clicked on a running row", () => {
    const pauseSpy = vi.spyOn(component, "pauseExecution").mockImplementation(() => {});
    renderRows([makeExecution({ executionStatus: "RUNNING", workflowId: 42 })]);

    const pauseButton = actionButton("pause");
    expect(pauseButton).toBeTruthy();
    // A native click (rather than triggerEventHandler) so the [disabled] predicate is part of
    // what is being asserted: a running execution is exactly the case that must be pausable.
    const nativePause = pauseButton!.nativeElement as HTMLButtonElement;
    expect(nativePause.disabled).toBe(false);
    // The other two controls in the same row, in their opposite state: a running execution
    // cannot be resumed, and can still be killed.
    expect(isDisabled("redo")).toBe(true);
    expect(isDisabled("stop")).toBe(false);

    nativePause.click();

    expect(pauseSpy).toHaveBeenCalledWith(42);
  });

  it("resumes the execution when the resume control is clicked on a paused row", () => {
    const resumeSpy = vi.spyOn(component, "resumeExecution").mockImplementation(() => {});
    renderRows([makeExecution({ executionStatus: "PAUSED", workflowId: 43 })]);

    const resumeButton = actionButton("redo");
    expect(resumeButton).toBeTruthy();
    const nativeResume = resumeButton!.nativeElement as HTMLButtonElement;
    expect(nativeResume.disabled).toBe(false);
    // A paused execution cannot be paused again, and is still killable.
    expect(isDisabled("pause")).toBe(true);
    expect(isDisabled("stop")).toBe(false);

    nativeResume.click();

    expect(resumeSpy).toHaveBeenCalledWith(43);
  });

  it("greys out kill, pause and resume once the execution has finished", () => {
    // The kill control fires a WorkflowKillRequest at a live execution; on a run that has
    // already ended there is nothing to kill, pause or resume, so all three must be dead. Only
    // the history control stays live.
    renderRows([makeExecution({ executionStatus: "KILLED", workflowId: 45 })]);

    expect(isDisabled("stop")).toBe(true);
    expect(isDisabled("pause")).toBe(true);
    expect(isDisabled("redo")).toBe(true);
    expect(isDisabled("history")).toBe(false);
  });

  it("paints each row's status cell with that status's colour", () => {
    // The colour is the only cue for a row's state, and getStatusColor's own unit tests say
    // nothing about which value the template feeds it. Two rows with different statuses, so a
    // binding that ignores the status cannot pass by rendering one right colour.
    renderRows([
      makeExecution({ executionStatus: "RUNNING", workflowId: 51 }),
      makeExecution({ executionStatus: "KILLED", workflowId: 52 }),
    ]);

    const statusColour = (rowIndex: number): string =>
      (dataRows()[rowIndex].queryAll(By.css("td"))[3].nativeElement as HTMLElement).style.color;

    expect(statusColour(0)).toBe("orange");
    expect(statusColour(1)).toBe("red");
  });

  it("sorts each column by that column's own server-side key", () => {
    // onSortChange is unit-tested with a key the test supplies, so the key each *header* passes
    // is observed nowhere: four adjacent columns are freely interchangeable without it.
    const sortSpy = vi.spyOn(component, "onSortChange").mockImplementation(() => {});
    renderRows([makeExecution()]);

    // Five headers carry the add-on directive; the Status one filters instead of sorting and so
    // contributes no call.
    fixture.debugElement
      .queryAll(By.directive(NzThAddOnComponent))
      .forEach(header => header.componentInstance.nzSortOrderChange.emit("ascend"));

    expect(sortSpy.mock.calls.map(call => call[0])).toEqual([
      "workflow_name",
      "execution_name",
      "initiator",
      "end_time",
    ]);
  });

  it("hands the page bar the 1-indexed page, the page size and the server-side total", () => {
    // The component tracks the page 0-indexed and the page bar is 1-indexed, and the rows arrive
    // already paged by the server. The pagination suite drives onQueryParamsChange and asserts
    // component fields, so this is the other half of that contract.
    vi.mocked(service.getTotalWorkflows).mockReturnValue(of(65));
    component.pageSize = 5;
    component.currentPageIndex = 4; // 0-indexed page 5
    renderRows([makeExecution()]);

    const table = fixture.debugElement.query(By.directive(NzTableComponent)).componentInstance;
    expect(table.nzPageIndex).toBe(5);
    expect(table.nzPageSize).toBe(5);
    expect(table.nzTotal).toBe(65);
    // Re-paginating server-paged rows would hide all but the first pageSize of each fetch.
    expect(table.nzFrontPagination).toBe(false);
  });

  it("labels each status filter with the status it actually filters for", () => {
    renderRows([makeExecution()]);

    const statusHeader = fixture.debugElement.queryAll(By.directive(NzThAddOnComponent))[3];
    const filters = statusHeader.componentInstance.nzFilters as { text: string; value: string }[];

    // A label that does not match its value filters for a status other than the one clicked.
    expect(filters.map(entry => entry.text)).toEqual(filters.map(entry => entry.value));
    // Every status the table can colour has to be filterable; "UNKNOWN" is offered as well.
    for (const status of ["READY", "RUNNING", "PAUSED", "COMPLETED", "FAILED", "KILLED", "JUST COMPLETED"]) {
      expect(filters.map(entry => entry.value)).toContain(status);
    }
    expect(filters).toHaveLength(8);
  });

  it("opens the execution history for the row's workflow", () => {
    const historySpy = vi.spyOn(component, "clickToViewHistory").mockImplementation(() => {});
    renderRows([makeExecution({ workflowId: 44, workflowName: "wf-44" })]);

    const historyButton = actionButton("history");
    expect(historyButton).toBeTruthy();

    (historyButton!.nativeElement as HTMLButtonElement).click();

    // Both arguments matter: the id selects the workflow, the name only titles the dialog.
    expect(historySpy).toHaveBeenCalledWith(44, "wf-44");
  });

  it("reads Not Available in the end-time cell while the execution is still running", () => {
    // The duration and end-time cells have independent fallbacks, so assert them separately: a
    // still-running execution has a real elapsed time but no end time yet. The test below
    // renders the mirror of this configuration, which is what actually pins the two conditions
    // to their own cells. The clock is fixed because the rendered duration is computed from
    // Date.now() for a RUNNING row.
    const now = 1_700_000_000_000;
    vi.useFakeTimers();
    vi.setSystemTime(now);
    try {
      renderRows([makeExecution({ executionStatus: "RUNNING", startTime: now - 5_000, endTime: 0 })]);

      const cells = cellsOf(0);
      expect(cells[4]).toBe("00:00:05");
      expect(cells[5]).toBe("Not Available");
    } finally {
      vi.useRealTimers();
    }
  });

  it("reads Not Available in the duration cell while still showing a negative-duration run's end time", () => {
    // The mirror of the test above, and the reason the two cells need separate assertions: here
    // the duration falls back and the end time does not. A negative duration is what the
    // duration cell's fallback exists for (the recorded start can be later than the recorded
    // end when the two timestamps come from clocks that disagree).
    const end = 1_700_000_000_000;
    renderRows([makeExecution({ executionStatus: "COMPLETED", startTime: end + 100_000, endTime: end })]);

    const cells = cellsOf(0);
    expect(cells[4]).toBe("Not Available");
    // A real timestamp, not the fallback. Built from the Date API rather than from the
    // component's own converter so the expectation is not the production code under test.
    expect(cells[5]).toBe(new Date(end).toLocaleString("en-US", { timeZoneName: "short" }));
  });
});
