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
import { ActivatedRoute } from "@angular/router";
import { NZ_MODAL_DATA, NzModalModule, NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import type { ModalOptions } from "ng-zorro-antd/modal";
import { config, of, throwError } from "rxjs";
import * as Plotly from "plotly.js-basic-dist-min";
import Fuse from "fuse.js";

import { WorkflowExecutionHistoryComponent } from "./workflow-execution-history.component";
import { WorkflowRuntimeStatisticsComponent } from "./workflow-runtime-statistics/workflow-runtime-statistics.component";
import { WorkflowExecutionsService } from "../../../../service/user/workflow-executions/workflow-executions.service";
import { WorkflowExecutionsEntry } from "../../../../type/workflow-executions-entry";
import { WorkflowRuntimeStatistics } from "../../../../type/workflow-runtime-statistics";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { UserService } from "../../../../../common/service/user/user.service";
import { StubUserService } from "../../../../../common/service/user/stub-user.service";
import { OperatorMetadataService } from "../../../../../workspace/service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../../../workspace/service/operator-metadata/stub-operator-metadata.service";
import { commonTestProviders } from "../../../../../common/testing/test-utils";

// Plotly draws onto real canvas/WebGL surfaces jsdom does not provide; the component
// only ever calls newPlot, so replace the module wholesale (same precedent as
// menu.component.spec.ts mocking file-saver).
vi.mock("plotly.js-basic-dist-min", () => ({ newPlot: vi.fn() }));

function makeEntry(overrides: Partial<WorkflowExecutionsEntry> = {}): WorkflowExecutionsEntry {
  return {
    eId: 1,
    vId: 1,
    cuId: 1,
    sId: 0,
    userName: "alice",
    googleAvatar: "",
    name: "untitled",
    startingTime: 0,
    completionTime: 60000,
    status: 3,
    result: "",
    bookmarked: false,
    logLocation: "",
    ...overrides,
  };
}

/** 3 executions: alice runs #1 (Running, 1 min) and #3 (Completed, 3 min), bob runs #2 (Completed, 2 min). */
function makeDefaultEntries(): WorkflowExecutionsEntry[] {
  return [
    makeEntry({
      eId: 1,
      name: "twitter analysis",
      userName: "alice",
      status: 1,
      startingTime: 0,
      completionTime: 60000,
      cuId: 3,
    }),
    makeEntry({
      eId: 2,
      name: "reddit crawl",
      userName: "bob",
      status: 3,
      startingTime: 1000,
      completionTime: 121000,
      cuId: 1,
      bookmarked: true,
    }),
    makeEntry({
      eId: 3,
      name: "Untitled Execution",
      userName: "alice",
      status: 3,
      startingTime: 2000,
      completionTime: 182000,
      cuId: 2,
    }),
  ];
}

function fuseResults(...items: WorkflowExecutionsEntry[]): Fuse.FuseResult<WorkflowExecutionsEntry>[] {
  return items.map((item, refIndex) => ({ item, refIndex }));
}

/**
 * RxJS reports errors from subscriptions without an error callback asynchronously
 * via config.onUnhandledError (rename's subscribe(next).add(finalizer) is such a
 * case); swallow the report deterministically so it cannot fail an unrelated test.
 */
function withRxjsUnhandledErrorsSuppressed(run: () => void): void {
  const previousHandler = config.onUnhandledError;
  config.onUnhandledError = () => {};
  vi.useFakeTimers();
  try {
    run();
    vi.runAllTimers();
  } finally {
    vi.useRealTimers();
    config.onUnhandledError = previousHandler;
  }
}

describe("WorkflowExecutionHistoryComponent", () => {
  let component: WorkflowExecutionHistoryComponent;
  let fixture: ComponentFixture<WorkflowExecutionHistoryComponent>;
  let entries: WorkflowExecutionsEntry[];
  let executionsService: {
    retrieveWorkflowExecutions: ReturnType<typeof vi.fn>;
    groupSetIsBookmarked: ReturnType<typeof vi.fn>;
    groupDeleteWorkflowExecutions: ReturnType<typeof vi.fn>;
    updateWorkflowExecutionsName: ReturnType<typeof vi.fn>;
    retrieveWorkflowRuntimeStatistics: ReturnType<typeof vi.fn>;
  };
  let notificationService: { error: ReturnType<typeof vi.fn> };

  interface SetupOptions {
    entries?: WorkflowExecutionsEntry[];
    /** pass null to omit modal data (exercise the @Optional() route fallback) */
    modalData?: { wid: number } | null;
    routeParams?: Record<string, unknown>;
  }

  async function setup(options: SetupOptions = {}): Promise<void> {
    entries = options.entries ?? makeDefaultEntries();
    executionsService = {
      retrieveWorkflowExecutions: vi.fn().mockReturnValue(of(entries)),
      groupSetIsBookmarked: vi.fn().mockReturnValue(of({})),
      groupDeleteWorkflowExecutions: vi.fn().mockReturnValue(of({})),
      updateWorkflowExecutionsName: vi.fn().mockReturnValue(of({})),
      retrieveWorkflowRuntimeStatistics: vi.fn().mockReturnValue(of([])),
    };
    notificationService = { error: vi.fn() };

    await TestBed.configureTestingModule({
      imports: [WorkflowExecutionHistoryComponent, HttpClientTestingModule, NzModalModule],
      providers: [
        { provide: WorkflowExecutionsService, useValue: executionsService },
        { provide: NotificationService, useValue: notificationService },
        // WorkflowActionService is injected (though unused by the component logic);
        // the stub metadata service lets the real one instantiate, as in menu.component.spec.ts.
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        // UserService backs the <texera-user-avatar> child in each table row.
        { provide: UserService, useClass: StubUserService },
        { provide: NZ_MODAL_DATA, useValue: options.modalData === null ? null : options.modalData ?? { wid: 1 } },
        { provide: ActivatedRoute, useValue: { snapshot: { params: options.routeParams ?? {} } } },
        ...commonTestProviders,
      ],
    }).compileComponents();

    vi.mocked(Plotly.newPlot).mockClear();
    fixture = TestBed.createComponent(WorkflowExecutionHistoryComponent);
    component = fixture.componentInstance;
    // first detectChanges runs ngOnInit (table load) + ngAfterViewInit (charts)
    fixture.detectChanges();
  }

  afterEach(() => {
    fixture?.destroy();
  });

  describe("initialization and wid resolution", () => {
    it("takes the wid from NZ_MODAL_DATA and loads that workflow's executions", async () => {
      await setup();

      expect(component.wid).toBe(1);
      expect(executionsService.retrieveWorkflowExecutions).toHaveBeenCalledWith(1);
      expect(component.allExecutionEntries).toEqual(entries);
      expect(component.paginatedExecutionEntries.map(e => e.eId)).toEqual([1, 2, 3]);
      expect(component.workflowExecutionsDisplayedList).toEqual(component.paginatedExecutionEntries);
    });

    it("falls back to the route snapshot id when modal data is absent", async () => {
      await setup({ modalData: null, routeParams: { id: 7 } });

      expect(component.wid).toBe(7);
      expect(executionsService.retrieveWorkflowExecutions).toHaveBeenCalledWith(7);
    });

    it("defaults the wid to 0 when neither modal data nor route param exists", async () => {
      await setup({ modalData: null });

      expect(component.wid).toBe(0);
    });
  });

  describe("charts (ngAfterViewInit)", () => {
    it("draws a username pie, a status pie, and a process-time bar chart", async () => {
      await setup();

      const newPlot = vi.mocked(Plotly.newPlot);
      expect(newPlot).toHaveBeenCalledTimes(3);

      const [usernameChartId, usernameData, usernameLayout] = newPlot.mock.calls[0];
      expect(usernameChartId).toBe("#execution-userName-pie-chart");
      const usernamePie = (usernameData as unknown as Array<{ labels: string[]; values: number[]; type: string }>)[0];
      expect(usernamePie.type).toBe("pie");
      expect(usernamePie.labels).toEqual(["alice", "bob"]);
      expect(usernamePie.values).toEqual([2, 1]);
      expect(usernameLayout).toEqual(
        expect.objectContaining({ width: 450, height: 450, title: { text: "Users who ran the execution" } })
      );

      const [statusChartId, statusData] = newPlot.mock.calls[1];
      expect(statusChartId).toBe("#execution-status-pie-chart");
      const statusPie = (statusData as unknown as Array<{ labels: string[]; values: number[] }>)[0];
      expect(statusPie.labels).toEqual(["Running", "Completed"]);
      expect(statusPie.values).toEqual([1, 2]);

      const [barChartId, barData, barLayout] = newPlot.mock.calls[2];
      expect(barChartId).toBe("#execution-average-process-time-bar-chart");
      const bar = (barData as unknown as Array<{ x: string[]; y: number[]; type: string }>)[0];
      expect(bar.type).toBe("bar");
      // ceil(3 rows / divider 10) = 1-row buckets; process times are 1, 2, 3 minutes
      expect(bar.x).toEqual(["1~1", "2~2", "3~3"]);
      expect(bar.y).toEqual([1, 2, 3]);
      expect(barLayout).toEqual(expect.objectContaining({ width: 600, height: 600 }));
    });

    it("buckets 20 rows into ceil(20/10)=2-row groups keyed by position and averages minutes", async () => {
      await setup();
      // row k takes k minutes: startingTime 0, completionTime k * 60000
      const rows = Array.from({ length: 20 }, (_, i) =>
        makeEntry({ eId: i + 1, startingTime: 0, completionTime: (i + 1) * 60000 })
      );

      const buckets = component.getBarChartProcessTimeData(rows);

      expect(Object.keys(buckets)).toEqual([
        "1~2",
        "3~4",
        "5~6",
        "7~8",
        "9~10",
        "11~12",
        "13~14",
        "15~16",
        "17~18",
        "19~20",
      ]);
      expect(buckets["1~2"]).toBe(1.5); // avg(1, 2)
      expect(buckets["19~20"]).toBe(19.5); // avg(19, 20)
    });
  });

  describe("getExecutionStatus", () => {
    beforeEach(async () => {
      await setup();
    });

    it("maps every status code to its [label, icon, color] triple", () => {
      expect(component.getExecutionStatus(0)).toEqual(["Initializing", "sync", "#a6bd37"]);
      expect(component.getExecutionStatus(1)).toEqual(["Running", "play-circle", "orange"]);
      expect(component.getExecutionStatus(2)).toEqual(["Paused", "pause-circle", "magenta"]);
      expect(component.getExecutionStatus(3)).toEqual(["Completed", "check-circle", "green"]);
      expect(component.getExecutionStatus(4)).toEqual(["Failed", "exclamation-circle", "gray"]);
      expect(component.getExecutionStatus(5)).toEqual(["Killed", "minus-circle", "red"]);
    });

    it("falls back to a question-circle for unknown codes", () => {
      expect(component.getExecutionStatus(99)).toEqual(["", "question-circle", "gray"]);
    });
  });

  describe("sorting", () => {
    beforeEach(async () => {
      await setup();
    });

    it("sorts by Name (ID) case-insensitively in both directions", () => {
      component.ascSort("Name (ID)");
      expect(component.workflowExecutionsDisplayedList!.map(e => e.name)).toEqual([
        "reddit crawl",
        "twitter analysis",
        "Untitled Execution",
      ]);

      component.dscSort("Name (ID)");
      expect(component.workflowExecutionsDisplayedList!.map(e => e.name)).toEqual([
        "Untitled Execution",
        "twitter analysis",
        "reddit crawl",
      ]);
    });

    it("sorts by Username in both directions", () => {
      component.ascSort("Username");
      expect(component.workflowExecutionsDisplayedList!.map(e => e.userName)).toEqual(["alice", "alice", "bob"]);

      component.dscSort("Username");
      expect(component.workflowExecutionsDisplayedList!.map(e => e.userName)).toEqual(["bob", "alice", "alice"]);
    });

    it("sorts by Execution Start Time in both directions", () => {
      component.dscSort("Execution Start Time");
      expect(component.workflowExecutionsDisplayedList!.map(e => e.eId)).toEqual([3, 2, 1]);

      component.ascSort("Execution Start Time");
      expect(component.workflowExecutionsDisplayedList!.map(e => e.eId)).toEqual([1, 2, 3]);
    });

    it("sorts by Execution Completion Time in both directions", () => {
      component.dscSort("Execution Completion Time");
      expect(component.workflowExecutionsDisplayedList!.map(e => e.eId)).toEqual([3, 2, 1]);

      component.ascSort("Execution Completion Time");
      expect(component.workflowExecutionsDisplayedList!.map(e => e.eId)).toEqual([1, 2, 3]);
    });

    it("sorts by Computing Unit ID numerically in both directions", () => {
      component.ascSort("Computing Unit ID");
      expect(component.workflowExecutionsDisplayedList!.map(e => e.cuId)).toEqual([1, 2, 3]);

      component.dscSort("Computing Unit ID");
      expect(component.workflowExecutionsDisplayedList!.map(e => e.cuId)).toEqual([3, 2, 1]);
    });

    it("onHit alternates between descending and ascending on repeated clicks", () => {
      const ascSpy = vi.spyOn(component, "ascSort");
      const dscSpy = vi.spyOn(component, "dscSort");

      expect(component.showORhide[2]).toBe(false);
      component.onHit("Name (ID)", 2);
      expect(dscSpy).toHaveBeenCalledWith("Name (ID)");
      expect(ascSpy).not.toHaveBeenCalled();
      expect(component.showORhide[2]).toBe(true);

      component.onHit("Name (ID)", 2);
      expect(ascSpy).toHaveBeenCalledWith("Name (ID)");
      expect(component.showORhide[2]).toBe(false);
    });
  });

  describe("pagination", () => {
    const twentyFive = () =>
      Array.from({ length: 25 }, (_, i) => makeEntry({ eId: i + 1, name: `run ${i + 1}`, startingTime: i }));

    it("shows the first page of 10 by default and re-slices on page index change", async () => {
      await setup({ entries: twentyFive() });

      expect(component.paginatedExecutionEntries.map(e => e.eId)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

      component.onPageIndexChange(2);
      expect(component.currentPageIndex).toBe(2);
      expect(component.paginatedExecutionEntries.map(e => e.eId)).toEqual([11, 12, 13, 14, 15, 16, 17, 18, 19, 20]);
      expect(component.workflowExecutionsDisplayedList).toEqual(component.paginatedExecutionEntries);
    });

    it("re-slices with the new size on page size change (index is kept)", async () => {
      await setup({ entries: twentyFive() });
      component.onPageIndexChange(2);

      component.onPageSizeChange(5);

      expect(component.pageSize).toBe(5);
      expect(component.paginatedExecutionEntries.map(e => e.eId)).toEqual([6, 7, 8, 9, 10]);
    });

    it("clears the search box whenever the page is re-sliced", async () => {
      await setup({ entries: twentyFive() });
      component.executionSearchValue = "user:alice";

      component.onPageIndexChange(2);

      expect(component.executionSearchValue).toBe("");
    });
  });

  describe("row selection", () => {
    beforeEach(async () => {
      await setup();
    });

    it("updateEidSet and updateRowSet add on check and delete on uncheck", () => {
      const row = component.paginatedExecutionEntries[0];

      component.updateEidSet(row.eId, true);
      component.updateRowSet(row, true);
      expect(component.setOfEid.has(1)).toBe(true);
      expect(component.setOfExecution.has(row)).toBe(true);

      component.updateEidSet(row.eId, false);
      component.updateRowSet(row, false);
      expect(component.setOfEid.size).toBe(0);
      expect(component.setOfExecution.size).toBe(0);
    });

    it("onAllChecked(true) selects every paginated row and sets the header checkbox", () => {
      component.onAllChecked(true);

      expect(component.setOfEid).toEqual(new Set([1, 2, 3]));
      expect(component.setOfExecution.size).toBe(3);
      expect(component.checked).toBe(true);

      component.onAllChecked(false);
      expect(component.setOfEid.size).toBe(0);
      expect(component.checked).toBe(false);
    });

    it("onItemChecked leaves the header checkbox unchecked until all rows are selected", () => {
      component.onItemChecked(component.paginatedExecutionEntries[0], true);
      expect(component.checked).toBe(false);

      component.onItemChecked(component.paginatedExecutionEntries[1], true);
      component.onItemChecked(component.paginatedExecutionEntries[2], true);
      expect(component.checked).toBe(true);
    });
  });

  describe("bookmarking", () => {
    beforeEach(async () => {
      await setup();
    });

    it("onBookmarkToggle flips the flag and sends the previous value to the service", () => {
      const row = component.paginatedExecutionEntries[0]; // bookmarked: false

      component.onBookmarkToggle(row);

      expect(row.bookmarked).toBe(true);
      expect(executionsService.groupSetIsBookmarked).toHaveBeenCalledWith(1, [1], false);
    });

    it("onBookmarkToggle reverts the flag when the service errors", () => {
      executionsService.groupSetIsBookmarked.mockReturnValue(throwError(() => new Error("boom")));
      const row = component.paginatedExecutionEntries[0];

      component.onBookmarkToggle(row);

      expect(row.bookmarked).toBe(false);
    });

    it("setBookmarked bookmarks everything when the selection is mixed", () => {
      const unbookmarked = component.paginatedExecutionEntries[0];
      const bookmarked = component.paginatedExecutionEntries[1];
      component.setOfExecution = new Set([unbookmarked, bookmarked]);
      component.setOfEid = new Set([unbookmarked.eId, bookmarked.eId]);

      component.setBookmarked();

      expect(unbookmarked.bookmarked).toBe(true);
      expect(bookmarked.bookmarked).toBe(true);
      expect(executionsService.groupSetIsBookmarked).toHaveBeenCalledWith(1, [1, 2], false);
    });

    it("setBookmarked un-bookmarks everything when all selected rows are bookmarked", () => {
      component.paginatedExecutionEntries.forEach(e => (e.bookmarked = true));
      component.setOfExecution = new Set(component.paginatedExecutionEntries);
      component.setOfEid = new Set(component.paginatedExecutionEntries.map(e => e.eId));

      component.setBookmarked();

      component.paginatedExecutionEntries.forEach(e => expect(e.bookmarked).toBe(false));
      expect(executionsService.groupSetIsBookmarked).toHaveBeenCalledWith(1, [1, 2, 3], true);
    });
  });

  describe("deletion", () => {
    it("onDelete removes the row and refreshes the pagination", async () => {
      await setup();
      const row = component.paginatedExecutionEntries[0];

      component.onDelete(row);

      expect(executionsService.groupDeleteWorkflowExecutions).toHaveBeenCalledWith(1, [1]);
      expect(component.allExecutionEntries.map(e => e.eId)).toEqual([2, 3]);
      expect(component.workflowExecutionsDisplayedList!.map(e => e.eId)).toEqual([2, 3]);
    });

    it("onGroupDelete removes all selected rows and clears both selection sets", async () => {
      await setup();
      component.onItemChecked(component.paginatedExecutionEntries[0], true);
      component.onItemChecked(component.paginatedExecutionEntries[2], true);

      component.onGroupDelete();

      expect(executionsService.groupDeleteWorkflowExecutions).toHaveBeenCalledWith(1, [1, 3]);
      expect(component.allExecutionEntries.map(e => e.eId)).toEqual([2]);
      expect(component.setOfEid.size).toBe(0);
      expect(component.setOfExecution.size).toBe(0);
    });

    it("deleting the only row on page 2 moves back to page 1", async () => {
      const eleven = Array.from({ length: 11 }, (_, i) => makeEntry({ eId: i + 1 }));
      await setup({ entries: eleven });
      component.onPageIndexChange(2);
      expect(component.paginatedExecutionEntries.map(e => e.eId)).toEqual([11]);

      component.onDelete(component.paginatedExecutionEntries[0]);

      expect(component.currentPageIndex).toBe(1);
      expect(component.paginatedExecutionEntries).toHaveLength(10);
    });
  });

  describe("renaming", () => {
    beforeEach(async () => {
      await setup();
    });

    it("skips the API call and just closes the editor when the name is unchanged", () => {
      const row = component.workflowExecutionsDisplayedList![0];
      component.workflowExecutionsIsEditingName = [0, 2];

      component.confirmUpdateWorkflowExecutionsCustomName(row, row.name, 0);

      expect(executionsService.updateWorkflowExecutionsName).not.toHaveBeenCalled();
      expect(component.workflowExecutionsIsEditingName).toEqual([2]);
    });

    it("persists a changed name, updates all three lists, and resets the fuse collection", () => {
      const row = component.workflowExecutionsDisplayedList![1]; // eId 2
      const setCollectionSpy = vi.spyOn(component.fuse, "setCollection");
      component.workflowExecutionsIsEditingName = [1];

      component.confirmUpdateWorkflowExecutionsCustomName(row, "renamed run", 1);

      expect(executionsService.updateWorkflowExecutionsName).toHaveBeenCalledWith(1, 2, "renamed run");
      expect(component.allExecutionEntries[1].name).toBe("renamed run");
      expect(component.paginatedExecutionEntries[1].name).toBe("renamed run");
      expect(component.workflowExecutionsDisplayedList![1].name).toBe("renamed run");
      expect(setCollectionSpy).toHaveBeenCalledWith(component.paginatedExecutionEntries);
      expect(component.workflowExecutionsIsEditingName).toEqual([]);
    });

    it("clears the editing flag even when the rename request errors", () => {
      executionsService.updateWorkflowExecutionsName.mockReturnValue(throwError(() => new Error("boom")));
      const row = component.workflowExecutionsDisplayedList![0];
      component.workflowExecutionsIsEditingName = [0];

      withRxjsUnhandledErrorsSuppressed(() => {
        component.confirmUpdateWorkflowExecutionsCustomName(row, "other name", 0);
      });

      expect(row.name).toBe("twitter analysis"); // not renamed
      expect(component.workflowExecutionsIsEditingName).toEqual([]);
    });
  });

  describe("searchExecution", () => {
    beforeEach(async () => {
      await setup();
    });

    it("restores the full page when the search box is blank", () => {
      const searchSpy = vi.spyOn(component.fuse, "search");
      component.workflowExecutionsDisplayedList = [];
      component.executionSearchValue = "   ";

      component.searchExecution();

      expect(component.workflowExecutionsDisplayedList).toEqual(component.paginatedExecutionEntries);
      expect(searchSpy).not.toHaveBeenCalled();
    });

    it("translates status:running into a fuse $and query with the mapped code", () => {
      const searchSpy = vi.spyOn(component.fuse, "search").mockReturnValue(fuseResults(entries[0]));
      component.executionSearchValue = "status:running";

      component.searchExecution();

      expect(searchSpy).toHaveBeenCalledWith({ $and: [{ $path: ["status"], $val: "1" }] });
      expect(component.workflowExecutionsDisplayedList).toEqual([entries[0]]);
    });

    it("combines multiple conditions into one $and query", () => {
      const searchSpy = vi.spyOn(component.fuse, "search").mockReturnValue(fuseResults());
      component.executionSearchValue = "user:alice status:completed";

      component.searchExecution();

      expect(searchSpy).toHaveBeenCalledWith({
        $and: [
          { $path: ["userName"], $val: "alice" },
          { $path: ["status"], $val: "3" },
        ],
      });
    });

    it("treats a bare token as an execution-name search", () => {
      const searchSpy = vi.spyOn(component.fuse, "search").mockReturnValue(fuseResults(entries[2]));
      component.executionSearchValue = "Untitled";

      component.searchExecution();

      expect(searchSpy).toHaveBeenCalledWith({ $and: [{ $path: ["name"], $val: "Untitled" }] });
    });

    it("rejects a malformed token with more than one colon", () => {
      vi.spyOn(component.fuse, "search").mockReturnValue(fuseResults());
      component.executionSearchValue = "a:b:c";

      component.searchExecution();

      expect(notificationService.error).toHaveBeenCalledWith("Please check the format of the search query");
    });

    it("rejects an unknown search field", () => {
      vi.spyOn(component.fuse, "search").mockReturnValue(fuseResults());
      component.executionSearchValue = "foo:x";

      component.searchExecution();

      expect(notificationService.error).toHaveBeenCalledWith("Cannot search by foo");
    });

    it("rejects a status value that is not a known execution status", () => {
      vi.spyOn(component.fuse, "search").mockReturnValue(fuseResults());
      component.executionSearchValue = "status:sleeping";

      component.searchExecution();

      expect(notificationService.error).toHaveBeenCalledWith("Status sleeping is not available to execution");
    });
  });

  describe("searchInputOnChange autocomplete", () => {
    beforeEach(async () => {
      await setup();
    });

    it("suggests matching statuses with the field prefix preserved", () => {
      component.searchInputOnChange("status:run");

      expect(component.filteredExecutionInfo).toEqual(["status:running"]);
    });

    it("suggests execution names for free text", () => {
      component.searchInputOnChange("twi");

      expect(component.filteredExecutionInfo).toEqual(["twitter analysis"]);
    });

    it("deduplicates suggestions across rows with the same value", () => {
      // alice appears on two rows but must be suggested once
      component.searchInputOnChange("user:ali");

      expect(component.filteredExecutionInfo).toEqual(["user:alice"]);
    });
  });

  describe("runtime statistics modal", () => {
    it("fetches the stats then opens the runtime-statistics modal with them", async () => {
      await setup();
      const stats = [{ operatorId: "op-1" }] as unknown as WorkflowRuntimeStatistics[];
      executionsService.retrieveWorkflowRuntimeStatistics.mockReturnValue(of(stats));
      const modalService = TestBed.inject(NzModalService);
      const modalRef = {} as NzModalRef;
      const createSpy = vi.spyOn(modalService, "create").mockReturnValue(modalRef);

      component.showRuntimeStatistics(2, 5);

      expect(executionsService.retrieveWorkflowRuntimeStatistics).toHaveBeenCalledWith(1, 2, 5);
      expect(createSpy).toHaveBeenCalledTimes(1);
      const options = createSpy.mock.calls[0][0] as ModalOptions;
      expect(options.nzContent).toBe(WorkflowRuntimeStatisticsComponent);
      expect(options.nzData).toEqual({ workflowRuntimeStatistics: stats });
      expect(component.modalRef).toBe(modalRef);
    });
  });

  describe("display helpers", () => {
    beforeEach(async () => {
      await setup();
    });

    it("abbreviate cuts execution names at 20 characters and usernames at 5", () => {
      expect(component.abbreviate("averylongexecutionname", true)).toBe("averylongexecutionna");
      expect(component.abbreviate("short", true)).toBe("short");
      expect(component.abbreviate("alexander", false)).toBe("alexa");
      expect(component.abbreviate("bob", false)).toBe("bob");
    });

    it("setAvatarColor returns a stable rgba color per user", () => {
      const first = component.setAvatarColor("alice");

      expect(first).toMatch(/^rgba\(\d+,\d+,\d+,0\.8\)$/);
      expect(component.setAvatarColor("alice")).toBe(first);
    });
  });

  describe("template rendering", () => {
    beforeEach(async () => {
      await setup();
    });

    it("renders one table row per execution with its name and id", () => {
      const labels = fixture.nativeElement.querySelectorAll("label.execution-description");

      expect(labels).toHaveLength(3);
      expect(labels[0].textContent).toContain("twitter analysis (1)");
      expect(labels[1].textContent).toContain("reddit crawl (2)");
    });

    it("colors the status icon from the status mapping", () => {
      const statusIcons = fixture.nativeElement.querySelectorAll("i.status-icon") as NodeListOf<HTMLElement>;

      expect(statusIcons).toHaveLength(3);
      expect(statusIcons[0].style.color).toBe("orange"); // Running
      expect(statusIcons[1].style.color).toBe("green"); // Completed
    });

    it("swaps the name label for an input while the row is being renamed", () => {
      expect(fixture.nativeElement.querySelector("input[placeholder='twitter analysis']")).toBeNull();

      component.workflowExecutionsIsEditingName.push(0);
      fixture.detectChanges();

      expect(fixture.nativeElement.querySelector("input[placeholder='twitter analysis']")).toBeTruthy();
    });

    it("shows the group bookmark/delete actions only when rows are selected", () => {
      expect(fixture.nativeElement.querySelectorAll(".ant-card-actions i")).toHaveLength(0);

      component.setOfEid.add(1);
      fixture.detectChanges();

      expect(fixture.nativeElement.querySelectorAll(".ant-card-actions i")).toHaveLength(2);
    });
  });
});
