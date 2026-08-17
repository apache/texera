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
import { UserQuotaComponent } from "./user-quota.component";
import { UserQuotaService } from "../../../service/user/quota/user-quota.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { of } from "rxjs";
import { By } from "@angular/platform-browser";
import type { Mocked } from "vitest";
import { ExecutionQuota, Workflow, WorkflowQuota } from "../../../../common/type/user";
import { DatasetQuota } from "../../../type/quota-statistic.interface";
import { AdminUserService } from "../../../service/admin/user/admin-user.service";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";

// Real Plotly renders into a DOM element by id; create one and assert on the
// `data`/`layout` Plotly attaches to that graph div. (Module-level mocking of
// plotly.js-basic-dist-min was flaky across the CI matrix — the mock did not
// always intercept, letting the real newPlot throw "No DOM element with id".)
function chartDiv(id: string): void {
  document.getElementById(id)?.remove(); // avoid duplicate ids across reruns/retries
  const div = document.createElement("div");
  div.id = id;
  document.body.appendChild(div);
}

// ISO 'YYYY-MM-DD' for a date `days` before now (kept by the 1-year filter for small values).
function isoDaysAgo(days: number): string {
  const d = new Date();
  d.setUTCHours(0, 0, 0, 0);
  d.setUTCDate(d.getUTCDate() - days);
  return d.toISOString().slice(0, 10);
}

// ISO 'YYYY-MM-DD' on `day` of the month `monthsAgo` months before now.
function isoInMonthsAgo(monthsAgo: number, day: number): string {
  const d = new Date();
  d.setUTCHours(0, 0, 0, 0);
  d.setUTCDate(1); // avoid month-length overflow before shifting the month
  d.setUTCMonth(d.getUTCMonth() - monthsAgo);
  d.setUTCDate(day);
  return d.toISOString().slice(0, 10);
}

function execution(eid: number, workflowId: number, result: number, runtime: number, log: number): ExecutionQuota {
  return {
    eid,
    workflowId,
    workflowName: `wf-${workflowId}`,
    resultBytes: result,
    runTimeStatsBytes: runtime,
    logBytes: log,
  };
}

const sumValues = (data: Array<[string, number]>): number => data.reduce((acc, [, v]) => acc + v, 0);

// Fixtures for the Cache Size comparator. The three byte triples are picked so that ordering by
// any single count — or by the sum with any one of the three terms dropped — yields a different
// sequence than the full sum does, and so that no two of the six orderings share a tie.
const SIZE_SORT_BIG = execution(30, 1, 1, 450, 452); // 903
const SIZE_SORT_MIDDLE = execution(10, 1, 800, 1, 1); // 802
const SIZE_SORT_SMALL = execution(20, 1, 2, 700, 2); // 704

describe("UserQuotaComponent", () => {
  let component: UserQuotaComponent;
  let fixture: ComponentFixture<UserQuotaComponent>;
  let mockUserQuotaService: Mocked<UserQuotaService>;

  beforeEach(() => {
    mockUserQuotaService = {
      getCreatedDatasets: vi.fn(),
      getCreatedWorkflows: vi.fn(),
      getAccessWorkflows: vi.fn(),
      getExecutionQuota: vi.fn(),
      deleteExecutionCollection: vi.fn(),
    } as unknown as Mocked<UserQuotaService>;
    mockUserQuotaService.getCreatedDatasets.mockReturnValue(of([]));
    mockUserQuotaService.getCreatedWorkflows.mockReturnValue(of([]));
    mockUserQuotaService.getAccessWorkflows.mockReturnValue(of([]));
    mockUserQuotaService.getExecutionQuota.mockReturnValue(of([]));

    TestBed.configureTestingModule({
      providers: [{ provide: UserQuotaService, useValue: mockUserQuotaService }, ...commonTestProviders],
      imports: [UserQuotaComponent, HttpClientTestingModule],
    });

    fixture = TestBed.createComponent(UserQuotaComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => vi.restoreAllMocks());

  it("should create", () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  /**
   * The mirror of the modal test at the bottom of this file. The constructor's two branches differ
   * in exactly four things — the user id, the two header colours and the fixed height — and only the
   * modal half of each was pinned, so the inline page could be made to look like the modal (or to
   * read someone else's quota) without a failure.
   */
  it("paints the standalone page header and reads the signed-in user's own quota", () => {
    fixture.detectChanges();

    expect(component.userId).toBe(-1); // the sentinel that means "whoever is logged in"
    expect(mockUserQuotaService.getExecutionQuota).toHaveBeenCalledWith(-1);
    expect(component.backgroundColor).toBe("white");
    expect(component.textColor).toBe("Black");
    expect(component.dynamicHeight).toBe(""); // the modal's fixed height must not apply inline

    const card = fixture.nativeElement.querySelector("nz-card") as HTMLElement;
    expect(card.style.background).toBe("white");
    const heading = fixture.nativeElement.querySelector("h2.page-title") as HTMLElement;
    expect(heading.style.color.toLowerCase()).toBe("black");
    const scroller = fixture.nativeElement.querySelector("div") as HTMLElement;
    expect(scroller.style.height).toBe("");
  });

  describe("aggregateByMonth", () => {
    it("sums the values that share a 'YYYY-MM' prefix", () => {
      const result = component.aggregateByMonth([
        ["2024-01-05", 2],
        ["2024-01-20", 3],
        ["2024-02-10", 5],
      ]);
      expect(result).toEqual([
        ["2024-01", 5],
        ["2024-02", 5],
      ]);
    });
  });

  describe("filterOutdatedData", () => {
    it("keeps entries within the last year and drops older ones", () => {
      const recent = isoDaysAgo(30);
      const old = isoDaysAgo(400);
      expect(
        component.filterOutdatedData([
          [recent, 1],
          [old, 2],
        ])
      ).toEqual([[recent, 1]]);
    });
  });

  describe("aggregateData", () => {
    it("returns the (filtered) data unchanged when there are fewer than 8 points", () => {
      const data: Array<[string, number]> = [
        [isoDaysAgo(10), 1],
        [isoDaysAgo(20), 2],
        [isoDaysAgo(30), 3],
      ];
      expect(component.aggregateData(data, 5)).toEqual(data);
    });

    it("aggregates by month when the data spans at least three months", () => {
      const data: Array<[string, number]> = [
        [isoInMonthsAgo(2, 5), 1],
        [isoInMonthsAgo(2, 15), 1],
        [isoInMonthsAgo(1, 5), 1],
        [isoInMonthsAgo(1, 15), 1],
        [isoInMonthsAgo(1, 25), 1],
        [isoInMonthsAgo(0, 3), 1],
        [isoInMonthsAgo(0, 6), 1],
        [isoInMonthsAgo(0, 9), 1],
      ];
      const result = component.aggregateData(data, 5) as Array<[string, number]>;
      expect(result.length).toBe(3); // one bucket per month
      expect(sumValues(result)).toBe(sumValues(data)); // aggregation preserves the total
    });

    it("aggregates by day-group when there are 8+ points within fewer than three months", () => {
      const data: Array<[string, number]> = Array.from({ length: 8 }, (_, i) => [isoInMonthsAgo(0, i + 1), i + 1]);
      const result = component.aggregateData(data, 5) as Array<[string, number]>;
      expect(result.length).toBeGreaterThan(0);
      expect(result.length).toBeLessThan(data.length); // grouping collapses points
      expect(sumValues(result)).toBe(sumValues(data)); // total preserved
    });
  });

  describe("refreshData", () => {
    it("loads datasets, shared workflows and executions, and groups executions by workflow", () => {
      const datasets: DatasetQuota[] = [
        { did: 1, name: "d1", creationTime: Date.now(), size: 100 },
        { did: 2, name: "d2", creationTime: Date.now(), size: 200 },
      ];
      const accessWorkflows = [7, 8, 9]; // three ids, so a dropped assignment cannot look like an empty load
      mockUserQuotaService.getCreatedDatasets.mockReturnValue(of(datasets));
      mockUserQuotaService.getAccessWorkflows.mockReturnValue(of(accessWorkflows));
      mockUserQuotaService.getExecutionQuota.mockReturnValue(
        of([execution(10, 1, 100, 5, 10), execution(11, 1, 50, 5, 5), execution(12, 2, 20, 2, 3)])
      );
      // Chart rendering is exercised separately; stub it here to isolate the data wiring.
      vi.spyOn(component, "generatePieChart").mockImplementation(() => {});
      vi.spyOn(component, "generateLineChart").mockImplementation(() => {});

      component.refreshData();

      expect(component.datasetList).toEqual(datasets);
      expect(component.totalUploadedDatasetCount).toBe(2);
      expect(component.totalUploadedDatasetSize).toBe(300);
      // the "Workflows with Access" box counts these, so losing the assignment silently zeroes it
      expect(component.accessWorkflows).toEqual(accessWorkflows);
      expect(component.totalQuotaSize).toBe(200); // 115 + 60 + 25
      expect(component.workflows.map(w => w.workflowId)).toEqual([1, 2]);
      expect(component.workflows[0].executions.map(e => e.eid)).toEqual([10, 11]);
      expect(component.workflows[1].executions.map(e => e.eid)).toEqual([12]);
    });

    it("counts the created datasets and workflows per calendar day, and charts each into its own div", () => {
      // Derived from the same Date arithmetic the component uses, so the fixture holds in
      // any timezone rather than only in UTC.
      const recent = Date.now();
      const older = recent - 40 * 24 * 60 * 60 * 1000;
      const recentDay = new Date(recent).toLocaleDateString();
      const olderDay = new Date(older).toLocaleDateString();
      expect(recentDay).not.toBe(olderDay); // the fixture has to straddle two days to be meaningful

      const createdWorkflow = (workflowId: number, creationTime: number): Workflow => ({
        userId: 1,
        workflowId,
        workflowName: `wf-${workflowId}`,
        creationTime,
        lastModifiedTime: creationTime,
      });
      // The dataset and workflow subscribes each run their own copy of the same bucketing loop, so
      // both need data — and the two tallies are deliberately *different* ([1, 2] vs [2, 1]) so that
      // routing one series into the other's chart cannot masquerade as the right answer.
      mockUserQuotaService.getCreatedDatasets.mockReturnValue(
        of([
          { did: 1, name: "d1", creationTime: recent, size: 100 },
          { did: 2, name: "d2", creationTime: older, size: 200 },
          { did: 3, name: "d3", creationTime: older, size: 300 },
        ])
      );
      mockUserQuotaService.getCreatedWorkflows.mockReturnValue(
        of([createdWorkflow(1, recent), createdWorkflow(2, recent), createdWorkflow(3, older)])
      );
      // Assert on what the per-day tally hands to the aggregator: that is the raw output of the
      // bucketing loop, unaffected by the grouping/filtering stage tested above.
      const aggregateSpy = vi.spyOn(component, "aggregateData");
      const pieChartSpy = vi.spyOn(component, "generatePieChart").mockImplementation(() => {});
      const lineChartSpy = vi.spyOn(component, "generateLineChart").mockImplementation(() => {});

      component.refreshData();

      expect(aggregateSpy.mock.calls[0][0]).toEqual([
        [recentDay, 1],
        [olderDay, 2], // two datasets share a day and are tallied together
      ]);
      expect(aggregateSpy.mock.calls[1][0]).toEqual([
        [recentDay, 2], // two workflows share a day and are tallied together
        [olderDay, 1],
      ]);

      // The pie series is built in the same loop: name/size pairs, in that order. The three names and
      // the three sizes are all distinct, so a slot swap cannot come out looking the same.
      expect(pieChartSpy.mock.calls[0]).toEqual([
        [
          ["d1", 100],
          ["d2", 200],
          ["d3", 300],
        ],
        "Dataset Size Distribution",
        "sizePieChart",
      ]);

      // Each series has to reach its own graph div: the last argument selects the DOM node the chart
      // is painted into, so getting it wrong blanks one chart and draws twice over the other.
      expect(lineChartSpy.mock.calls[0][3]).toBe("Dataset Upload Overview");
      expect(lineChartSpy.mock.calls[0][4]).toBe("datasetLineChart");
      expect(lineChartSpy.mock.calls[1][3]).toBe("Workflow Upload Overview");
      expect(lineChartSpy.mock.calls[1][4]).toBe("workflowLineChart");
    });

    it("clears the running totals so a reload does not double-count them", () => {
      // refreshData is the reload path, so its resets only do work on the second call — without them
      // the quota total and the workflow panels accumulate every time the page refreshes.
      mockUserQuotaService.getExecutionQuota.mockReturnValue(
        of([execution(10, 1, 100, 5, 10), execution(12, 2, 20, 2, 3)])
      );
      vi.spyOn(component, "generatePieChart").mockImplementation(() => {});
      vi.spyOn(component, "generateLineChart").mockImplementation(() => {});

      component.refreshData();
      const quotaAfterFirst = component.totalQuotaSize;
      const executionsAfterFirst = component.workflows.map(w => w.executions.map(e => e.eid));
      component.refreshData();

      expect(quotaAfterFirst).toBe(140); // 115 + 25
      // Not just the panel count: dropping the `workflows = []` reset keeps the panel count at two
      // and instead files every execution a second time under the workflow it already belongs to.
      expect(executionsAfterFirst).toEqual([[10], [12]]);
      expect(component.totalQuotaSize).toBe(quotaAfterFirst);
      expect(component.workflows.map(w => w.executions.map(e => e.eid))).toEqual(executionsAfterFirst);
    });
  });

  describe("deleteCollection", () => {
    it("removes the execution and subtracts its bytes from the total", () => {
      component.workflows = [
        { workflowId: 1, workflowName: "wf-1", executions: [execution(10, 1, 100, 5, 10), execution(11, 1, 50, 5, 5)] },
      ];
      component.totalQuotaSize = 175; // 115 + 60
      mockUserQuotaService.deleteExecutionCollection.mockReturnValue(of(undefined));

      component.deleteCollection(10);

      expect(mockUserQuotaService.deleteExecutionCollection).toHaveBeenCalledWith(10);
      expect(component.totalQuotaSize).toBe(60);
      expect(component.workflows[0].executions.map(e => e.eid)).toEqual([11]);
    });

    it("drops the workflow when its last execution is removed", () => {
      component.workflows = [{ workflowId: 1, workflowName: "wf-1", executions: [execution(10, 1, 100, 5, 10)] }];
      component.totalQuotaSize = 115;
      mockUserQuotaService.deleteExecutionCollection.mockReturnValue(of(undefined));

      component.deleteCollection(10);

      expect(component.workflows).toEqual([]);
    });

    it("skips the workflows that do not own the deleted execution and still prunes the emptied one", () => {
      component.workflows = [
        { workflowId: 1, workflowName: "wf-1", executions: [execution(10, 1, 100, 5, 10)] },
        { workflowId: 2, workflowName: "wf-2", executions: [execution(20, 2, 7, 3, 1), execution(21, 2, 4, 2, 1)] },
      ];
      component.totalQuotaSize = 133; // 115 for wf-1, 11 + 7 for wf-2
      mockUserQuotaService.deleteExecutionCollection.mockReturnValue(of(undefined));

      component.deleteCollection(10);

      // wf-2 owns nothing with this id, so it must be stepped over rather than reached into —
      // and the sweep has to survive that far for the now-empty wf-1 to be pruned at all.
      expect(component.workflows.map(w => w.workflowId)).toEqual([2]);
      expect(component.workflows[0].executions.map(e => e.eid)).toEqual([20, 21]);
      // wf-2's bytes differ from the deleted execution's, so a wrong subtrahend lands elsewhere.
      expect(component.totalQuotaSize).toBe(18);
    });
  });

  /**
   * The Cache Size column sorts on the sum of the three byte counts. The byte triples below are
   * picked so that ordering by any single count — or by the sum with any one term dropped —
   * produces a different sequence than the full sum does.
   */
  describe("sortBySize", () => {
    // Largest-first is what the comparator returns today. Note that this is inverted relative to the
    // NzTableSortFn contract (`a - b`, which nz-table negates for 'descend'); see the header-click
    // test below, which pins the resulting caret/order mismatch as the user actually sees it.
    it("orders executions by total cache size, largest first", () => {
      const scrambled = [SIZE_SORT_MIDDLE, SIZE_SORT_SMALL, SIZE_SORT_BIG];

      expect([...scrambled].sort(component.sortBySize).map(e => e.eid)).toEqual([30, 10, 20]);
    });
  });

  describe("chart generation", () => {
    it("generatePieChart renders the labels/values and sizing layout onto the target div", () => {
      chartDiv("pieDiv");

      component.generatePieChart(
        [
          ["a", 1],
          ["b", 2],
        ],
        "Pie Title",
        "pieDiv"
      );

      const gd = document.getElementById("pieDiv") as unknown as { data: any[]; layout: any };
      expect(gd.data[0]).toMatchObject({ values: [1, 2], labels: ["a", "b"], type: "pie" });
      expect(gd.layout.width).toBe(component.DEFAULT_PIE_CHART_WIDTH);
      expect(gd.layout.height).toBe(component.DEFAULT_PIE_CHART_HEIGHT);
      expect(gd.layout.title).toMatchObject({ text: "Pie Title" });
    });

    it("generateLineChart renders the x/y series and axis labels onto the target div", () => {
      chartDiv("lineDiv");

      component.generateLineChart(
        [
          ["2024-01-01", 1],
          ["2024-01-02", 3],
        ],
        "X Label",
        "Y Label",
        "Line Title",
        "lineDiv"
      );

      const gd = document.getElementById("lineDiv") as unknown as { data: any[]; layout: any };
      expect(gd.data[0]).toMatchObject({ x: ["2024-01-01", "2024-01-02"], y: [1, 3], type: "scatter" });
      expect(gd.layout.title).toMatchObject({ text: "Line Title" });
      expect(gd.layout.xaxis.title).toMatchObject({ text: "X Label" });
      expect(gd.layout.yaxis.title).toMatchObject({ text: "Y Label" });
      // y spans 3 - 1 = 2, within the narrow-range threshold, so the axis is forced onto whole
      // numbers rather than letting Plotly pick fractional ticks for a series of counts.
      expect(gd.layout.yaxis.tickmode).toBe("linear");
      expect(gd.layout.yaxis.dtick).toBe(1);
    });

    // The other leg of the same pair. On its own an "is undefined" assertion would also hold if the
    // two keys were deleted outright; it is the whole-number assertion in the test above that makes
    // this one mean "the threshold decides", rather than "the keys are never set".
    it("generateLineChart leaves the y-axis tick density to Plotly when the series spans more than five", () => {
      chartDiv("wideLineDiv");

      component.generateLineChart(
        [
          ["2024-01-01", 1],
          ["2024-01-02", 20],
        ],
        "X Label",
        "Y Label",
        "Line Title",
        "wideLineDiv"
      );

      const gd = document.getElementById("wideLineDiv") as unknown as { data: any[]; layout: any };
      expect(gd.layout.yaxis.tickmode).toBeUndefined();
      expect(gd.layout.yaxis.dtick).toBeUndefined();
    });
  });
  /**
   * The Result Cache tab is template-only: the suite above drives the component's data and charts
   * and never renders this tab, so the per-execution row — including the size it reports and the id
   * its delete button carries — was unexercised.
   */
  describe("result cache tab", () => {
    /** Selects a tab by its title and returns the host element. */
    function openTab(title: string): HTMLElement {
      const host = fixture.nativeElement as HTMLElement;
      const tab = Array.from(host.querySelectorAll<HTMLElement>(".ant-tabs-tab")).find(t =>
        (t.textContent || "").includes(title)
      );
      if (!tab) {
        throw new Error(`Result Cache spec: tab not found: ${title}`);
      }
      tab.click();
      fixture.detectChanges();
      return host;
    }

    /** Expands the collapse panel whose header contains the given text. */
    function openPanel(host: HTMLElement, header: string): void {
      const panel = Array.from(host.querySelectorAll<HTMLElement>(".ant-collapse-header")).find(h =>
        (h.textContent || "").includes(header)
      );
      panel!.click();
      fixture.detectChanges();
    }

    /** Renders the Result Cache tab with the given workflows and opens the first panel. */
    function renderCache(workflows: any[], openHeader = "wf-1"): HTMLElement {
      // ngOnInit loads the quota and resets `workflows`, so the first cycle has to run before the
      // fixture data is put in place, or it is wiped before anything renders.
      fixture.detectChanges();
      component.workflows = workflows;
      fixture.detectChanges();
      const host = openTab("Result Cache");
      openPanel(host, openHeader);
      return host;
    }

    const workflow = (workflowId: number, executions: any[]) => ({
      workflowId,
      workflowName: `wf-${workflowId}`,
      executions,
    });

    it("groups the executions under one panel per workflow", () => {
      const host = renderCache([workflow(1, [execution(10, 1, 1, 1, 1)]), workflow(2, [execution(20, 2, 1, 1, 1)])]);

      const headers = Array.from(host.querySelectorAll(".ant-collapse-header")).map(h => h.textContent?.trim());
      expect(headers).toEqual(["wf-1", "wf-2"]);
    });

    it("lists every execution of the opened workflow", () => {
      const host = renderCache([workflow(1, [execution(10, 1, 1, 1, 1), execution(11, 1, 1, 1, 1)])]);

      const eids = Array.from(host.querySelectorAll("tbody tr")).map(r =>
        r.querySelectorAll("td")[1]?.textContent?.trim()
      );
      expect(eids).toEqual(["10", "11"]);
    });

    it("reports the cache size as result plus log plus runtime statistics", () => {
      // Three separate byte counts are added together. Distinct powers of two, so dropping or
      // double-counting any one of them lands on a different total rather than coincidentally
      // matching — which is exactly how a missing term would otherwise go unnoticed.
      const host = renderCache([workflow(1, [execution(10, 1, 1024, 4096, 2048)])]);

      const sizeCell = host.querySelectorAll("tbody tr td")[2];
      expect(sizeCell.textContent?.trim()).toBe(component.formatSize(1024 + 4096 + 2048));
    });

    it("deletes the collection of the row it was pressed on", () => {
      // The row carries the execution id; passing the workflow id would delete the wrong cache.
      const spy = vi.spyOn(component, "deleteCollection").mockImplementation(() => {});
      const host = renderCache([workflow(1, [execution(10, 1, 1, 1, 1), execution(11, 1, 1, 1, 1)])]);

      const secondRowButton = host.querySelectorAll("tbody tr")[1].querySelector("button")!;
      secondRowButton.click();
      // nz-popconfirm defers the action to its confirmation, so trigger that directly.
      fixture.debugElement.queryAll(By.css("button[nz-popconfirm]"))[1].triggerEventHandler("nzOnConfirm", null);

      expect(spy).toHaveBeenCalledWith(11);
    });

    it("pages the executions rather than listing all of them", () => {
      const executions = Array.from({ length: 5 }, (_, i) => execution(100 + i, 1, 1, 1, 1));
      const host = renderCache([workflow(1, executions)]);

      expect(host.querySelectorAll("tbody tr").length).toBe(3);
    });

    it("reorders the rows by total cache size when the Cache Size header is used", async () => {
      // ngOnInit charts before the tab panes exist, so real Plotly rejects with "No DOM element
      // with id ...". The synchronous tests above never give Node a turn to report those
      // rejections; this one awaits a macrotask, so stub the charts out of the way.
      vi.spyOn(component, "generatePieChart").mockImplementation(() => {});
      vi.spyOn(component, "generateLineChart").mockImplementation(() => {});
      const host = renderCache([workflow(1, [SIZE_SORT_MIDDLE, SIZE_SORT_SMALL, SIZE_SORT_BIG])]);
      const eids = () =>
        Array.from(host.querySelectorAll("tbody tr")).map(r => r.querySelectorAll("td")[1]?.textContent?.trim());
      expect(eids()).toEqual(["10", "20", "30"]); // as supplied, before any sort

      const sizeHeader = Array.from(host.querySelectorAll<HTMLElement>("thead th")).find(h =>
        (h.textContent || "").includes("Cache Size")
      )!;
      const caretActive = (direction: "up" | "down") =>
        sizeHeader.querySelector(`.ant-table-column-sorter-${direction}`)!.classList.contains("active");
      const clickSort = async () => {
        sizeHeader.click();
        // nz-table publishes a changed sort operator on a macrotask (`delay(0)`).
        await new Promise(resolve => setTimeout(resolve, 0));
        fixture.detectChanges();
      };

      await clickSort();
      // NOTE: the direction indicator and the row order disagree, and that is recorded here rather
      // than blessed. nzSortDirections defaults to ['ascend', 'descend', null], so the first click
      // selects 'ascend' and lights the up caret — but the rows come out LARGEST first, because
      // sortBySize returns `b - a` while NzTableSortFn expects `a - b` (nz-table negates the result
      // itself for 'descend'). Correcting the comparator is meant to flip both halves below.
      expect(caretActive("up")).toBe(true);
      expect(caretActive("down")).toBe(false);
      expect(eids()).toEqual(["30", "10", "20"]);

      await clickSort();
      expect(caretActive("down")).toBe(true);
      expect(caretActive("up")).toBe(false);
      expect(eids()).toEqual(["20", "10", "30"]);
    });
  });
});

/**
 * `admin-user.component` opens this same component inside a modal and hands it the target user
 * through NZ_MODAL_DATA. That constructor branch needs the token provided, and the suite above
 * has already instantiated its test module, so it gets its own TestBed here.
 */
describe("UserQuotaComponent (opened from the admin user modal)", () => {
  const MODAL_UID = 42;
  let fixture: ComponentFixture<UserQuotaComponent>;
  let component: UserQuotaComponent;
  let adminService: Mocked<AdminUserService>;
  let regularService: Mocked<UserQuotaService>;

  function emptyQuotaService<T>(): Mocked<T> {
    return {
      getCreatedDatasets: vi.fn().mockReturnValue(of([])),
      getCreatedWorkflows: vi.fn().mockReturnValue(of([])),
      getAccessWorkflows: vi.fn().mockReturnValue(of([])),
      getExecutionQuota: vi.fn().mockReturnValue(of([])),
      deleteExecutionCollection: vi.fn().mockReturnValue(of(undefined)),
    } as unknown as Mocked<T>;
  }

  beforeEach(() => {
    adminService = emptyQuotaService<AdminUserService>();
    regularService = emptyQuotaService<UserQuotaService>();

    TestBed.configureTestingModule({
      providers: [
        { provide: AdminUserService, useValue: adminService },
        { provide: UserQuotaService, useValue: regularService },
        { provide: NZ_MODAL_DATA, useValue: { uid: MODAL_UID } },
        ...commonTestProviders,
      ],
      imports: [UserQuotaComponent, HttpClientTestingModule],
    });

    fixture = TestBed.createComponent(UserQuotaComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => vi.restoreAllMocks());

  it("reads the quota of the user named by the modal, through the admin service", () => {
    fixture.detectChanges();

    expect(component.userId).toBe(MODAL_UID);
    expect(adminService.getExecutionQuota).toHaveBeenCalledWith(MODAL_UID);
    expect(adminService.getCreatedWorkflows).toHaveBeenCalledWith(MODAL_UID);
    expect(regularService.getExecutionQuota).not.toHaveBeenCalled();
  });

  it("paints the modal header instead of the standalone page header", () => {
    fixture.detectChanges();

    // The field initializers are "white"/"Black", so both of these differ from the inline mode.
    expect(component.backgroundColor).toBe("lightcoral");
    expect(component.textColor).toBe("white");
    // The third difference: the modal keeps the fixed height the inline page clears.
    expect(component.dynamicHeight).toBe("700px");
    const card = fixture.nativeElement.querySelector("nz-card") as HTMLElement;
    expect(card.style.background).toBe("lightcoral");
    const heading = fixture.nativeElement.querySelector("h2.page-title") as HTMLElement;
    expect(heading.style.color).toBe("white");
    const scroller = fixture.nativeElement.querySelector("div") as HTMLElement;
    expect(scroller.style.height).toBe("700px");
  });
});
