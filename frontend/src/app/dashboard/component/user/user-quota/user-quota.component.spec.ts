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
import type { Mocked } from "vitest";
import { ExecutionQuota } from "../../../../common/type/user";
import { DatasetQuota } from "../../../type/quota-statistic.interface";

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
    it("loads datasets and executions, and groups executions by workflow", () => {
      const datasets: DatasetQuota[] = [
        { did: 1, name: "d1", creationTime: Date.now(), size: 100 },
        { did: 2, name: "d2", creationTime: Date.now(), size: 200 },
      ];
      mockUserQuotaService.getCreatedDatasets.mockReturnValue(of(datasets));
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
      expect(component.totalQuotaSize).toBe(200); // 115 + 60 + 25
      expect(component.workflows.map(w => w.workflowId)).toEqual([1, 2]);
      expect(component.workflows[0].executions.map(e => e.eid)).toEqual([10, 11]);
      expect(component.workflows[1].executions.map(e => e.eid)).toEqual([12]);
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
    });
  });
});
