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
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { WorkflowRuntimeStatisticsComponent } from "./workflow-runtime-statistics.component";
import { WorkflowRuntimeStatistics } from "../../../../../type/workflow-runtime-statistics";
import { commonTestProviders } from "../../../../../../common/testing/test-utils";

// Real Plotly renders into a DOM element by id; create one and assert on the
// `data`/`layout` Plotly attaches to that graph div. Module-level mocking of
// plotly.js-basic-dist-min is flaky across the CI matrix (the shared module
// registry means the mock does not always intercept), so the data-shaping logic
// is asserted directly via the (private) grouping/dataset methods, and the
// plotting behavior is asserted against the real graph div.
function chartDiv(): { data?: unknown[]; layout?: { title?: { text?: string }; xaxis?: unknown; yaxis?: unknown } } {
  document.getElementById("chart")?.remove(); // avoid duplicate ids across reruns/retries
  const div = document.createElement("div");
  div.id = "chart";
  document.body.appendChild(div);
  return div as unknown as { data?: unknown[]; layout?: { title?: { text?: string } } };
}

// The shape createDataset produces and hands to Plotly.newPlot as its second argument.
type Series = { x: number[]; y: number[]; mode: string; name: string };

const NANOS = 1_000_000_000;

describe("WorkflowRuntimeStatisticsComponent", () => {
  let fixture: ComponentFixture<WorkflowRuntimeStatisticsComponent>;
  let component: WorkflowRuntimeStatisticsComponent;
  let warnSpy: ReturnType<typeof vi.spyOn>;

  // Deterministic, fully-populated statistic; only the fields under test are overridden.
  function makeStat(overrides: Partial<WorkflowRuntimeStatistics>): WorkflowRuntimeStatistics {
    return {
      operatorId: "scan-op-111111",
      timestamp: 1000,
      inputTupleCount: 0,
      inputTupleSize: 0,
      outputTupleCount: 0,
      outputTupleSize: 0,
      totalDataProcessingTime: 0,
      totalControlProcessingTime: 0,
      totalIdleTime: 0,
      numberOfWorkers: 1,
      status: 0,
      ...overrides,
    };
  }

  // Two operators, the first appearing twice, so grouping/relative-time behavior is observable.
  // initialTimestamp = 1000 (first stat) => relative timestamps 0, 2000, 1000.
  function validStats(): WorkflowRuntimeStatistics[] {
    return [
      makeStat({
        operatorId: "scan-op-111111",
        timestamp: 1000,
        inputTupleCount: 10,
        totalDataProcessingTime: 2 * NANOS,
        totalControlProcessingTime: 3 * NANOS,
        totalIdleTime: 4 * NANOS,
        numberOfWorkers: 1,
      }),
      makeStat({
        operatorId: "scan-op-111111",
        timestamp: 3000,
        inputTupleCount: 20,
        totalDataProcessingTime: 5 * NANOS,
        numberOfWorkers: 2,
      }),
      makeStat({
        operatorId: "filter-op-222222",
        timestamp: 2000,
        inputTupleCount: 100,
        totalDataProcessingTime: 1 * NANOS,
        numberOfWorkers: 4,
      }),
    ];
  }

  async function createFixture(modalData: { workflowRuntimeStatistics?: WorkflowRuntimeStatistics[] }): Promise<void> {
    await TestBed.configureTestingModule({
      imports: [WorkflowRuntimeStatisticsComponent, NoopAnimationsModule],
      providers: [{ provide: NZ_MODAL_DATA, useValue: modalData }, ...commonTestProviders],
    }).compileComponents();
    fixture = TestBed.createComponent(WorkflowRuntimeStatisticsComponent);
    component = fixture.componentInstance;
  }

  // Calls the private grouping method (its output is otherwise only reachable through Plotly).
  function group(): Record<string, WorkflowRuntimeStatistics[]> {
    return (
      component as unknown as { groupStatisticsByOperatorId(): Record<string, WorkflowRuntimeStatistics[]> }
    ).groupStatisticsByOperatorId();
  }

  // Builds the dataset for a metric index directly off a grouped result.
  function dataset(metricIndex: number, grouped = group()): Series[] {
    (component as unknown as { groupedStatistics?: Record<string, WorkflowRuntimeStatistics[]> }).groupedStatistics =
      grouped;
    return (component as unknown as { createDataset(i: number): Series[] }).createDataset(metricIndex);
  }

  function seriesNamed(data: Series[], name: string): Series {
    const found = data.find(s => s.name === name);
    expect(found).toBeDefined();
    return found as Series;
  }

  beforeEach(() => {
    warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
  });

  afterEach(() => {
    document.getElementById("chart")?.remove();
    vi.restoreAllMocks();
    fixture?.destroy();
  });

  it("should create", async () => {
    await createFixture({ workflowRuntimeStatistics: validStats() });
    chartDiv();
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it("ngOnInit plots the metric-0 dataset and layout onto the #chart div", async () => {
    await createFixture({ workflowRuntimeStatistics: validStats() });
    const gd = chartDiv();
    fixture.detectChanges(); // ngOnInit -> createChart(0)

    // Two distinct operatorIds => two grouped series rendered onto the chart div.
    expect(gd.data).toBeDefined();
    expect((gd.data as Series[]).length).toBe(2);
    // Metric index 0 selects "Input Tuple Count" for the layout titles.
    expect(gd.layout?.title?.text).toBe("Input Tuple Count");
    expect((gd.layout as { xaxis: { title: { text: string } } }).xaxis.title.text).toBe("Time (s)");
    expect((gd.layout as { yaxis: { title: { text: string } } }).yaxis.title.text).toBe("Input Tuple Count");
  });

  it("ngOnInit warns and does not plot when workflowRuntimeStatistics is undefined", async () => {
    await createFixture({ workflowRuntimeStatistics: undefined });
    const gd = chartDiv();
    fixture.detectChanges();

    expect(warnSpy).toHaveBeenCalledWith("No workflow runtime statistics available.");
    expect(gd.data).toBeUndefined();
  });

  it("groupStatisticsByOperatorId converts ns->s, makes timestamps relative, and groups repeated operatorIds", async () => {
    await createFixture({ workflowRuntimeStatistics: validStats() });
    const grouped = group();

    // Two stats for scan-op-111111 collapsed under one group key; one for filter.
    expect(Object.keys(grouped).sort()).toEqual(["filter-op-222222", "scan-op-111111"]);
    const scan = grouped["scan-op-111111"] as Array<WorkflowRuntimeStatistics & { dataProcessingTime: number }>;
    expect(scan.length).toBe(2);
    // totalDataProcessingTime (2e9, 5e9 ns) divided by 1e9 => seconds.
    expect(scan.map(s => s.dataProcessingTime)).toEqual([2, 5]);
    // control/idle also converted ns->s on the first stat (3e9 -> 3, 4e9 -> 4).
    expect((scan[0] as unknown as { controlProcessingTime: number }).controlProcessingTime).toBe(3);
    expect((scan[0] as unknown as { idleTime: number }).idleTime).toBe(4);
    // timestamps made relative to the first stat (1000): 0 and 2000.
    expect(scan.map(s => s.timestamp)).toEqual([0, 2000]);
    // filter stat timestamp 2000 - initial 1000 = 1000.
    expect(grouped["filter-op-222222"][0].timestamp).toBe(1000);
  });

  it("groupStatisticsByOperatorId skips stats missing an operatorId", async () => {
    await createFixture({
      workflowRuntimeStatistics: [
        makeStat({ operatorId: "scan-op-111111", timestamp: 1000, inputTupleCount: 10 }),
        makeStat({ operatorId: "", timestamp: 2000, inputTupleCount: 999 }),
        makeStat({ operatorId: "scan-op-111111", timestamp: 3000, inputTupleCount: 20 }),
      ],
    });
    const grouped = group();

    expect(warnSpy).toHaveBeenCalledWith("Missing operatorId in statistic:", expect.anything());
    // Only the scan group survives; the operatorId-less stat contributed nothing.
    expect(Object.keys(grouped)).toEqual(["scan-op-111111"]);
    expect(grouped["scan-op-111111"].length).toBe(2);
  });

  it("createDataset removes sink operators and names series '<operatorName>-<last6ofId>'", async () => {
    await createFixture({
      workflowRuntimeStatistics: [
        makeStat({ operatorId: "aggregate-op-abcdef", timestamp: 1000, inputTupleCount: 7 }),
        makeStat({ operatorId: "sink-op-999999", timestamp: 1000, inputTupleCount: 1234 }),
      ],
    });
    const data = dataset(0);

    // The sink operator is dropped, leaving only the aggregate series.
    expect(data.length).toBe(1);
    expect(data.map(s => s.name)).not.toContain("sink-999999");
    // Name = first "-" segment + last 6 chars of the full id.
    expect(data[0].name).toBe("aggregate-abcdef");
    expect(data[0].y).toEqual([7]);
  });

  it("createDataset selects the metric by index and scales x by 1/1000", async () => {
    await createFixture({ workflowRuntimeStatistics: validStats() });
    const grouped = group();

    // Metric 0 = Input Tuple Count.
    const scanInput = seriesNamed(dataset(0, grouped), "scan-111111");
    expect(scanInput.y).toEqual([10, 20]);
    // x = relative timestamp / 1000 => [0/1000, 2000/1000].
    expect(scanInput.x).toEqual([0, 2]);
    expect(scanInput.mode).toBe("lines");

    // Metric 7 = Number of Workers (a genuinely different series for the same operator).
    const scanWorkers = seriesNamed(dataset(7, grouped), "scan-111111");
    expect(scanWorkers.y).toEqual([1, 2]);
    expect(scanWorkers.y).not.toEqual(scanInput.y);

    // Metric 4 = Total Data Processing Time (ns->s converted during grouping).
    const scanProc = seriesNamed(dataset(4, grouped), "scan-111111");
    expect(scanProc.y).toEqual([2, 5]);
  });

  it("onTabChanged re-plots the newly selected metric onto the #chart div", async () => {
    await createFixture({ workflowRuntimeStatistics: validStats() });
    const gd = chartDiv();
    fixture.detectChanges(); // metric index 0 (Input Tuple Count)
    expect(gd.layout?.title?.text).toBe("Input Tuple Count");

    component.onTabChanged(7); // metric index 7 (Number of Workers)
    // Plotly re-renders the same div; the layout title tracks the new metric.
    expect(gd.layout?.title?.text).toBe("Number of Workers");
    const workers = seriesNamed(gd.data as Series[], "scan-111111");
    expect(workers.y).toEqual([1, 2]);
  });

  it("createChart warns and does not plot when the dataset is empty (only a sink operator)", async () => {
    await createFixture({
      workflowRuntimeStatistics: [makeStat({ operatorId: "sink-op-999999", timestamp: 1000, inputTupleCount: 42 })],
    });
    const gd = chartDiv();
    fixture.detectChanges();

    expect(warnSpy).toHaveBeenCalledWith("No data available for the chart.");
    expect(gd.data).toBeUndefined();
  });

  it("createChart warns twice and does not plot when the statistics array is empty", async () => {
    await createFixture({ workflowRuntimeStatistics: [] });
    const gd = chartDiv();
    fixture.detectChanges();

    // groupStatisticsByOperatorId warns about the empty input, then createChart warns about the empty dataset.
    expect(warnSpy).toHaveBeenCalledWith("No workflow runtime statistics available.");
    expect(warnSpy).toHaveBeenCalledWith("No data available for the chart.");
    expect(gd.data).toBeUndefined();
  });
});
