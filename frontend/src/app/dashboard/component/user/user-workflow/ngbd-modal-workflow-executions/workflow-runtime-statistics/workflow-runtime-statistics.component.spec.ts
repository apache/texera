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
import type { Mock } from "vitest";
import * as Plotly from "plotly.js-basic-dist-min";
import { WorkflowRuntimeStatisticsComponent } from "./workflow-runtime-statistics.component";
import { WorkflowRuntimeStatistics } from "../../../../../type/workflow-runtime-statistics";
import { commonTestProviders } from "../../../../../../common/testing/test-utils";

// jsdom has no plotting engine; mock the module so the component's rendering call
// becomes an observable spy and the exact dataset/layout it builds can be asserted.
vi.mock("plotly.js-basic-dist-min", () => ({ newPlot: vi.fn() }));

const newPlotMock = Plotly.newPlot as unknown as Mock;

// The shape createDataset produces and hands to Plotly.newPlot as its second argument.
type Series = { x: number[]; y: number[]; mode: string; name: string };

const NANOS = 1_000_000_000;

describe("WorkflowRuntimeStatisticsComponent", () => {
  let fixture: ComponentFixture<WorkflowRuntimeStatisticsComponent>;
  let component: WorkflowRuntimeStatisticsComponent;
  let warnSpy: Mock;

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

  // The dataset (2nd arg) passed to newPlot on a given call.
  function datasetOfCall(callIndex: number): Series[] {
    return newPlotMock.mock.calls[callIndex][1] as Series[];
  }

  function seriesNamed(dataset: Series[], name: string): Series {
    const found = dataset.find(s => s.name === name);
    expect(found).toBeDefined();
    return found as Series;
  }

  beforeEach(() => {
    newPlotMock.mockClear();
    warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {}) as unknown as Mock;
  });

  afterEach(() => {
    warnSpy.mockRestore();
    fixture?.destroy();
  });

  it("should create", async () => {
    await createFixture({ workflowRuntimeStatistics: validStats() });
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it("ngOnInit groups by operatorId and plots once with the chart id, a non-empty dataset, and the metric-0 layout", async () => {
    await createFixture({ workflowRuntimeStatistics: validStats() });
    fixture.detectChanges(); // triggers ngOnInit -> createChart(0)

    expect(newPlotMock).toHaveBeenCalledTimes(1);
    const [chartId, dataset, layout] = newPlotMock.mock.calls[0];
    expect(chartId).toBe("chart");
    // Two distinct operatorIds => two grouped series.
    expect((dataset as Series[]).length).toBe(2);
    expect((dataset as Series[]).length).toBeGreaterThan(0);
    // Metric index 0 selects "Input Tuple Count" for the layout titles.
    expect((layout as any).title.text).toBe("Input Tuple Count");
    expect((layout as any).xaxis.title.text).toBe("Time (s)");
    expect((layout as any).yaxis.title.text).toBe("Input Tuple Count");
  });

  it("ngOnInit warns and does not plot when workflowRuntimeStatistics is undefined", async () => {
    await createFixture({ workflowRuntimeStatistics: undefined });
    fixture.detectChanges();

    expect(warnSpy).toHaveBeenCalledWith("No workflow runtime statistics available.");
    expect(newPlotMock).not.toHaveBeenCalled();
  });

  it("groupStatisticsByOperatorId converts ns->s, makes timestamps relative, and groups repeated operatorIds", async () => {
    await createFixture({ workflowRuntimeStatistics: validStats() });
    fixture.detectChanges();
    // Switch to metric index 4 (Total Data Processing Time) to observe the ns->s conversion.
    component.onTabChanged(4);

    const dataset = datasetOfCall(newPlotMock.mock.calls.length - 1);
    const scan = seriesNamed(dataset, "scan-111111");

    // Two stats for scan-op-111111 collapsed under one series.
    expect(scan.y.length).toBe(2);
    // totalDataProcessingTime (2e9, 5e9 ns) divided by 1e9 => seconds.
    expect(scan.y).toEqual([2, 5]);
    // x = (timestamp - initialTimestamp) / 1000 => (0/1000, 2000/1000).
    expect(scan.x).toEqual([0, 2]);

    const filter = seriesNamed(dataset, "filter-222222");
    expect(filter.y).toEqual([1]);
    // filter stat timestamp 2000 - initial 1000 = 1000; /1000 => 1.
    expect(filter.x).toEqual([1]);
  });

  it("groupStatisticsByOperatorId skips stats missing an operatorId", async () => {
    await createFixture({
      workflowRuntimeStatistics: [
        makeStat({ operatorId: "scan-op-111111", timestamp: 1000, inputTupleCount: 10 }),
        makeStat({ operatorId: "", timestamp: 2000, inputTupleCount: 999 }),
        makeStat({ operatorId: "scan-op-111111", timestamp: 3000, inputTupleCount: 20 }),
      ],
    });
    fixture.detectChanges();

    expect(warnSpy).toHaveBeenCalledWith("Missing operatorId in statistic:", expect.anything());
    const dataset = datasetOfCall(0);
    // Only the scan series survives; the operatorId-less stat contributed nothing.
    expect(dataset.length).toBe(1);
    const scan = seriesNamed(dataset, "scan-111111");
    expect(scan.y).toEqual([10, 20]);
  });

  it("createDataset removes sink operators and names series '<operatorName>-<last6ofId>'", async () => {
    await createFixture({
      workflowRuntimeStatistics: [
        makeStat({ operatorId: "aggregate-op-abcdef", timestamp: 1000, inputTupleCount: 7 }),
        makeStat({ operatorId: "sink-op-999999", timestamp: 1000, inputTupleCount: 1234 }),
      ],
    });
    fixture.detectChanges();

    const dataset = datasetOfCall(0);
    // The sink operator is dropped, leaving only the aggregate series.
    expect(dataset.length).toBe(1);
    expect(dataset.map(s => s.name)).not.toContain("sink-999999");
    // Name = first "-" segment + last 6 chars of the full id.
    expect(dataset[0].name).toBe("aggregate-abcdef");
    expect(dataset[0].y).toEqual([7]);
  });

  it("onTabChanged re-plots with metric-specific y-values (input tuple count vs number of workers)", async () => {
    await createFixture({ workflowRuntimeStatistics: validStats() });
    fixture.detectChanges(); // metric index 0 (Input Tuple Count)
    const inputCounts = seriesNamed(datasetOfCall(0), "scan-111111").y;
    expect(inputCounts).toEqual([10, 20]);

    component.onTabChanged(7); // metric index 7 (Number of Workers)
    expect(newPlotMock).toHaveBeenCalledTimes(2);
    const workers = seriesNamed(datasetOfCall(1), "scan-111111").y;
    expect(workers).toEqual([1, 2]);
    // The two metrics genuinely differ for the same series.
    expect(workers).not.toEqual(inputCounts);
    // Layout title tracks the newly-selected metric.
    expect((newPlotMock.mock.calls[1][2] as any).title.text).toBe("Number of Workers");
  });

  it("createChart warns and does not plot when the dataset is empty (only a sink operator)", async () => {
    await createFixture({
      workflowRuntimeStatistics: [makeStat({ operatorId: "sink-op-999999", timestamp: 1000, inputTupleCount: 42 })],
    });
    fixture.detectChanges();

    expect(newPlotMock).not.toHaveBeenCalled();
    expect(warnSpy).toHaveBeenCalledWith("No data available for the chart.");
  });

  it("createChart warns twice and does not plot when the statistics array is empty", async () => {
    await createFixture({ workflowRuntimeStatistics: [] });
    fixture.detectChanges();

    expect(newPlotMock).not.toHaveBeenCalled();
    // groupStatisticsByOperatorId warns about the empty input, then createChart warns about the empty dataset.
    expect(warnSpy).toHaveBeenCalledWith("No workflow runtime statistics available.");
    expect(warnSpy).toHaveBeenCalledWith("No data available for the chart.");
  });
});
