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
import { BehaviorSubject } from "rxjs";
import { HeatmapLegendComponent } from "./heatmap-legend.component";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { WorkflowStatusService } from "../../service/workflow-status/workflow-status.service";
import { OperatorPerformanceMetrics } from "../../service/workflow-status/performance-metrics";
import { HeatmapView } from "../../service/heatmap/heatmap-scoring";

function makeMetrics(overrides: Partial<OperatorPerformanceMetrics>): OperatorPerformanceMetrics {
  return {
    dataProcessingTimeNs: 0,
    controlProcessingTimeNs: 0,
    idleTimeNs: 0,
    inputRows: 0,
    outputRows: 0,
    inputSize: 0,
    outputSize: 0,
    numWorkers: 1,
    ...overrides,
  };
}

describe("HeatmapLegendComponent", () => {
  let fixture: ComponentFixture<HeatmapLegendComponent>;
  let viewSubject: BehaviorSubject<HeatmapView | null>;
  let metricsSubject: BehaviorSubject<Record<string, OperatorPerformanceMetrics>>;

  const legendElement = (): HTMLElement | null => fixture.nativeElement.querySelector(".heatmap-legend");
  const textOf = (selector: string): string =>
    (fixture.nativeElement.querySelector(selector) as HTMLElement).textContent!.trim();
  const labels = (): string[] =>
    Array.from(fixture.nativeElement.querySelectorAll(".heatmap-legend__labels span") as NodeListOf<HTMLElement>).map(
      el => el.textContent!.trim()
    );

  beforeEach(() => {
    viewSubject = new BehaviorSubject<HeatmapView | null>(null);
    metricsSubject = new BehaviorSubject<Record<string, OperatorPerformanceMetrics>>({});

    const workflowActionServiceStub = {
      getJointGraphWrapper: () => ({ getHeatmapViewStream: () => viewSubject.asObservable() }),
    } as unknown as WorkflowActionService;
    const workflowStatusServiceStub = {
      getPerformanceMetricsStream: () => metricsSubject.asObservable(),
    } as unknown as WorkflowStatusService;

    TestBed.configureTestingModule({
      imports: [HeatmapLegendComponent],
      providers: [
        { provide: WorkflowActionService, useValue: workflowActionServiceStub },
        { provide: WorkflowStatusService, useValue: workflowStatusServiceStub },
      ],
    });
    fixture = TestBed.createComponent(HeatmapLegendComponent);
    fixture.detectChanges();
  });

  it("is hidden while the overlay is off (view is null)", () => {
    expect(legendElement()).toBeNull();
  });

  it("shows the active view's title and the metric range once the overlay is on", () => {
    metricsSubject.next({
      fast: makeMetrics({ dataProcessingTimeNs: 5_000_000 }),
      slow: makeMetrics({ dataProcessingTimeNs: 8_620_000_000 }),
    });
    viewSubject.next(HeatmapView.Runtime);
    fixture.detectChanges();

    expect(textOf(".heatmap-legend__title")).toBe("Runtime");
    expect(labels()).toEqual(["5 ms", "8.62 s"]);
  });

  it("shows — for both labels when there are no metrics yet", () => {
    viewSubject.next(HeatmapView.Runtime);
    fixture.detectChanges();

    expect(labels()).toEqual(["—", "—"]);
  });

  it("keeps not-measurable operators out of the range so they don't anchor the low label", () => {
    // The blocking operator (no output yet) has no time-per-row value; without
    // the filter it would read as 0 and pin the min label to "0".
    metricsSubject.next({
      blocking: makeMetrics({ dataProcessingTimeNs: 30_000_000_000, outputRows: 0 }),
      producer: makeMetrics({ dataProcessingTimeNs: 2_000_000_000, outputRows: 4 }),
    });
    viewSubject.next(HeatmapView.TimePerRow);
    fixture.detectChanges();

    // Only the producer is measurable: 0.5 s/row is both min and max.
    expect(labels()).toEqual(["500.0 ms/row", "500.0 ms/row"]);
  });

  it("shows — when no operator is measurable for the view", () => {
    // A lone source has no input rows, so I/O imbalance is undefined for it.
    metricsSubject.next({ source: makeMetrics({ outputRows: 250 }) });
    viewSubject.next(HeatmapView.IoImbalance);
    fixture.detectChanges();

    expect(textOf(".heatmap-legend__title")).toBe("I/O imbalance");
    expect(labels()).toEqual(["—", "—"]);
  });

  it("updates the title and range when the view switches", () => {
    metricsSubject.next({
      op: makeMetrics({ dataProcessingTimeNs: 5_000_000, inputRows: 1_000, outputRows: 250 }),
    });
    viewSubject.next(HeatmapView.Runtime);
    fixture.detectChanges();
    expect(textOf(".heatmap-legend__title")).toBe("Runtime");

    viewSubject.next(HeatmapView.IoImbalance);
    fixture.detectChanges();
    expect(textOf(".heatmap-legend__title")).toBe("I/O imbalance");
    // |250 - 1000| / (250 + 1000) = 0.6, the only operator anchors both ends.
    expect(labels()).toEqual(["0.60", "0.60"]);
  });

  it("hides again when the overlay is turned off", () => {
    viewSubject.next(HeatmapView.Runtime);
    fixture.detectChanges();
    expect(legendElement()).not.toBeNull();

    viewSubject.next(null);
    fixture.detectChanges();
    expect(legendElement()).toBeNull();
  });
});
