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
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { of, throwError } from "rxjs";
import * as fs from "node:fs";
import * as path from "node:path";
import { MetricsPanelComponent } from "./metrics-panel.component";
import { ObservabilityService } from "../../../../service/user/observability/observability.service";
import { MetricsQueryResponse, NamedMetric } from "../../../../service/user/observability/observability.types";

// The unit-test bundler rewrites `__dirname` to the bundle root, so the
// component source can only be located by its stable path under the
// frontend working directory (cwd is always `frontend/` in CI and locally).
const componentPath = path.resolve(
  process.cwd(),
  "src/app/dashboard/component/user/observability/metrics-panel/metrics-panel.component.ts"
);

describe("MetricsPanelComponent", () => {
  let component: MetricsPanelComponent;
  let fixture: ComponentFixture<MetricsPanelComponent>;
  let mockService: {
    queryMetrics: ReturnType<typeof vi.fn>;
  };

  beforeEach(async () => {
    mockService = { queryMetrics: vi.fn() };
    // Default: return empty points so the component can construct
    // chart options without exploding.
    mockService.queryMetrics.mockReturnValue(of<MetricsQueryResponse>({ metric: "stub", points: [] }));
    await TestBed.configureTestingModule({
      imports: [MetricsPanelComponent, HttpClientTestingModule, NoopAnimationsModule],
      providers: [{ provide: ObservabilityService, useValue: mockService }],
    }).compileComponents();
    fixture = TestBed.createComponent(MetricsPanelComponent);
    component = fixture.componentInstance;
  });

  it("dispatches one queryMetrics call per chart on init", () => {
    fixture.detectChanges();
    expect(mockService.queryMetrics).toHaveBeenCalledTimes(component.chartDescriptors.length);
    const names = mockService.queryMetrics.mock.calls.map(c => (c[0] as { name: NamedMetric }).name);
    expect(names).toEqual(
      expect.arrayContaining([
        "runsPerDay",
        "totalRuns",
        "activeWorkflows",
        "successRate",
        "failureRate",
        "avgDuration",
        "p50Duration",
        "p95Duration",
        "p99Duration",
      ])
    );
  });

  it("populates chartOptions[name] for each successful response", () => {
    const resp: MetricsQueryResponse = {
      metric: "p95Duration",
      points: [
        { timestampMs: 1, value: 0.5 },
        { timestampMs: 2, value: 0.8 },
      ],
    };
    mockService.queryMetrics.mockReturnValue(of(resp));
    fixture.detectChanges();
    expect(component.chartOptions.p95Duration).toBeDefined();
    // The series data is bound as the typed (time, value) tuples —
    // ECharts treats this as numeric data, never as code.
    const series = (component.chartOptions.p95Duration as any).series[0];
    expect(series.type).toBe("line");
    expect(series.data).toEqual([
      [1, 0.5],
      [2, 0.8],
    ]);
  });

  it("renders a human error when a query fails", () => {
    mockService.queryMetrics.mockReturnValue(
      throwError(() => ({ error: { code: "backend_unreachable", message: "metrics backend down" } }))
    );
    fixture.detectChanges();
    expect(component.errorMessage).toBe("metrics backend down");
  });

  it("replaces stale chart options on refresh", () => {
    // The synchronous mock observable resolves each request inside
    // refresh(), so by the time refresh() returns the four charts have
    // already been re-populated. We assert the stale marker is gone
    // (i.e. the entry was rewritten, not preserved).
    fixture.detectChanges();
    component.chartOptions = { runsPerDay: { __stale: true } as any };
    component.refresh();
    expect((component.chartOptions.runsPerDay as any).__stale).toBeUndefined();
  });

  // ----- security tripwires ------------------------------------------

  it("never builds an ECharts formatter callback from server input", () => {
    // ECharts `formatter` strings/functions can be a code-injection
    // vector if built from response data. The component file should
    // not declare ANY formatter property in the option object.
    const componentSource = fs.readFileSync(componentPath, "utf-8");
    // Permit the WORD "formatter" in comments (where we explain the
    // rule) but reject any actual `formatter:` property assignment.
    const codeLines = componentSource
      .split("\n")
      .filter(line => !line.trim().startsWith("*") && !line.trim().startsWith("//"));
    const formatterAssignments = codeLines.filter(line => /\bformatter\s*:/.test(line));
    expect(formatterAssignments).toEqual([]);
  });

  it("title and axis labels come from static descriptors, not server output", () => {
    const resp: MetricsQueryResponse = {
      // A malicious backend could return a 'metric' field designed to
      // surface in the UI. Our buildOption ignores it and uses the
      // static chartDescriptors[].title.
      metric: '<script>alert("xss")</script>',
      points: [{ timestampMs: 1, value: 0 }],
    };
    mockService.queryMetrics.mockReturnValue(of(resp));
    fixture.detectChanges();
    // The chart title is now rendered by the nz-card header binding
    // (which receives a static descriptor.title), so the ECharts
    // option does NOT carry a title at all. The security invariant
    // is: nothing in the chart option set comes from response strings.
    const serialised = JSON.stringify(component.chartOptions);
    expect(serialised).not.toContain("<script>");
    expect(serialised).not.toContain("xss");
  });

  // ---- summary stats (hero row above each chart) ---------------------

  it("computes a positive trend and delta% for an increasing series", () => {
    const resp: MetricsQueryResponse = {
      metric: "runsPerDay",
      points: [
        { timestampMs: 1, value: 10 },
        { timestampMs: 2, value: 15 },
      ],
    };
    mockService.queryMetrics.mockReturnValue(of(resp));
    fixture.detectChanges();
    const s = component.summaries.runsPerDay!;
    expect(s).toBeDefined();
    expect(s.latest).toBe(15);
    expect(s.trend).toBe("up");
    expect(s.deltaPct).toBe(50); // (15-10)/10 = 50%
  });

  it("computes a negative trend for a decreasing series", () => {
    const resp: MetricsQueryResponse = {
      metric: "p95Duration",
      points: [
        { timestampMs: 1, value: 20 },
        { timestampMs: 2, value: 10 },
      ],
    };
    mockService.queryMetrics.mockReturnValue(of(resp));
    fixture.detectChanges();
    const s = component.summaries.p95Duration!;
    expect(s.trend).toBe("down");
    expect(s.deltaPct).toBe(50);
  });

  it("flags trend as 'flat' when the change is below the noise threshold", () => {
    const resp: MetricsQueryResponse = {
      metric: "activeWorkflows",
      points: [
        { timestampMs: 1, value: 100 },
        { timestampMs: 2, value: 100.1 }, // 0.1% change — below the 0.5% threshold
      ],
    };
    mockService.queryMetrics.mockReturnValue(of(resp));
    fixture.detectChanges();
    expect(component.summaries.activeWorkflows!.trend).toBe("flat");
  });

  it("handles a zero baseline without producing Infinity", () => {
    const resp: MetricsQueryResponse = {
      metric: "failureRate",
      points: [
        { timestampMs: 1, value: 0 },
        { timestampMs: 2, value: 5 },
      ],
    };
    mockService.queryMetrics.mockReturnValue(of(resp));
    fixture.detectChanges();
    const s = component.summaries.failureRate!;
    expect(Number.isFinite(s.deltaPct)).toBe(true);
    expect(s.trend).toBe("up");
  });

  it("returns a placeholder summary for an empty response", () => {
    mockService.queryMetrics.mockReturnValue(of<MetricsQueryResponse>({ metric: "x", points: [] }));
    fixture.detectChanges();
    const s = component.summaries.runsPerDay!;
    expect(s.latest).toBe(0);
    expect(s.trend).toBe("flat");
  });
});
