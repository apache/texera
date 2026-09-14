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

import { ComponentFixture, discardPeriodicTasks, fakeAsync, TestBed, tick } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { of, throwError } from "rxjs";
import * as fs from "node:fs";
import * as path from "node:path";
import { MetricsPanelComponent } from "./metrics-panel.component";
import { ObservabilityService } from "../../../../service/user/observability/observability.service";
import { AdminUserService } from "../../../../service/admin/user/admin-user.service";
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
  let mockAdminUserService: { getUserList: ReturnType<typeof vi.fn> };

  beforeEach(async () => {
    mockService = { queryMetrics: vi.fn() };
    // Default: return empty points so the component can construct
    // chart options without exploding.
    mockService.queryMetrics.mockReturnValue(of<MetricsQueryResponse>({ metric: "stub", points: [] }));
    mockAdminUserService = { getUserList: vi.fn().mockReturnValue(of([])) };
    await TestBed.configureTestingModule({
      imports: [MetricsPanelComponent, HttpClientTestingModule, NoopAnimationsModule],
      providers: [
        { provide: ObservabilityService, useValue: mockService },
        { provide: AdminUserService, useValue: mockAdminUserService },
      ],
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

  it("forwards the selected userId on every metrics request", () => {
    fixture.detectChanges(); // initial refresh with no user filter
    expect(mockService.queryMetrics.mock.calls.every(c => (c[0] as { userId?: number }).userId === undefined)).toBe(
      true
    );

    mockService.queryMetrics.mockClear();
    component.form.controls.userId.setValue(42);
    component.refresh();
    expect(mockService.queryMetrics.mock.calls.length).toBeGreaterThan(0);
    expect(mockService.queryMetrics.mock.calls.every(c => (c[0] as { userId?: number }).userId === 42)).toBe(true);
  });

  it("auto-raises the step for a large window so points stay under the cap", () => {
    // ~10 years with a 60s preferred step would be ~5.3M points per series.
    const from = new Date(Date.UTC(2015, 0, 1));
    const to = new Date(Date.UTC(2025, 0, 1));
    component.form.controls.range.setValue([from, to]);
    component.form.controls.stepSec.setValue(60);
    component.refresh();

    const steps = mockService.queryMetrics.mock.calls.map(c => (c[0] as { stepSec: number }).stepSec);
    const windowSec = Math.round((to.getTime() - from.getTime()) / 1000);
    expect(steps.length).toBeGreaterThan(0);
    steps.forEach(s => {
      expect(s).toBeGreaterThan(60); // raised above the preferred step
      expect(windowSec / s).toBeLessThanOrEqual(28_000); // under the points budget
    });
    expect(component.stepHint).toContain("auto-raised");
  });

  it("leaves the step untouched and sets no hint when the window is small", () => {
    component.form.controls.range.setValue([new Date(Date.UTC(2025, 0, 1)), new Date(Date.UTC(2025, 0, 2))]);
    component.form.controls.stepSec.setValue(3600);
    component.refresh();
    const steps = mockService.queryMetrics.mock.calls.map(c => (c[0] as { stepSec: number }).stepSec);
    steps.forEach(s => expect(s).toBe(3600));
    expect(component.stepHint).toBeNull();
  });

  it("rounds the totalRuns count and reports a flat trend (window total, instant query)", () => {
    // increase() can leave a fractional residue on an integer counter; the
    // hero must read as a whole run count.
    const resp: MetricsQueryResponse = {
      metric: "totalRuns",
      points: [{ timestampMs: 1, value: 40.9998 }],
    };
    mockService.queryMetrics.mockReturnValue(of(resp));
    fixture.detectChanges();
    const s = component.summaries.totalRuns!;
    expect(s.latest).toBe(41);
    expect(s.trend).toBe("flat");
  });

  it("flags an empty response as no-data so the card shows the dash, not a misleading 0", () => {
    mockService.queryMetrics.mockReturnValue(of<MetricsQueryResponse>({ metric: "x", points: [] }));
    fixture.detectChanges();
    const s = component.summaries.runsPerDay!;
    expect(s.hasData).toBe(false);
    expect(s.latest).toBe(0);
    expect(s.trend).toBe("flat");
  });

  it("follow mode slides the window to end at now on refresh, preserving its duration", () => {
    const from = new Date(Date.UTC(2020, 0, 1));
    const to = new Date(Date.UTC(2020, 0, 8)); // 7-day window
    const durationMs = to.getTime() - from.getTime();
    component.form.controls.follow.setValue(true);
    component.form.controls.range.setValue([from, to]);

    const before = Date.now();
    component.refresh();
    const after = Date.now();

    const call = mockService.queryMetrics.mock.calls.at(-1)![0] as { fromMs: number; toMs: number };
    expect(call.toMs).toBeGreaterThanOrEqual(before);
    expect(call.toMs).toBeLessThanOrEqual(after);
    expect(call.toMs - call.fromMs).toBe(durationMs); // duration preserved
    // The picker reflects the shifted window so the user sees the live range.
    expect(component.form.controls.range.value![1].getTime()).toBe(call.toMs);
  });

  it("with follow off, refresh queries the exact picked window", () => {
    const from = new Date(Date.UTC(2020, 0, 1));
    const to = new Date(Date.UTC(2020, 0, 8));
    component.form.controls.follow.setValue(false);
    component.form.controls.range.setValue([from, to]);
    component.refresh();
    const call = mockService.queryMetrics.mock.calls.at(-1)![0] as { fromMs: number; toMs: number };
    expect(call.fromMs).toBe(from.getTime());
    expect(call.toMs).toBe(to.getTime());
  });

  it("auto-refresh re-queries on the configured interval and stops when set to off", fakeAsync(() => {
    // Drive lifecycle without rendering: the date picker's locale-dependent
    // reformat (ng-zorro defaults to zh_CN, unregistered in the test bed)
    // is irrelevant to the timer logic under test.
    component.ngOnInit(); // one fan-out, no timer yet (autoRefreshSec = 0)
    const perRefresh = component.chartDescriptors.length;
    const initial = mockService.queryMetrics.mock.calls.length;
    expect(initial).toBe(perRefresh);

    component.form.controls.autoRefreshSec.setValue(30);
    tick(30_000);
    expect(mockService.queryMetrics.mock.calls.length).toBe(initial + perRefresh);
    tick(30_000);
    expect(mockService.queryMetrics.mock.calls.length).toBe(initial + 2 * perRefresh);

    component.form.controls.autoRefreshSec.setValue(0); // stop the timer
    const afterOff = mockService.queryMetrics.mock.calls.length;
    tick(120_000);
    expect(mockService.queryMetrics.mock.calls.length).toBe(afterOff);
    discardPeriodicTasks();
  }));
});
