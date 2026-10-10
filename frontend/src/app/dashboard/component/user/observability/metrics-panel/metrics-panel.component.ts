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

import { Component, OnDestroy, OnInit } from "@angular/core";
import { DecimalPipe, NgFor, NgIf } from "@angular/common";
import { Subject, takeUntil } from "rxjs";
import { FormControl, FormGroup, ReactiveFormsModule, Validators } from "@angular/forms";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzCardComponent } from "ng-zorro-antd/card";
import { NzDatePickerComponent, NzRangePickerComponent } from "ng-zorro-antd/date-picker";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzAlertComponent } from "ng-zorro-antd/alert";
import { NzSelectModule } from "ng-zorro-antd/select";
import { NzSpinComponent } from "ng-zorro-antd/spin";
import { NzTooltipModule } from "ng-zorro-antd/tooltip";
import { loadPanelPrefs, savePanelPrefs } from "../../../../service/user/observability/observability-prefs";
import { AdminUserService } from "../../../../service/admin/user/admin-user.service";
import { NgxEchartsDirective, provideEchartsCore } from "ngx-echarts";
import * as echarts from "echarts/core";
import { LineChart, GaugeChart } from "echarts/charts";
import { GridComponent, TitleComponent, TooltipComponent, LegendComponent } from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import type { EChartsCoreOption } from "echarts/core";
import { ObservabilityService, ValidationError } from "../../../../service/user/observability/observability.service";
import { MetricsQueryResponse, NamedMetric } from "../../../../service/user/observability/observability.types";

// Register the minimum set of ECharts components we use. Tree-shaking
// keeps the bundle reasonable.
echarts.use([LineChart, GaugeChart, GridComponent, TitleComponent, TooltipComponent, LegendComponent, CanvasRenderer]);

// VictoriaMetrics rejects a query_range that would return more than
// ~30k points per series (-search.maxPointsPerTimeseries). We target a
// margin below that so off-by-one / step-alignment never trips the limit.
const SAFE_MAX_POINTS = 28_000;

/**
 * Workflow stats panel.
 *
 * Nine named server-side queries (throughput, outcome rates, and
 * duration percentiles). The named-query allowlist lives in
 * NAMED_METRICS and is enforced by both the gateway and the service
 * client; the descriptors below are presentation only — each carries
 * a human description surfaced via an info tooltip on its card.
 *
 * Security: every chart receives its data as a typed
 * (timestampMs, value) array bound to `series.data`. There is no
 * `formatter` callback built from response strings — backend output
 * is never interpreted as code or template.
 */
@Component({
  selector: "texera-observability-metrics-panel",
  templateUrl: "./metrics-panel.component.html",
  styleUrls: ["./metrics-panel.component.scss"],
  providers: [provideEchartsCore({ echarts })],
  imports: [
    NgIf,
    NgFor,
    DecimalPipe,
    ReactiveFormsModule,
    NzAlertComponent,
    NzButtonComponent,
    NzCardComponent,
    NzDatePickerComponent,
    NzRangePickerComponent,
    NzInputDirective,
    NzSelectModule,
    NzSpinComponent,
    NzTooltipModule,
    NgxEchartsDirective,
  ],
})
export class MetricsPanelComponent implements OnInit, OnDestroy {
  // Static labels + descriptions shown for each chart. Source:
  // server-side closed enum; the values here are presentation only.
  // `aggregate` controls the hero stat: "latest" = last sample (default),
  // "sum" = window total. `instant` metrics (e.g. totalRuns) come back as
  // a single window-wide scalar, so they render a hero number only, no
  // trend chart.
  readonly chartDescriptors: ReadonlyArray<{
    name: NamedMetric;
    title: string;
    unit: string;
    description: string;
    aggregate?: "latest" | "sum";
    instant?: boolean;
  }> = [
    {
      name: "runsPerDay",
      title: "Workflow runs per day",
      unit: "runs/day",
      description:
        "Number of workflow executions started in the trailing 24 hours, evaluated at each point in time. " +
        "Derived from the texera_workflow_starts_total counter via increase(…[1d]).",
    },
    {
      name: "totalRuns",
      title: "Total runs in window",
      unit: "runs",
      aggregate: "sum",
      instant: true,
      description:
        "Workflow executions started within the selected time range. An exact, system-wide count read from the " +
        "execution records (not the sampled metrics counter). Use the user filter to scope it to one user.",
    },
    {
      name: "activeWorkflows",
      title: "Active executions",
      unit: "count",
      description:
        "Workflow executions currently running (started but not yet completed), as a live gauge summed across all computing units.",
    },
    {
      name: "successRate",
      title: "Success rate",
      unit: "%",
      description:
        "Percentage of completed runs that finished successfully (outcome = success) over each step interval. " +
        "Successful completions ÷ all completions × 100.",
    },
    {
      name: "failureRate",
      title: "Failure rate",
      unit: "%",
      description:
        "Percentage of completed runs that did not succeed (errored, killed, or otherwise non-success) over each step interval. " +
        "The complement of the success rate.",
    },
    {
      name: "avgDuration",
      title: "Average duration",
      unit: "seconds",
      description:
        "Mean wall-clock duration of completed workflow runs, in seconds, over each step interval " +
        "(duration_seconds_sum ÷ duration_seconds_count).",
    },
    {
      name: "p50Duration",
      title: "P50 duration (median)",
      unit: "seconds",
      description: "Median workflow run duration in seconds — half of runs finish faster than this.",
    },
    {
      name: "p95Duration",
      title: "P95 duration",
      unit: "seconds",
      description:
        "95th-percentile workflow run duration in seconds — 95% of runs finish faster than this. Useful for spotting tail latency.",
    },
    {
      name: "p99Duration",
      title: "P99 duration",
      unit: "seconds",
      description: "99th-percentile (worst-case tail) workflow run duration in seconds.",
    },
  ];

  // nz-range-picker is bound as a single tuple — `[from, to]`. A
  // previous shape with two separate FormControls failed at runtime
  // because the picker calls `.map` on the bound value expecting an
  // array.
  // Default to the trailing 30 days so the headline stats reflect
  // essentially all data in a typical deployment, rather than only the
  // last day. The range picker still lets the user narrow it.
  form = new FormGroup({
    range: new FormControl<[Date, Date] | null>(
      [new Date(Date.now() - 30 * 24 * 3600 * 1000), new Date()],
      Validators.required
    ),
    // The user's PREFERRED resolution, in [1s, 1h]. The effective step sent
    // to the backend is auto-raised above this when the window is large, so
    // points-per-series stays under VictoriaMetrics' cap (see
    // computeEffectiveStep). With no window maximum any more, a fixed step
    // alone could otherwise blow that cap and get the query rejected.
    stepSec: new FormControl<number>(3600, [Validators.min(1), Validators.max(3600)]),
    // Optional per-user filter for DB-backed counts (totalRuns). null = all
    // users. Other (metrics-backend) cards ignore it.
    userId: new FormControl<number | null>(null),
  });

  /** Users for the run-count filter dropdown. Populated once on init from
   *  the admin user list (this panel is admin-only). */
  users: ReadonlyArray<{ uid: number; name: string }> = [];

  /** Per-chart option, keyed by metric name. The template binds
   *  each card to `chartOptions[name]` so missing data renders an
   *  explicit empty state. */
  chartOptions: Partial<Record<NamedMetric, EChartsCoreOption>> = {};

  /** Hero stat shown above each chart: most recent value + trend vs.
   *  the window's first point. Computed once per refresh so the
   *  template can stay declarative. */
  summaries: Partial<Record<NamedMetric, MetricSummary>> = {};

  loading = false;
  errorMessage: string | null = null;

  /** Set when the effective step was auto-raised above the preferred step to
   *  fit the points budget; shown as an inline hint. null when not relaxed. */
  stepHint: string | null = null;

  private readonly destroy$ = new Subject<void>();

  constructor(
    private observabilityService: ObservabilityService,
    private adminUserService: AdminUserService
  ) {}

  ngOnInit(): void {
    // Restore the operator's last step / user filter. The time range is
    // intentionally not persisted and keeps its fresh default window.
    const prefs = loadPanelPrefs<typeof this.form.value>("metrics");
    if (prefs) this.form.patchValue(prefs);
    this.form.valueChanges.pipe(takeUntil(this.destroy$)).subscribe(v => savePanelPrefs("metrics", v, ["range"]));

    // Load the user list for the run-count filter. Best-effort: a failure
    // just leaves the dropdown empty (the panel still works for "all users").
    this.adminUserService
      .getUserList()
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: list => (this.users = list.map(u => ({ uid: u.uid, name: u.name }))),
        error: (err: unknown) => {
          // eslint-disable-next-line no-console
          console.warn("[observability] failed to load user list for run-count filter", err);
        },
      });
    this.refresh();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  refresh(): void {
    const v = this.form.value;
    const range = v.range;
    if (!range || range.length !== 2 || !range[0] || !range[1]) return;

    this.loading = true;
    this.errorMessage = null;
    this.chartOptions = {};
    this.summaries = {};

    const fromMs = range[0].getTime();
    const toMs = range[1].getTime();
    const preferredStep = v.stepSec ?? 60;
    // Auto-relax: a fixed step over a now-unbounded window can exceed the
    // points-per-series cap. Raise the step just enough to stay under it.
    const effectiveStep = this.computeEffectiveStep(fromMs, toMs, preferredStep);
    this.stepHint =
      effectiveStep > preferredStep
        ? `Step auto-raised to ${this.formatStep(effectiveStep)} (from ${this.formatStep(preferredStep)}) ` +
          `to keep the series under ${SAFE_MAX_POINTS.toLocaleString()} points for this window.`
        : null;

    // Fan out one request per chart, independent so one failure doesn't
    // kill the others. Clear `loading` only once all requests settle.
    let pending = this.chartDescriptors.length;
    const settle = () => {
      if (--pending === 0) this.loading = false;
    };
    this.chartDescriptors.forEach(desc => {
      try {
        this.observabilityService
          .queryMetrics({
            name: desc.name,
            fromMs,
            toMs,
            stepSec: effectiveStep,
            userId: v.userId ?? undefined,
          })
          .pipe(takeUntil(this.destroy$))
          .subscribe({
            next: resp => {
              this.chartOptions[desc.name] = this.buildOption(desc.title, desc.unit, resp);
              this.summaries[desc.name] = this.computeSummary(resp, desc.aggregate ?? "latest");
              settle();
            },
            error: (err: unknown) => {
              // Per-metric breadcrumb: the fan-out shares a single
              // errorMessage, so without this an operator can't tell
              // which of the N charts actually failed.
              // eslint-disable-next-line no-console
              console.warn(`[observability] metric '${desc.name}' failed to load`, err);
              this.errorMessage = humanizeError(err);
              settle();
            },
          });
      } catch (e) {
        if (e instanceof ValidationError) {
          // eslint-disable-next-line no-console
          console.warn(`[observability] metric '${desc.name}' rejected before dispatch: ${e.message}`);
          this.errorMessage = e.message;
        } else {
          this.errorMessage = humanizeError(e);
        }
        settle();
      }
    });
  }

  /**
   * Smallest step (seconds) that keeps a series under SAFE_MAX_POINTS for the
   * given window, but never below the user's preferred step. With the window
   * maximum removed, this is what stops a large range from exceeding the
   * backend's points-per-series cap. DB-backed cards ignore the step, so the
   * relaxed value is harmless for them.
   */
  private computeEffectiveStep(fromMs: number, toMs: number, preferredStep: number): number {
    const windowSec = Math.max(1, Math.round((toMs - fromMs) / 1000));
    const minStep = Math.ceil(windowSec / SAFE_MAX_POINTS);
    return Math.max(preferredStep, minStep, 1);
  }

  /** Human-friendly step label: seconds, minutes, or hours. */
  private formatStep(sec: number): string {
    if (sec < 60) return `${sec}s`;
    if (sec < 3600) return `${Math.round(sec / 60)}m`;
    return `${Math.round((sec / 3600) * 10) / 10}h`;
  }

  /**
   * Build the ECharts option object for one chart. **All values are
   * bound as typed data; nothing reaches a `formatter:` string built
   * from response text.** The tooltip uses ECharts' built-in
   * formatter which takes our data points and emits its own safe
   * HTML — we never construct that HTML from server output.
   */
  private buildOption(title: string, unit: string, resp: MetricsQueryResponse): EChartsCoreOption {
    // Typed numeric tuples — ECharts treats these as data, not strings.
    const data: ReadonlyArray<[number, number]> = resp.points.map(p => [p.timestampMs, p.value]);

    return {
      // Title lives in the nz-card header; suppressing it here gives
      // the chart its full vertical real estate.
      tooltip: {
        trigger: "axis",
        // No formatter callback — ECharts default renders the
        // [time, value] tuples; we never wire response strings here.
      },
      xAxis: {
        type: "time",
        boundaryGap: false,
        axisLabel: { fontSize: 11 },
      },
      yAxis: {
        type: "value",
        name: unit, // static unit string from our descriptor
        nameTextStyle: { fontSize: 11, color: "rgba(0,0,0,0.55)" },
        axisLabel: { fontSize: 11 },
        splitLine: { lineStyle: { color: "rgba(0,0,0,0.06)" } },
      },
      series: [
        {
          type: "line",
          showSymbol: false,
          smooth: true,
          data,
          lineStyle: { width: 2, color: "#1668dc" },
          // Soft area-fill gives a richer visual without changing the
          // semantics — the y-axis still shows the numeric scale.
          areaStyle: {
            color: {
              type: "linear",
              x: 0,
              y: 0,
              x2: 0,
              y2: 1,
              colorStops: [
                { offset: 0, color: "rgba(22, 104, 220, 0.30)" },
                { offset: 1, color: "rgba(22, 104, 220, 0.02)" },
              ],
            },
          },
        },
      ],
      grid: { left: 48, right: 18, top: 14, bottom: 24 },
    };
  }

  /**
   * Reduce a point series down to the values shown in the hero row.
   *
   * - aggregate "latest" (default): most recent value, % change vs. the
   *   first sample, and a trend marker.
   * - aggregate "sum": total of all points over the window (e.g. total
   *   runs). Trend is not meaningful for a window total, so it is flat.
   *
   * Returns a zeroed summary when the series is empty (template renders
   * the placeholder dash).
   */
  private computeSummary(resp: MetricsQueryResponse, aggregate: "latest" | "sum"): MetricSummary {
    if (resp.points.length === 0) {
      return { latest: 0, deltaPct: 0, trend: "flat" };
    }
    if (aggregate === "sum") {
      // Count metrics arrive as a single window-wide value (defensively, we
      // still sum in case more than one point comes back). A run count is an
      // integer, so round off the fractional residue increase() can leave.
      const total = resp.points.reduce((acc, p) => acc + p.value, 0);
      return { latest: Math.round(total), deltaPct: 0, trend: "flat" };
    }
    const first = resp.points[0].value;
    const latest = resp.points[resp.points.length - 1].value;
    // Avoid divide-by-zero: when the baseline is 0 we report the
    // delta as 100% (or 0% if latest is also 0) rather than ∞.
    let deltaPct = 0;
    if (first !== 0) {
      deltaPct = ((latest - first) / Math.abs(first)) * 100;
    } else if (latest !== 0) {
      deltaPct = 100;
    }
    const trend: MetricSummary["trend"] = Math.abs(deltaPct) < 0.5 ? "flat" : deltaPct > 0 ? "up" : "down";
    return { latest, deltaPct: Math.abs(deltaPct), trend };
  }
}

/** Hero stat for one named-metric card. */
interface MetricSummary {
  latest: number;
  deltaPct: number;
  trend: "up" | "down" | "flat";
}

function humanizeError(err: unknown): string {
  if (typeof err === "object" && err !== null) {
    const body = (err as { error?: { code?: string; message?: string } }).error;
    if (body?.message) return body.message;
  }
  return "Failed to load metrics.";
}
