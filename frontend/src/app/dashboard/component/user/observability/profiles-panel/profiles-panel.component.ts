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
import { CommonModule } from "@angular/common";
import { Subject, takeUntil } from "rxjs";
import { FormControl, FormGroup, ReactiveFormsModule, Validators } from "@angular/forms";
import { NzAlertComponent } from "ng-zorro-antd/alert";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzDatePickerComponent, NzRangePickerComponent } from "ng-zorro-antd/date-picker";
import { NzEmptyComponent } from "ng-zorro-antd/empty";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzSpinComponent } from "ng-zorro-antd/spin";
import { ObservabilityService, ValidationError } from "../../../../service/user/observability/observability.service";
import { loadPanelPrefs, savePanelPrefs } from "../../../../service/user/observability/observability-prefs";
import {
  ProfilesQueryResponse,
  ProfileTimelinePoint,
  ProfileTopEntry,
} from "../../../../service/user/observability/observability.types";

// Parca's own UI renders the full interactive flame graph (call stacks, search,
// zoom). We intentionally do NOT render the flame graph in the browser (it is a
// large DOM / memory hog); this panel shows lightweight stats and links out.
// Dev default; in a remote deployment Parca is usually not browser-reachable,
// so override here if you expose it.
const PARCA_UI_BASE = "http://localhost:7070";
// Mirror of the gateway's profile-type identifier (ParcaQueryBuilder), used only
// to build the Parca deep-link expression.
const PROFILE_TYPE = "parca_agent:samples:count:cpu:nanoseconds:delta";

/**
 * Profiles panel. Shows high-level CPU stats from Parca: a CPU-over-time
 * timeline, a summary line, and a ranked "top functions" table. The full
 * interactive flame graph is one click away in Parca (the deep link) rather
 * than rendered here, which keeps the page light.
 *
 * Defaults to the Texera JVMs (`comm = java`) over the last hour because
 * merging every process on the host is slow and rarely useful as one view.
 * Note: with the eBPF agent, Java frames are often unsymbolized and roll up
 * under "(unsymbolized)"; native processes (e.g. postgres) show real names.
 */
@Component({
  selector: "texera-observability-profiles-panel",
  templateUrl: "./profiles-panel.component.html",
  styleUrls: ["./profiles-panel.component.scss"],
  imports: [
    CommonModule,
    ReactiveFormsModule,
    NzAlertComponent,
    NzButtonComponent,
    NzDatePickerComponent,
    NzEmptyComponent,
    NzInputDirective,
    NzRangePickerComponent,
    NzSpinComponent,
  ],
})
export class ProfilesPanelComponent implements OnInit, OnDestroy {
  // SVG viewBox for the timeline sparkline.
  readonly timelineWidth = 600;
  readonly timelineHeight = 40;

  // nz-range-picker requires a tuple value; using two separate
  // FormControls crashes the picker at writeValue time.
  form = new FormGroup({
    range: new FormControl<[Date, Date] | null>(
      // Default window: last hour.
      [new Date(Date.now() - 60 * 60 * 1000), new Date()],
      Validators.required
    ),
    // Default to the Texera JVMs (comm=java): relevant, and far faster to
    // merge than the whole host (clearing this filter merges every process,
    // which can take ~20s). Clear or change it to profile other processes.
    comm: new FormControl<string | null>("java"),
  });

  loading = false;
  errorMessage: string | null = null;
  totalSamples = 0;
  top: ReadonlyArray<ProfileTopEntry> = [];
  timeline: ReadonlyArray<ProfileTimelinePoint> = [];

  private readonly destroy$ = new Subject<void>();

  constructor(private observabilityService: ObservabilityService) {}

  ngOnInit(): void {
    // Restore the operator's last filter (process/comm); the time range is
    // intentionally not persisted and keeps its fresh "last hour" default.
    const prefs = loadPanelPrefs<typeof this.form.value>("profiles");
    if (prefs) this.form.patchValue(prefs);
    this.form.valueChanges.pipe(takeUntil(this.destroy$)).subscribe(v => savePanelPrefs("profiles", v, ["range"]));
    this.refresh();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  /** True when there is anything to show. */
  get hasData(): boolean {
    return this.totalSamples > 0 && this.top.length > 0;
  }

  /** Current process filter, for display. */
  get filterLabel(): string {
    return this.form.value.comm?.trim() || "all processes";
  }

  /** The hottest function name (top of the table), for the summary line. */
  get topConsumer(): string {
    return this.top[0]?.name ?? "n/a";
  }

  /** Largest self value, for scaling the table bars. */
  get maxFlat(): number {
    return this.top.reduce((m, e) => Math.max(m, e.flat), 0) || 1;
  }

  /** Percent of total for a sample value (0 when no data). */
  pct(value: number): number {
    return this.totalSamples > 0 ? (value / this.totalSamples) * 100 : 0;
  }

  /** Relative bar width (0-100) for a top-table row, scaled to the max. */
  barPct(entry: ProfileTopEntry): number {
    return (entry.flat / this.maxFlat) * 100;
  }

  /** Polyline points for the timeline sparkline, normalized to the viewBox. */
  get timelinePoints(): string {
    const pts = this.timeline;
    if (pts.length === 0) return "";
    const w = this.timelineWidth;
    const h = this.timelineHeight;
    const max = Math.max(...pts.map(p => p.value), 1);
    const n = pts.length;
    return pts
      .map((p, i) => {
        const x = n === 1 ? w : (i / (n - 1)) * w;
        const y = h - (p.value / max) * h;
        return `${x.toFixed(1)},${y.toFixed(1)}`;
      })
      .join(" ");
  }

  /** Deep link into Parca's UI for the current filter + time window, where the
   *  full call-stack flame graph lives. */
  get parcaDeepLink(): string {
    const range = this.form.value.range;
    const now = Date.now();
    const fromMs = range?.[0]?.getTime() ?? now - 60 * 60 * 1000;
    const toMs = range?.[1]?.getTime() ?? now;
    const comm = this.form.value.comm?.trim();
    const selector = comm ? `deployment="texera",comm="${comm}"` : 'deployment="texera"';
    const params = new URLSearchParams({
      expression_a: `${PROFILE_TYPE}{${selector}}`,
      from_a: String(fromMs),
      to_a: String(toMs),
      merge_a: "true",
    });
    return `${PARCA_UI_BASE}/?${params.toString()}`;
  }

  refresh(): void {
    const v = this.form.value;
    const range = v.range;
    if (!range || range.length !== 2 || !range[0] || !range[1]) return;
    this.loading = true;
    this.errorMessage = null;
    this.totalSamples = 0;
    this.top = [];
    this.timeline = [];

    try {
      this.observabilityService
        .queryProfiles({
          comm: v.comm?.trim() || undefined,
          fromMs: range[0].getTime(),
          toMs: range[1].getTime(),
        })
        .pipe(takeUntil(this.destroy$))
        .subscribe({
          next: (resp: ProfilesQueryResponse) => {
            this.totalSamples = resp.totalSamples;
            this.top = resp.top ?? [];
            this.timeline = resp.timeline ?? [];
            this.loading = false;
          },
          error: (err: unknown) => {
            this.errorMessage = humanizeError(err);
            this.loading = false;
          },
        });
    } catch (e) {
      if (e instanceof ValidationError) {
        this.errorMessage = e.message;
      }
      this.loading = false;
    }
  }
}

function humanizeError(err: unknown): string {
  if (typeof err === "object" && err !== null) {
    const body = (err as { error?: { code?: string; message?: string } }).error;
    if (body?.message) return body.message;
  }
  return "Failed to load profile.";
}
