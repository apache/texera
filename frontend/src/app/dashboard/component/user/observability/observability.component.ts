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

import { Component, OnInit } from "@angular/core";
import { NgIf } from "@angular/common";
import { NzTabsComponent, NzTabComponent } from "ng-zorro-antd/tabs";
import { NzAlertComponent } from "ng-zorro-antd/alert";
import { NzEmptyComponent } from "ng-zorro-antd/empty";
import { FormsModule } from "@angular/forms";
import { ObservabilityService } from "../../../service/user/observability/observability.service";
import { ObservabilityHealth } from "../../../service/user/observability/observability.types";
import { LogsPanelComponent } from "./logs-panel/logs-panel.component";
import { MetricsPanelComponent } from "./metrics-panel/metrics-panel.component";
import { TracesPanelComponent } from "./traces-panel/traces-panel.component";
import { ProfilesPanelComponent } from "./profiles-panel/profiles-panel.component";
import { TracesPivotService } from "../../../service/user/observability/traces-pivot.service";
import { OnDestroy } from "@angular/core";
import { Subject, takeUntil } from "rxjs";

/**
 * Shell page for the observability dashboard. Four tabs (Logs,
 * Metrics, Traces, Profiles), each guarded by the per-signal
 * reachability check from /api/observability/health. Tabs whose
 * backend reports unreachable render an explicit "Unreachable"
 * card rather than a broken chart.
 */
@Component({
  selector: "texera-observability",
  templateUrl: "./observability.component.html",
  styleUrls: ["./observability.component.scss"],
  imports: [
    NgIf,
    FormsModule,
    NzTabsComponent,
    NzTabComponent,
    NzAlertComponent,
    NzEmptyComponent,
    LogsPanelComponent,
    MetricsPanelComponent,
    TracesPanelComponent,
    ProfilesPanelComponent,
  ],
})
export class ObservabilityComponent implements OnInit, OnDestroy {
  /** Reachability state. ``null`` means "still loading". A failed
   *  /health call sets every check to false so the UI surfaces the
   *  gateway-down case explicitly. */
  health: ObservabilityHealth | null = null;
  healthError = false;

  /** Active tab index — controls which panel is mounted. */
  activeTab = 0;

  /** Trace id forwarded to the traces panel when the user pivots
   *  from a log row. Null until the first pivot. */
  pivotedTraceId: string | null = null;

  private readonly destroy$ = new Subject<void>();

  /** Index of the Traces tab — kept as a constant so the
   *  pivot handler isn't coupled to the ordering by magic number. */
  private static readonly TRACES_TAB_INDEX = 2;

  constructor(
    private observabilityService: ObservabilityService,
    private tracesPivot: TracesPivotService
  ) {}

  ngOnInit(): void {
    this.observabilityService
      .health()
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: h => {
          this.health = h;
          this.healthError = false;
        },
        error: (err: unknown) => {
          // eslint-disable-next-line no-console
          console.error(
            "[observability] health check failed — gateway unreachable; rendering all signals as degraded",
            err
          );
          this.health = {
            status: "degraded",
            checks: { logs: false, metrics: false, traces: false, profiles: false },
          };
          this.healthError = true;
        },
      });

    this.tracesPivot.onPivot.pipe(takeUntil(this.destroy$)).subscribe(traceId => {
      this.pivotedTraceId = traceId;
      this.activeTab = ObservabilityComponent.TRACES_TAB_INDEX;
    });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  /** Convenience accessors so the template stays declarative. */
  isReachable(signal: "logs" | "metrics" | "traces" | "profiles"): boolean {
    return this.health?.checks[signal] === true;
  }
}
