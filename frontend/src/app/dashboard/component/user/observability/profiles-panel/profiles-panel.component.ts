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
import { CommonModule } from "@angular/common";
import { FormControl, FormGroup, ReactiveFormsModule, Validators } from "@angular/forms";
import { NzAlertComponent } from "ng-zorro-antd/alert";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzDatePickerComponent, NzRangePickerComponent } from "ng-zorro-antd/date-picker";
import { NzEmptyComponent } from "ng-zorro-antd/empty";
import { NzInputDirective } from "ng-zorro-antd/input";
import {
  ObservabilityService,
  ValidationError,
} from "../../../../service/user/observability/observability.service";
import {
  FlameFrame,
  ProfilesQueryResponse,
} from "../../../../service/user/observability/observability.types";
import { FlameChartComponent } from "./flame-chart/flame-chart.component";

/**
 * Profiles panel: CPU/alloc flame graph from Parca, scoped by an
 * optional workflow / execution id pair.
 *
 * Two empty-state branches matter:
 *   - The Parca server is unreachable. Handled one level up by the
 *     shell's per-signal reachability gate (PR 8); we never even
 *     mount in that case.
 *   - The agent is running but has no samples for the requested
 *     scope yet — common on macOS/Windows developers who set
 *     TEXERA_OBSERVABILITY_PROFILES=disabled, and also on the
 *     first few minutes after a workflow starts. Surfaced inline
 *     with a clear nz-empty card explaining the cause.
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
    FlameChartComponent,
  ],
})
export class ProfilesPanelComponent implements OnInit {
  // nz-range-picker requires a tuple value; using two separate
  // FormControls crashes the picker at writeValue time.
  form = new FormGroup({
    range: new FormControl<[Date, Date] | null>(
      [new Date(Date.now() - 60 * 60 * 1000), new Date()],
      Validators.required
    ),
    workflowId: new FormControl<number | null>(null),
    executionId: new FormControl<number | null>(null),
  });

  loading = false;
  errorMessage: string | null = null;
  result: ProfilesQueryResponse | null = null;
  root: FlameFrame | null = null;
  totalSamples = 0;

  constructor(private observabilityService: ObservabilityService) {}

  ngOnInit(): void {
    this.refresh();
  }

  refresh(): void {
    const v = this.form.value;
    const range = v.range;
    if (!range || range.length !== 2 || !range[0] || !range[1]) return;
    this.loading = true;
    this.errorMessage = null;
    this.result = null;
    this.root = null;
    this.totalSamples = 0;

    try {
      this.observabilityService
        .queryProfiles({
          workflowId: v.workflowId ?? undefined,
          executionId: v.executionId ?? undefined,
          fromMs: range[0].getTime(),
          toMs: range[1].getTime(),
        })
        .subscribe({
          next: resp => {
            this.result = resp;
            this.root = resp.root;
            this.totalSamples = resp.totalSamples;
            this.loading = false;
          },
          error: err => {
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
