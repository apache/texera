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
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { interval } from "rxjs";
import { switchMap } from "rxjs/operators";
import { DatePipe, NgFor, NgIf } from "@angular/common";
import {
  NzTableComponent,
  NzTheadComponent,
  NzTbodyComponent,
  NzTrDirective,
  NzTableCellDirective,
  NzThMeasureDirective,
  NzThAddOnComponent,
  NzTdAddOnComponent,
  NzTableSortFn,
  NzTableFilterFn,
} from "ng-zorro-antd/table";
import { NzCardComponent } from "ng-zorro-antd/card";
import { NzBadgeComponent } from "ng-zorro-antd/badge";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzSpinComponent } from "ng-zorro-antd/spin";
import { WorkflowComputingUnitManagingService } from "../../../../common/service/computing-unit/workflow-computing-unit/workflow-computing-unit-managing.service";
import { DashboardWorkflowComputingUnit } from "../../../../common/type/workflow-computing-unit";
import { getComputingUnitBadgeColor, getComputingUnitStatusTooltip } from "../../../../common/util/computing-unit.util";
import { formatRelativeTime } from "../../../../common/util/format.util";
import { UserAvatarComponent } from "../../user/user-avatar/user-avatar.component";

// How often the table refreshes so live status (Pending -> Running) and newly created
// units stay fresh, matching the poll cadence of the admin executions page.
const COMPUTING_UNIT_REFRESH_INTERVAL_MS = 5000;

// A computing unit's specs are "NaN" placeholders for local units, which have no limits.
const NOT_APPLICABLE = "NaN";

@UntilDestroy()
@Component({
  templateUrl: "./admin-computing-unit.component.html",
  styleUrls: ["./admin-computing-unit.component.scss"],
  imports: [
    NzCardComponent,
    NzTableComponent,
    NzTheadComponent,
    NzTbodyComponent,
    NzTrDirective,
    NzTableCellDirective,
    NzThMeasureDirective,
    NzThAddOnComponent,
    NzTdAddOnComponent,
    NzBadgeComponent,
    NzTooltipDirective,
    NzSpinComponent,
    UserAvatarComponent,
    NgFor,
    NgIf,
    DatePipe,
  ],
})
export class AdminComputingUnitComponent implements OnInit {
  computingUnits: ReadonlyArray<DashboardWorkflowComputingUnit> = [];
  isLoading: boolean = true;
  // cuids of rows whose spec detail is expanded.
  readonly expandedCuids = new Set<number>();

  // Expose the shared formatters to the template.
  readonly getBadgeColor = getComputingUnitBadgeColor;
  readonly getStatusTooltip = getComputingUnitStatusTooltip;
  readonly formatRelativeTime = formatRelativeTime;

  readonly typeFilters = [
    { text: "Kubernetes", value: "kubernetes" },
    { text: "Local", value: "local" },
  ];
  readonly statusFilters = [
    { text: "Running", value: "Running" },
    { text: "Pending", value: "Pending" },
  ];

  readonly sortByName: NzTableSortFn<DashboardWorkflowComputingUnit> = (a, b) =>
    (a.computingUnit.name ?? "").localeCompare(b.computingUnit.name ?? "");
  readonly sortByOwner: NzTableSortFn<DashboardWorkflowComputingUnit> = (a, b) =>
    (a.ownerName ?? "").localeCompare(b.ownerName ?? "");
  readonly sortByType: NzTableSortFn<DashboardWorkflowComputingUnit> = (a, b) =>
    a.computingUnit.type.localeCompare(b.computingUnit.type);
  readonly sortByStatus: NzTableSortFn<DashboardWorkflowComputingUnit> = (a, b) => a.status.localeCompare(b.status);
  readonly sortByCreated: NzTableSortFn<DashboardWorkflowComputingUnit> = (a, b) =>
    a.computingUnit.creationTime - b.computingUnit.creationTime;

  readonly filterByType: NzTableFilterFn<DashboardWorkflowComputingUnit> = (selected: string[], unit) =>
    selected.includes(unit.computingUnit.type);
  readonly filterByStatus: NzTableFilterFn<DashboardWorkflowComputingUnit> = (selected: string[], unit) =>
    selected.includes(unit.status);

  constructor(private computingUnitService: WorkflowComputingUnitManagingService) {}

  ngOnInit(): void {
    this.fetchData();

    // Refresh so status changes and new units surface without a manual reload; switchMap
    // drops a stale in-flight request if the interval fires again before it resolves.
    interval(COMPUTING_UNIT_REFRESH_INTERVAL_MS)
      .pipe(
        switchMap(() => this.computingUnitService.listAllComputingUnits()),
        untilDestroyed(this)
      )
      .subscribe(units => (this.computingUnits = units));
  }

  /**
   * Load every computing unit once, showing the loading indicator (used on init only, so
   * the background poll never flashes the spinner over an already-populated table).
   */
  fetchData(): void {
    this.isLoading = true;
    this.computingUnitService
      .listAllComputingUnits()
      .pipe(untilDestroyed(this))
      .subscribe(units => {
        this.computingUnits = units;
        this.isLoading = false;
      });
  }

  /**
   * Track rows by cuid so a poll (which replaces every row object) reuses DOM instead of
   * rebuilding each row's avatar/badge/tooltip every 5s.
   */
  trackByCuid(_index: number, unit: DashboardWorkflowComputingUnit): number {
    return unit.computingUnit.cuid;
  }

  /**
   * Toggle whether a row's full spec detail is shown.
   */
  onExpandChange(cuid: number, expanded: boolean): void {
    if (expanded) {
      this.expandedCuids.add(cuid);
    } else {
      this.expandedCuids.delete(cuid);
    }
  }

  /**
   * A local unit has no resource limits (every spec is the "NaN" placeholder).
   */
  isLocal(unit: DashboardWorkflowComputingUnit): boolean {
    return unit.computingUnit.type === "local";
  }

  /**
   * One-line "size" summary shown in the table's Resources column, e.g. "2 CPU · 4Gi · 1 GPU".
   * GPU is omitted when there is none. Local units have no limits.
   */
  resourceSummary(unit: DashboardWorkflowComputingUnit): string {
    if (this.isLocal(unit)) {
      return "Local — no limits";
    }
    const { cpuLimit, memoryLimit, gpuLimit } = unit.computingUnit.resource;
    const parts: string[] = [];
    if (cpuLimit && cpuLimit !== NOT_APPLICABLE) {
      parts.push(`${cpuLimit} CPU`);
    }
    if (memoryLimit && memoryLimit !== NOT_APPLICABLE) {
      parts.push(memoryLimit);
    }
    if (gpuLimit && gpuLimit !== NOT_APPLICABLE && gpuLimit !== "0") {
      parts.push(`${gpuLimit} GPU`);
    }
    return parts.length > 0 ? parts.join(" · ") : "—";
  }

  /**
   * Present a spec value, rendering the "NaN" placeholder as an em dash.
   */
  displaySpec(value: string): string {
    return !value || value === NOT_APPLICABLE ? "—" : value;
  }
}
