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
import { EMPTY, interval } from "rxjs";
import { catchError, exhaustMap, finalize, startWith } from "rxjs/operators";
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
import { NzAlertComponent } from "ng-zorro-antd/alert";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzCardComponent } from "ng-zorro-antd/card";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzBadgeComponent } from "ng-zorro-antd/badge";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzSpinComponent } from "ng-zorro-antd/spin";
import { NzMessageService } from "ng-zorro-antd/message";
import { WorkflowComputingUnitManagingService } from "../../../../common/service/computing-unit/workflow-computing-unit/workflow-computing-unit-managing.service";
import {
  DashboardWorkflowComputingUnit,
  WorkflowComputingUnitResourceLimit,
} from "../../../../common/type/workflow-computing-unit";
import { ComputingUnitActionsService } from "../../../../common/service/computing-unit/computing-unit-actions/computing-unit-actions.service";
import {
  getComputingUnitBadgeColor,
  getComputingUnitStatusTooltip,
  unitTypeMessageTemplate,
} from "../../../../common/util/computing-unit.util";
import { formatRelativeTime } from "../../../../common/util/format.util";
import { extractErrorMessage } from "../../../../common/util/error";
import { UserAvatarComponent } from "../../user/user-avatar/user-avatar.component";
import { ComputingUnitState } from "../../../../common/type/computing-unit-connection.interface";

// Same cadence as the admin executions page.
const COMPUTING_UNIT_REFRESH_INTERVAL_MS = 5000;

// Local units have no limits, so their specs come back as this placeholder.
const NOT_APPLICABLE = "NaN";

const hasSpec = (value?: string) => !!value && value !== NOT_APPLICABLE;

// All resource fields except `nodeAddresses`, which is a list.
type SpecKey = Exclude<keyof WorkflowComputingUnitResourceLimit, "nodeAddresses">;

@UntilDestroy()
@Component({
  templateUrl: "./admin-computing-unit.component.html",
  styleUrls: ["./admin-computing-unit.component.scss"],
  imports: [
    NzAlertComponent,
    NzButtonComponent,
    NzCardComponent,
    NzIconDirective,
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
  // True from the first failed poll until the next one succeeds. While it holds, the rows are the last good snapshot
  // (or nothing, if the first poll failed), so the page warns instead of passing them off as current.
  pollFailing = false;
  lastUpdated?: Date;
  readonly expandedCuids = new Set<number>();
  // Units terminated from this page. A poll that started before the `DELETE` can still answer with them, so every
  // response is filtered by this set. A `cuid` is never reused, so there is no need to prune it.
  private readonly terminatedCuids = new Set<number>();

  readonly getBadgeColor = getComputingUnitBadgeColor;
  readonly getStatusTooltip = getComputingUnitStatusTooltip;
  readonly formatRelativeTime = formatRelativeTime;

  readonly specFields: { label: string; key: SpecKey }[] = [
    { label: "CPU", key: "cpuLimit" },
    { label: "Memory", key: "memoryLimit" },
    { label: "GPU", key: "gpuLimit" },
    { label: "JVM Memory", key: "jvmMemorySize" },
    { label: "Shared Memory", key: "shmSize" },
  ];

  readonly typeFilters = [
    { text: "Kubernetes", value: "kubernetes" },
    { text: "Local", value: "local" },
  ];
  // Every reportable status, so an admin can filter to the `Failed` ones, but not the `NoComputingUnit` sentinel.
  readonly statusFilters = Object.values(ComputingUnitState)
    .filter(status => status !== ComputingUnitState.NoComputingUnit)
    .map(status => ({ text: status, value: status }));

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

  constructor(
    private computingUnitService: WorkflowComputingUnitManagingService,
    private computingUnitActionsService: ComputingUnitActionsService,
    private messageService: NzMessageService
  ) {}

  ngOnInit(): void {
    // `startWith(0)` loads at once. `exhaustMap` rather than `switchMap` lets a response slower than the interval
    // finish instead of being cancelled by the next tick, so a slow cluster still refreshes. `catchError` sits inside
    // it, so a failed poll does not end the stream. It toasts only when polling starts failing, so a long outage does
    // not raise a new toast every tick; the warning above the table covers the rest of it.
    interval(COMPUTING_UNIT_REFRESH_INTERVAL_MS)
      .pipe(
        startWith(0),
        exhaustMap(() =>
          this.computingUnitService.listAllComputingUnits().pipe(
            catchError((err: unknown) => {
              if (!this.pollFailing) {
                this.messageService.error(extractErrorMessage(err));
              }
              this.pollFailing = true;
              return EMPTY;
            }),
            // Also runs after a failed load, so the spinner cannot spin forever.
            finalize(() => (this.isLoading = false))
          )
        ),
        untilDestroyed(this)
      )
      .subscribe(units => {
        this.pollFailing = false;
        this.lastUpdated = new Date();
        this.computingUnits = this.withoutTerminated(units);
      });
  }

  /** A poll replaces every row object, so key the rows by `cuid` to reuse their DOM. */
  trackByCuid(_index: number, unit: DashboardWorkflowComputingUnit): number {
    return unit.computingUnit.cuid;
  }

  onExpandChange(cuid: number, expanded: boolean): void {
    if (expanded) {
      this.expandedCuids.add(cuid);
    } else {
      this.expandedCuids.delete(cuid);
    }
  }

  isLocal(unit: DashboardWorkflowComputingUnit): boolean {
    return unit.computingUnit.type === "local";
  }

  /** Confirms the termination, then drops the row at once instead of at the next poll. */
  terminate(unit: DashboardWorkflowComputingUnit): void {
    const cuid = unit.computingUnit.cuid;
    this.computingUnitActionsService.confirmAndTerminate(cuid, unit, () => {
      this.terminatedCuids.add(cuid);
      this.computingUnits = this.withoutTerminated(this.computingUnits);
    });
  }

  terminateTooltip(unit: DashboardWorkflowComputingUnit): string {
    return unitTypeMessageTemplate[unit.computingUnit.type].terminateTooltip;
  }

  private withoutTerminated(units: ReadonlyArray<DashboardWorkflowComputingUnit>): DashboardWorkflowComputingUnit[] {
    return units.filter(u => !this.terminatedCuids.has(u.computingUnit.cuid));
  }

  /** The Resources column, e.g. "2 CPU · 4Gi · 1 GPU", with GPU left out when it is 0. */
  resourceSummary(unit: DashboardWorkflowComputingUnit): string {
    if (this.isLocal(unit)) {
      return "Local — no limits";
    }
    const { cpuLimit, memoryLimit, gpuLimit } = unit.computingUnit.resource;
    const parts: string[] = [];
    if (hasSpec(cpuLimit)) {
      parts.push(`${cpuLimit} CPU`);
    }
    if (hasSpec(memoryLimit)) {
      parts.push(memoryLimit);
    }
    if (hasSpec(gpuLimit) && gpuLimit !== "0") {
      parts.push(`${gpuLimit} GPU`);
    }
    return parts.length > 0 ? parts.join(" · ") : "—";
  }

  /** Shows the placeholder or an empty spec as an em dash. */
  displaySpec(value: string): string {
    return hasSpec(value) ? value : "—";
  }
}
