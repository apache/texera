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
import { HttpErrorResponse } from "@angular/common/http";
import { DatePipe, NgFor, NgIf } from "@angular/common";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { EMPTY, interval, Subject, throwError } from "rxjs";
import { catchError, exhaustMap, finalize, startWith, switchMap } from "rxjs/operators";
import { NzAlertComponent } from "ng-zorro-antd/alert";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzCardComponent } from "ng-zorro-antd/card";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { WorkflowComputingUnitManagingService } from "../../../../../common/service/computing-unit/workflow-computing-unit/workflow-computing-unit-managing.service";
import { ResourceCapacity } from "../../../../../common/type/workflow-computing-unit";
import { extractErrorMessage } from "../../../../../common/util/error";

// Capacity moves only when units start or stop, and each fetch lists every pod, so this polls slower than the table.
const CLUSTER_CAPACITY_REFRESH_INTERVAL_MS = 30_000;

const MILLICORES_PER_CORE = 1000;
const BYTES_PER_UNIT = 1024;
const BYTE_UNITS = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];

// Three decimals show millicores exactly.
const exactFormat = new Intl.NumberFormat("en-US", { maximumFractionDigits: 3 });
const bytesFormat = new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 });

interface CapacityStatus {
  minPercent: number;
  label: string;
  icon: string;
  className: string;
}

// Compared with the share as shown, so 94% is never flagged as nearly full.
const CAPACITY_STATUSES: CapacityStatus[] = [
  { minPercent: 95, label: "Nearly full", icon: "warning", className: "capacity-critical" },
  { minPercent: 80, label: "Running low", icon: "exclamation-circle", className: "capacity-warning" },
];

/** `percentUsed` is unset when there is no total to divide by, and `note` then says why. */
interface CapacityRow {
  resource: string;
  allocated: string;
  available: string;
  total: string;
  percentUsed?: number;
  fillPercent?: number;
  status?: CapacityStatus;
  note?: string;
}

const formatCores = (millicores: number): string => {
  const cores = exactFormat.format(millicores / MILLICORES_PER_CORE);
  return `${cores} ${cores === "1" ? "core" : "cores"}`;
};

/** Binary units, as Kubernetes quotes memory. */
const formatBytes = (bytes: number): string => {
  let value = bytes;
  let unit = 0;
  // Compared as shown, so 1023.99 MiB reads 1 GiB, not 1,024 MiB.
  while (Math.round(value * 10) / 10 >= BYTES_PER_UNIT && unit < BYTE_UNITS.length - 1) {
    value /= BYTES_PER_UNIT;
    unit++;
  }
  return `${bytesFormat.format(value)} ${BYTE_UNITS[unit]}`;
};

const formatGpus = (count: number): string => exactFormat.format(count);

const isAmount = (n: number): boolean => Number.isFinite(n) && n >= 0;

function toRow(resource: string, capacity: ResourceCapacity | undefined, format: (n: number) => string): CapacityRow {
  // A server response, so a missing, garbled or negative figure gets a row of its own.
  if (!capacity || ![capacity.total, capacity.allocated, capacity.available].every(isAmount)) {
    return { resource, allocated: "—", available: "—", total: "—", note: "Not reported" };
  }
  const { total, allocated, available } = capacity;
  const values = { resource, allocated: format(allocated), available: format(available), total: format(total) };
  if (total === 0) {
    return { ...values, note: "None in the cluster" };
  }
  // Rounded down, so 100% means nothing is left. Multiplying first keeps 29 of 100 from flooring to 28%.
  const percentUsed = Math.floor((allocated * 100) / total);
  return {
    ...values,
    percentUsed,
    // `allocated` can exceed `total`, so the share can pass 100% but the bar cannot.
    fillPercent: Math.min(allocated / total, 1) * 100,
    status: CAPACITY_STATUSES.find(s => percentUsed >= s.minPercent),
  };
}

@UntilDestroy()
@Component({
  selector: "texera-admin-cluster-capacity",
  templateUrl: "./admin-cluster-capacity.component.html",
  styleUrls: ["./admin-cluster-capacity.component.scss"],
  imports: [
    NzAlertComponent,
    NzButtonComponent,
    NzCardComponent,
    NzIconDirective,
    NzTooltipDirective,
    NgFor,
    NgIf,
    DatePipe,
  ],
})
export class AdminClusterCapacityComponent implements OnInit {
  rows?: CapacityRow[];
  // Set by a 404: units do not run on Kubernetes, so there is no cluster to show.
  notOnKubernetes = false;
  loadError?: string;
  lastUpdated?: Date;
  isFetching = false;
  private readonly refreshClicks = new Subject<void>();

  constructor(private computingUnitService: WorkflowComputingUnitManagingService) {}

  // Hidden until the first answer, so a deployment without Kubernetes never flashes the panel.
  get isVisible(): boolean {
    return !this.notOnKubernetes && (this.rows !== undefined || this.loadError !== undefined);
  }

  ngOnInit(): void {
    // A click restarts the clock. A 404 cannot change while the service runs, so it alone passes the inner
    // `catchError`, and the outer one ends the poll.
    this.refreshClicks
      .pipe(
        startWith(undefined),
        // Not `timer(0, …)`, which would delay the fetch to the next macrotask.
        switchMap(() => interval(CLUSTER_CAPACITY_REFRESH_INTERVAL_MS).pipe(startWith(0))),
        exhaustMap(() => {
          this.isFetching = true;
          return this.computingUnitService.getClusterCapacity().pipe(
            catchError((err: unknown) => {
              if (err instanceof HttpErrorResponse && err.status === 404) {
                return throwError(() => err);
              }
              // The alerts end on this message, so it cannot be empty.
              this.loadError = extractErrorMessage(err) || "the server gave no details";
              return EMPTY;
            }),
            finalize(() => (this.isFetching = false))
          );
        }),
        catchError(() => {
          this.notOnKubernetes = true;
          return EMPTY;
        }),
        untilDestroyed(this)
      )
      .subscribe(capacity => {
        this.loadError = undefined;
        this.lastUpdated = new Date();
        this.rows = [
          toRow("CPU", capacity?.cpu, formatCores),
          toRow("Memory", capacity?.memory, formatBytes),
          toRow("GPU", capacity?.gpu, formatGpus),
        ];
      });
  }

  refresh(): void {
    this.refreshClicks.next();
  }

  trackByResource(_index: number, row: CapacityRow): string {
    return row.resource;
  }
}
