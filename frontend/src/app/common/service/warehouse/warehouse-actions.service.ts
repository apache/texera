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

import { Injectable } from "@angular/core";
import { NzModalService } from "ng-zorro-antd/modal";
import { EMPTY, catchError, shareReplay } from "rxjs";
import { Observable, firstValueFrom } from "rxjs";
import { NotificationService } from "../notification/notification.service";
import { DashboardWarehouse } from "../../type/warehouse";
import { extractErrorMessage } from "../../util/error";
import { WarehouseService } from "./warehouse.service";

/**
 * Shared warehouse actions (#6933), mirroring ComputingUnitActionsService: one
 * entry point for creating and deleting a warehouse, so callers reach the API
 * the same way and the delete confirmation's wording lives in one place. Shared
 * with the workspace picker once it lands (#7817).
 */
@Injectable({
  providedIn: "root",
})
export class WarehouseActionsService {
  constructor(
    private modalService: NzModalService,
    private warehouseService: WarehouseService,
    private notificationService: NotificationService
  ) {}

  /**
   * Creates a warehouse and reports the outcome itself: the service owns the
   * subscription, so the request and its toasts survive the dialog — and the
   * whole page — being destroyed. Without this, navigating away mid-create
   * aborts the browser request while the server finishes anyway: the warehouse
   * exists, nothing was reported, and the next same-name attempt fails
   * confusingly. The returned observable replays the created warehouse for
   * callers that want it (the dialog relays it to its host) and never errors:
   * a failure is already reported here, and replaying it would surface in
   * relays with no error path as an unhandled RxJS error — the stream just
   * completes empty instead.
   */
  create(name: string): Observable<DashboardWarehouse> {
    const request$ = this.warehouseService.createWarehouse(name).pipe(shareReplay({ bufferSize: 1, refCount: false }));
    request$.subscribe({
      next: created => this.notificationService.success(`Warehouse "${created.name}" created.`),
      error: (err: unknown) =>
        this.notificationService.error(`Failed to create warehouse: ${extractErrorMessage(err)}`),
    });
    return request$.pipe(catchError(() => EMPTY));
  }

  /**
   * Asks for confirmation, then deletes the warehouse and all data stored in it.
   * Runs `onDeleted` after a successful delete so the caller can refresh its
   * list (warehouses have no push stream to do that for them, unlike computing
   * units).
   *
   * `nzOnOk` returns a promise so the dialog keeps its OK button spinning until
   * the request settles: deleting a warehouse that still holds data waits out
   * Lakekeeper's asynchronous purge (#7742) — bounded at roughly 16 seconds by
   * the backend's backoff — and closing the dialog immediately would read as a
   * frozen row.
   */
  confirmAndDelete(warehouse: DashboardWarehouse, onDeleted: () => void): void {
    this.modalService.confirm({
      nzTitle: `Delete warehouse "${warehouse.name}"?`,
      nzContent: "This permanently deletes the warehouse and all data stored in it.",
      nzOkText: "Delete",
      nzOkDanger: true,
      // Returning a promise is what keeps the dialog busy until the request
      // settles; each failure is reported where it belongs, so a refresh that
      // throws is never mistaken for a delete that failed.
      nzOnOk: async () => {
        try {
          await firstValueFrom(this.warehouseService.deleteWarehouse(warehouse.whid));
        } catch (err: unknown) {
          this.notificationService.error(`Failed to delete warehouse: ${extractErrorMessage(err)}`);
          return;
        }
        this.notificationService.success("Warehouse deleted.");
        try {
          onDeleted();
        } catch (err: unknown) {
          console.error("Failed to refresh after deleting a warehouse", err);
        }
      },
    });
  }
}
