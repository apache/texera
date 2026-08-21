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
import { Observable, firstValueFrom } from "rxjs";
import { NotificationService } from "../notification/notification.service";
import { DashboardWarehouse } from "../../type/warehouse";
import { extractErrorMessage } from "../../util/error";
import { WarehouseService } from "./warehouse.service";

/**
 * Shared warehouse actions (#6933), mirroring ComputingUnitActionsService:
 * create and delete sit behind one service so the confirm dialog and its
 * wording live in one place, shared with the workspace picker once it lands
 * (#7817).
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

  /** Creates a warehouse, so create and delete both sit behind this service. */
  create(name: string): Observable<DashboardWarehouse> {
    return this.warehouseService.createWarehouse(name);
  }

  /**
   * Asks for confirmation, then deletes the warehouse and all data stored in it.
   * Runs `onDeleted` after a successful delete so the caller can refresh its
   * list (warehouses have no push stream to do that for them, unlike computing
   * units).
   *
   * `nzOnOk` returns a promise so the dialog keeps its OK button spinning until
   * the request settles: deleting a warehouse that still holds data waits out
   * Lakekeeper's asynchronous purge (#7742), which can take tens of seconds,
   * and closing the dialog immediately would read as a frozen row.
   */
  confirmAndDelete(warehouse: DashboardWarehouse, onDeleted: () => void): void {
    this.modalService.confirm({
      nzTitle: `Delete warehouse "${warehouse.name}"?`,
      nzContent: "This permanently deletes the warehouse and all data stored in it.",
      nzOkText: "Delete",
      nzOkDanger: true,
      // Resolves on failure too: the error is reported as a toast, and leaving the
      // confirm dialog open on top of it would just have to be dismissed twice.
      nzOnOk: () =>
        firstValueFrom(this.warehouseService.deleteWarehouse(warehouse.whid))
          .then(() => {
            this.notificationService.success("Warehouse deleted.");
            onDeleted();
          })
          .catch((err: unknown) => {
            this.notificationService.error(`Failed to delete warehouse: ${extractErrorMessage(err)}`);
          }),
    });
  }
}
