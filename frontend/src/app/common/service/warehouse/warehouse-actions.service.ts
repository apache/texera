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
import { NotificationService } from "../notification/notification.service";
import { DashboardWarehouse } from "../../type/warehouse";
import { extractErrorMessage } from "../../util/error";
import { WarehouseService } from "./warehouse.service";

/**
 * Shared warehouse actions (#6933), mirroring ComputingUnitActionsService: the
 * dashboard tab and the workspace picker both delete warehouses, so the confirm
 * dialog and its wording live in one place.
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
   * Asks for confirmation, then deletes the warehouse and all data stored in it.
   * Runs `onDeleted` after a successful delete so the caller can refresh its
   * list (warehouses have no push stream to do that for them, unlike computing
   * units).
   */
  confirmAndDelete(warehouse: DashboardWarehouse, onDeleted: () => void): void {
    this.modalService.confirm({
      nzTitle: `Delete warehouse "${warehouse.name}"?`,
      nzContent: "This permanently deletes the warehouse and all data stored in it.",
      nzOkText: "Delete",
      nzOkDanger: true,
      nzOnOk: () => {
        this.warehouseService.deleteWarehouse(warehouse.whid).subscribe({
          next: () => {
            this.notificationService.success("Warehouse deleted.");
            onDeleted();
          },
          error: (err: unknown) => {
            this.notificationService.error(extractErrorMessage(err));
          },
        });
      },
    });
  }
}
