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
import { FormsModule } from "@angular/forms";
import { DatePipe, NgFor, NgIf } from "@angular/common";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzCardComponent } from "ng-zorro-antd/card";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzModalComponent, NzModalContentDirective, NzModalService } from "ng-zorro-antd/modal";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";

import { NotificationService } from "../../../../common/service/notification/notification.service";
import { WarehouseService } from "../../../../common/service/warehouse/warehouse.service";
import { DashboardWarehouse } from "../../../../common/type/warehouse";
import { extractErrorMessage } from "../../../../common/util/error";

/**
 * Dashboard page for per-user warehouses (#6933): list the caller's warehouses,
 * create one (Local flavor), delete one. Reachable only while the deployment
 * reports the feature enabled; the page re-checks and says so otherwise.
 */
@UntilDestroy()
@Component({
  selector: "texera-user-warehouse",
  templateUrl: "./user-warehouse.component.html",
  styleUrls: ["./user-warehouse.component.scss"],
  imports: [
    NgIf,
    NgFor,
    DatePipe,
    FormsModule,
    NzButtonComponent,
    NzCardComponent,
    NzIconDirective,
    NzInputDirective,
    NzModalComponent,
    NzModalContentDirective,
    NzTooltipDirective,
  ],
})
export class UserWarehouseComponent implements OnInit {
  warehouseEnabled = false;
  warehouses: DashboardWarehouse[] = [];

  createModalVisible = false;
  newWarehouseName = "";
  creating = false;

  constructor(
    private warehouseService: WarehouseService,
    private notificationService: NotificationService,
    private modalService: NzModalService
  ) {}

  ngOnInit(): void {
    this.refresh();
  }

  private refresh(): void {
    this.warehouseService
      .getStatus()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: status => {
          this.warehouseEnabled = status.enabled;
          this.warehouses = [...status.warehouses];
        },
        error: (err: unknown) => {
          console.error("Failed to fetch warehouses", err);
          this.notificationService.error("Failed to fetch warehouses.");
        },
      });
  }

  showCreateModal(): void {
    this.newWarehouseName = "";
    this.createModalVisible = true;
  }

  closeCreateModal(): void {
    this.createModalVisible = false;
  }

  createWarehouse(): void {
    const name = this.newWarehouseName.trim();
    if (!name || this.creating) {
      return;
    }
    this.creating = true;
    this.warehouseService
      .createWarehouse(name)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: created => {
          this.creating = false;
          this.createModalVisible = false;
          this.notificationService.success(`Warehouse "${created.name}" created.`);
          this.refresh();
        },
        error: (err: unknown) => {
          this.creating = false;
          this.notificationService.error(extractErrorMessage(err));
        },
      });
  }

  confirmDelete(warehouse: DashboardWarehouse): void {
    this.modalService.confirm({
      nzTitle: `Delete warehouse "${warehouse.name}"?`,
      nzContent: "This permanently deletes the warehouse and all data stored in it.",
      nzOkText: "Delete",
      nzOkDanger: true,
      nzOnOk: () => this.deleteWarehouse(warehouse.whid),
    });
  }

  private deleteWarehouse(whid: number): void {
    this.warehouseService
      .deleteWarehouse(whid)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.notificationService.success("Warehouse deleted.");
          this.refresh();
        },
        error: (err: unknown) => {
          this.notificationService.error(extractErrorMessage(err));
        },
      });
  }

  public trackByWhid(_idx: number, warehouse: DashboardWarehouse): number {
    return warehouse.whid;
  }
}
