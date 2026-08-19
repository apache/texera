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
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

import { ɵɵCdkVirtualScrollViewport, ɵɵCdkFixedSizeVirtualScroll, ɵɵCdkVirtualForOf } from "@angular/cdk/overlay";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzCardComponent } from "ng-zorro-antd/card";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzListComponent } from "ng-zorro-antd/list";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";

import { WarehouseCreateModalComponent } from "../../../../common/component/warehouse-create-modal/warehouse-create-modal.component";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { WarehouseActionsService } from "../../../../common/service/warehouse/warehouse-actions.service";
import { WarehouseService } from "../../../../common/service/warehouse/warehouse.service";
import { DashboardWarehouse } from "../../../../common/type/warehouse";
import { UserWarehouseListItemComponent } from "./user-warehouse-list-item/user-warehouse-list-item.component";

/**
 * Dashboard page for per-user warehouses (#6933), mirroring
 * UserComputingUnitComponent: list the caller's warehouses, create one (Local
 * flavor), delete one. Reachable only while the deployment reports the feature
 * enabled; the page re-checks and says so otherwise.
 */
@UntilDestroy()
@Component({
  selector: "texera-user-warehouse",
  templateUrl: "./user-warehouse.component.html",
  styleUrls: ["./user-warehouse.component.scss"],
  imports: [
    NgIf,
    NzCardComponent,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    ɵɵCdkVirtualScrollViewport,
    ɵɵCdkFixedSizeVirtualScroll,
    NzListComponent,
    ɵɵCdkVirtualForOf,
    UserWarehouseListItemComponent,
    WarehouseCreateModalComponent,
  ],
})
export class UserWarehouseComponent implements OnInit {
  warehouseEnabled = false;
  warehouses: DashboardWarehouse[] = [];

  // visibility of the shared create-warehouse modal
  addWarehouseModalVisible = false;

  constructor(
    private warehouseService: WarehouseService,
    private notificationService: NotificationService,
    private warehouseActionsService: WarehouseActionsService
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

  deleteWarehouse(warehouse: DashboardWarehouse): void {
    this.warehouseActionsService.confirmAndDelete(warehouse, () => this.refresh());
  }

  showAddWarehouseModalVisible(): void {
    this.addWarehouseModalVisible = true;
  }

  onWarehouseCreated(): void {
    this.refresh();
  }
}
