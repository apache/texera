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

import { Component, EventEmitter, Input, OnChanges, Output, SimpleChanges } from "@angular/core";
import { FormsModule } from "@angular/forms";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzModalComponent } from "ng-zorro-antd/modal";
import { NotificationService } from "../../service/notification/notification.service";
import { WarehouseService } from "../../service/warehouse/warehouse.service";
import { DashboardWarehouse } from "../../type/warehouse";
import { extractErrorMessage } from "../../util/error";

/**
 * Shared create-warehouse modal (#6933), embedded by the dashboard tab and the
 * workspace picker the same way ComputingUnitCreateModalComponent is: two-way
 * `[(visible)]` controls the dialog and `(warehouseCreated)` returns the
 * created warehouse.
 */
@UntilDestroy()
@Component({
  selector: "texera-warehouse-create-modal",
  templateUrl: "./warehouse-create-modal.component.html",
  styleUrls: ["./warehouse-create-modal.component.scss"],
  imports: [
    FormsModule,
    NzModalComponent,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NzInputDirective,
  ],
})
export class WarehouseCreateModalComponent implements OnChanges {
  // Must be bound two-way ([(visible)]): the modal closes itself.
  @Input() visible = false;
  @Output() visibleChange = new EventEmitter<boolean>();
  @Output() warehouseCreated = new EventEmitter<DashboardWarehouse>();

  newWarehouseName = "";
  creating = false;

  constructor(
    private warehouseService: WarehouseService,
    private notificationService: NotificationService
  ) {}

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["visible"] && this.visible) {
      this.newWarehouseName = "";
    }
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
          this.notificationService.success(`Warehouse "${created.name}" created.`);
          this.warehouseCreated.emit(created);
          this.closeModal();
        },
        error: (err: unknown) => {
          // Keep the modal open so the name can be corrected.
          this.creating = false;
          this.notificationService.error(extractErrorMessage(err));
        },
      });
  }

  handleCreateWarehouseModalCancel(): void {
    this.closeModal();
  }

  private closeModal(): void {
    this.visible = false;
    this.visibleChange.emit(false);
  }
}
