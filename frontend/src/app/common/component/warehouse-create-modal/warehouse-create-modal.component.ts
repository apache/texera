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
import { WarehouseActionsService } from "../../service/warehouse/warehouse-actions.service";
import { DashboardWarehouse } from "../../type/warehouse";
import { extractErrorMessage } from "../../util/error";

/**
 * Shared create-warehouse modal (#6933), embedded the same way
 * ComputingUnitCreateModalComponent is — two-way `[(visible)]` controls the
 * dialog and `(warehouseCreated)` returns the created warehouse — by the
 * dashboard tab today and by the workspace picker once it lands (#7817).
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

  constructor(
    private warehouseActionsService: WarehouseActionsService,
    private notificationService: NotificationService
  ) {}

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["visible"]?.currentValue === true) {
      this.newWarehouseName = "";
    }
  }

  /**
   * Mirrors ComputingUnitCreateModalComponent's submit flow: Create fires the
   * request and closes the dialog at once; the outcome arrives later as a toast
   * plus (warehouseCreated). There is no in-flight dialog state left to cancel,
   * so a create that lands after the close shows up visibly in the list instead
   * of surprising a retry with "already exists". Unlike the computing-unit
   * dialog, an empty name never reaches the request: the Create button is
   * disabled and the Enter path returns early, keeping the dialog open.
   */
  handleCreateWarehouseModalOk(): void {
    const name = this.newWarehouseName.trim();
    if (!name) {
      return;
    }
    this.createWarehouse(name);
    this.closeModal();
  }

  handleCreateWarehouseModalCancel(): void {
    this.closeModal();
  }

  private createWarehouse(name: string): void {
    this.warehouseActionsService
      .create(name)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: created => {
          this.notificationService.success(`Warehouse "${created.name}" created.`);
          this.warehouseCreated.emit(created);
        },
        error: (err: unknown) => {
          this.notificationService.error(`Failed to create warehouse: ${extractErrorMessage(err)}`);
        },
      });
  }

  private closeModal(): void {
    this.visible = false;
    this.visibleChange.emit(false);
  }
}
