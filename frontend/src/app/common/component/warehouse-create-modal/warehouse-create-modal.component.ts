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
import { WarehouseActionsService } from "../../service/warehouse/warehouse-actions.service";
import { DashboardWarehouse } from "../../type/warehouse";

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

  constructor(private warehouseActionsService: WarehouseActionsService) {}

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["visible"]?.currentValue === true) {
      this.newWarehouseName = "";
    }
  }

  // Mirrors the backend's VFSURIFactory.warehouseNamePattern (≤64 comes from
  // the input's maxlength), so an invalid name never leaves the dialog: the
  // Create button stays disabled and the Enter path returns early.
  private static readonly VALID_WAREHOUSE_NAME = /^[A-Za-z0-9][A-Za-z0-9_-]*$/;

  isValidWarehouseName(): boolean {
    return WarehouseCreateModalComponent.VALID_WAREHOUSE_NAME.test(this.newWarehouseName.trim());
  }

  /**
   * Mirrors ComputingUnitCreateModalComponent's submit flow: Create fires the
   * request and closes the dialog at once. The actions service owns the request
   * and its toasts, so the outcome arrives even if the user has navigated away;
   * this dialog only relays the created warehouse to its host while it is
   * still alive.
   */
  handleCreateWarehouseModalOk(): void {
    if (!this.isValidWarehouseName()) {
      return;
    }
    const name = this.newWarehouseName.trim();
    // The dialog stays clickable through its close animation; clearing the
    // name drops a second rapid click into the guard above instead of firing
    // a duplicate create.
    this.newWarehouseName = "";
    this.warehouseActionsService
      .create(name)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: created => this.warehouseCreated.emit(created),
        // The service reports failures and returns a stream that completes
        // empty; this guard only keeps a future contract change from
        // surfacing as an unhandled RxJS error.
        error: () => {},
      });
    this.closeModal();
  }

  handleCreateWarehouseModalCancel(): void {
    this.closeModal();
  }

  private closeModal(): void {
    this.visible = false;
    this.visibleChange.emit(false);
  }
}
