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
import { Subject, takeUntil } from "rxjs";
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
  creating = false;

  // Closing the dialog ends the attempt it was showing. Without this the request
  // outlives the dialog (the component itself is never torn down), so Cancel
  // would still create the warehouse and a late response would close — and
  // discard — whatever the user had typed after reopening.
  private readonly closed$ = new Subject<void>();

  constructor(
    private warehouseActionsService: WarehouseActionsService,
    private notificationService: NotificationService
  ) {}

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["visible"]?.currentValue === true) {
      this.newWarehouseName = "";
      // Cancelling mid-flight leaves creating set; without this the Create button
      // reopens stuck in its loading state.
      this.creating = false;
    } else if (changes["visible"]?.currentValue === false) {
      // The host can also close the dialog by flipping [(visible)] itself; that
      // close must abandon the in-flight attempt exactly like Cancel does, or a
      // late response would close — and discard — a reopened dialog.
      this.closed$.next();
    }
  }

  createWarehouse(): void {
    const name = this.newWarehouseName.trim();
    if (!name || this.creating) {
      return;
    }
    this.creating = true;
    this.warehouseActionsService
      .create(name)
      .pipe(takeUntil(this.closed$), untilDestroyed(this))
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
          this.notificationService.error(`Failed to create warehouse: ${extractErrorMessage(err)}`);
        },
      });
  }

  handleCreateWarehouseModalCancel(): void {
    this.closeModal();
  }

  private closeModal(): void {
    this.closed$.next();
    this.creating = false;
    this.visible = false;
    this.visibleChange.emit(false);
  }
}
