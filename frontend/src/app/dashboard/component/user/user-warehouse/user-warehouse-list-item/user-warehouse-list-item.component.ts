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

import { Component, EventEmitter, Input, Output } from "@angular/core";
import { DashboardWarehouse } from "../../../../../common/type/warehouse";
import { formatRelativeTime } from "../../../../../common/util/format.util";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzCardComponent } from "ng-zorro-antd/card";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzRowDirective, NzColDirective } from "ng-zorro-antd/grid";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";

/**
 * One warehouse row of the dashboard tab (#6933), mirroring
 * UserComputingUnitListItemComponent: the row only emits `deleted`; the
 * containing list owns the confirmation dialog.
 */
@Component({
  selector: "texera-user-warehouse-list-item",
  templateUrl: "./user-warehouse-list-item.component.html",
  styleUrls: ["./user-warehouse-list-item.component.scss"],
  imports: [
    NzCardComponent,
    NzRowDirective,
    NzColDirective,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    NzTooltipDirective,
  ],
})
export class UserWarehouseListItemComponent {
  private _warehouse?: DashboardWarehouse;
  @Output() deleted = new EventEmitter<void>();

  @Input()
  get warehouse(): DashboardWarehouse {
    if (!this._warehouse) {
      throw new Error("warehouse property must be provided to UserWarehouseListItemComponent.");
    }
    return this._warehouse;
  }

  set warehouse(value: DashboardWarehouse) {
    this._warehouse = value;
  }

  formatRelativeTime = formatRelativeTime;
}
