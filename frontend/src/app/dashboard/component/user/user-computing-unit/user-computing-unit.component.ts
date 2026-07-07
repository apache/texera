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
import { ComputingUnitStatusService } from "../../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { DashboardEntry } from "../../../type/dashboard-entry";
import { DashboardWorkflowComputingUnit } from "../../../../common/type/workflow-computing-unit";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { UserService } from "../../../../common/service/user/user.service";
import { ComputingUnitActionsService } from "../../../../common/service/computing-unit/computing-unit-actions/computing-unit-actions.service";
import { interval } from "rxjs";
import { NzCardComponent } from "ng-zorro-antd/card";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { ɵɵCdkVirtualScrollViewport, ɵɵCdkFixedSizeVirtualScroll, ɵɵCdkVirtualForOf } from "@angular/cdk/overlay";
import { NzListComponent } from "ng-zorro-antd/list";
import { UserComputingUnitListItemComponent } from "./user-computing-unit-list-item/user-computing-unit-list-item.component";
import { ComputingUnitCreateModalComponent } from "../../../../common/component/computing-unit-create-modal/computing-unit-create-modal.component";

@UntilDestroy()
@Component({
  selector: "texera-computing-unit-section",
  templateUrl: "user-computing-unit.component.html",
  styleUrls: ["user-computing-unit.component.scss"],
  imports: [
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
    UserComputingUnitListItemComponent,
    ComputingUnitCreateModalComponent,
  ],
})
export class UserComputingUnitComponent implements OnInit {
  public entries: DashboardEntry[] = [];
  public isLogin = this.userService.isLogin();
  public currentUid = this.userService.getCurrentUser()?.uid;

  allComputingUnits: DashboardWorkflowComputingUnit[] = [];

  // visibility of the shared create-computing-unit modal
  addComputeUnitModalVisible = false;

  constructor(
    private notificationService: NotificationService,
    private userService: UserService,
    private computingUnitStatusService: ComputingUnitStatusService,
    private computingUnitActionsService: ComputingUnitActionsService
  ) {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.isLogin = this.userService.isLogin();
        this.currentUid = this.userService.getCurrentUser()?.uid;
      });
  }

  ngOnInit() {
    this.computingUnitStatusService
      .getAllComputingUnits()
      .pipe(untilDestroyed(this))
      .subscribe(units => {
        this.allComputingUnits = units;
        this.entries = units.map(u => new DashboardEntry(u));
      });

    interval(1000)
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.computingUnitStatusService.refreshComputingUnitList();
      });
  }

  terminateComputingUnit(cuid: number): void {
    const unit = this.allComputingUnits.find(u => u.computingUnit.cuid === cuid);

    if (!unit) {
      this.notificationService.error("Invalid computing unit.");
      return;
    }

    this.computingUnitActionsService.confirmAndTerminate(cuid, unit);
  }

  showAddComputeUnitModalVisible(): void {
    this.addComputeUnitModalVisible = true;
  }

  onComputingUnitCreated(): void {
    this.computingUnitStatusService.refreshComputingUnitList();
  }
}
