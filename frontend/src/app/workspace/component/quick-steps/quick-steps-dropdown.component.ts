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

import { Component } from "@angular/core";
import { CommonModule } from "@angular/common";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzModalService } from "ng-zorro-antd/modal";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzIconModule } from "ng-zorro-antd/icon";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { NzMenuModule } from "ng-zorro-antd/menu";
import { NzTooltipModule } from "ng-zorro-antd/tooltip";
import { QuickStepService } from "../../../dashboard/service/user/quick-step/quick-step.service";
import { QuickStepExecutorService } from "../../service/quick-step/quick-step-executor.service";
import { QuickStep } from "../../../dashboard/type/quick-step.interface";
import { QuickStepProgressComponent } from "./quick-step-progress.component";
import { QuickStepEditorComponent } from "./quick-step-editor.component";
import { UserService } from "../../../common/service/user/user.service";

@UntilDestroy()
@Component({
  selector: "texera-quick-steps-dropdown",
  templateUrl: "./quick-steps-dropdown.component.html",
  styleUrls: ["./quick-steps-dropdown.component.scss"],
  imports: [
    CommonModule,
    NzButtonModule,
    NzIconModule,
    NzDropDownModule,
    NzMenuModule,
    NzTooltipModule,
  ],
})
export class QuickStepsDropdownComponent {
  public quickSteps: QuickStep[] = [];

  constructor(
    private quickStepService: QuickStepService,
    private executor: QuickStepExecutorService,
    private modalService: NzModalService,
    private userService: UserService
  ) {
    this.quickStepService
      .list$()
      .pipe(untilDestroyed(this))
      .subscribe(steps => (this.quickSteps = steps));
  }

  public run(quickStep: QuickStep): void {
    this.openProgressPanel();
    void this.executor.run(quickStep);
  }

  public openCreate(): void {
    const author = this.userService.getCurrentUser()?.name ?? "you";
    this.modalService.create({
      nzTitle: "Create Quick Step",
      nzContent: QuickStepEditorComponent,
      nzData: { author },
      nzFooter: null,
      nzWidth: 640,
    });
  }

  private openProgressPanel(): void {
    if (this.modalService.openModals.some(m => m.componentInstance instanceof QuickStepProgressComponent)) {
      return;
    }
    this.modalService.create({
      nzContent: QuickStepProgressComponent,
      nzFooter: null,
      nzClosable: false,
      nzMaskClosable: false,
      nzWidth: 520,
    });
  }
}
