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

import { Component, inject } from "@angular/core";
import { CommonModule } from "@angular/common";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzModalRef } from "ng-zorro-antd/modal";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzIconModule } from "ng-zorro-antd/icon";
import {
  QuickStepExecutorService,
  QuickStepRunState,
  StepStatus,
} from "../../service/quick-step/quick-step-executor.service";

@UntilDestroy()
@Component({
  selector: "texera-quick-step-progress",
  templateUrl: "./quick-step-progress.component.html",
  styleUrls: ["./quick-step-progress.component.scss"],
  imports: [CommonModule, NzButtonModule, NzIconModule],
})
export class QuickStepProgressComponent {
  protected readonly modal = inject(NzModalRef);
  public state: QuickStepRunState | null = null;

  constructor(private executor: QuickStepExecutorService) {
    this.executor
      .state$()
      .pipe(untilDestroyed(this))
      .subscribe(state => {
        this.state = state;
        if (state === null) {
          this.modal.close();
        }
      });
  }

  public get isRunning(): boolean {
    return !!this.state?.active;
  }

  public get isDone(): boolean {
    if (!this.state) return false;
    return !this.state.active;
  }

  public iconFor(status: StepStatus): string {
    switch (status) {
      case "completed":
        return "check-circle";
      case "running":
        return "loading";
      case "failed":
        return "close-circle";
      case "cancelled":
      case "skipped":
        return "minus-circle";
      default:
        return "clock-circle";
    }
  }

  public themeFor(status: StepStatus): "outline" | "twotone" {
    return status === "running" || status === "completed" ? "twotone" : "outline";
  }

  public classFor(status: StepStatus): string {
    return `step step--${status}`;
  }

  public cancel(): void {
    this.executor.cancel();
  }

  public close(): void {
    this.executor.dismiss();
    this.modal.close();
  }
}
