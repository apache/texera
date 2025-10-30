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

import { Component, Input, OnInit, OnDestroy } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import {
  ActionPlan,
  ActionPlanStatus,
  ActionPlanService,
} from "../../service/action-plan/action-plan.service";

@UntilDestroy()
@Component({
  selector: "texera-action-plan-view",
  templateUrl: "./action-plan-view.component.html",
  styleUrls: ["./action-plan-view.component.scss"],
})
export class ActionPlanViewComponent implements OnInit, OnDestroy {
  @Input() actionPlan!: ActionPlan;
  @Input() showFeedbackControls: boolean = false; // Show accept/reject buttons

  public rejectMessage: string = "";
  public ActionPlanStatus = ActionPlanStatus; // Expose enum to template

  // Track task completion states
  public taskCompletionStates: { [operatorId: string]: boolean } = {};

  constructor(private actionPlanService: ActionPlanService) {}

  ngOnInit(): void {
    if (!this.actionPlan) {
      console.error("ActionPlan is not provided!");
      return;
    }

    // Subscribe to task completion changes
    this.actionPlan.tasks.forEach(task => {
      this.taskCompletionStates[task.operatorId] = task.completed$.value;
      task.completed$.pipe(untilDestroyed(this)).subscribe(completed => {
        this.taskCompletionStates[task.operatorId] = completed;
      });
    });
  }

  ngOnDestroy(): void {
    // Cleanup handled by UntilDestroy decorator
  }

  /**
   * User accepted the action plan
   */
  public onAccept(): void {
    this.actionPlanService.acceptPlan(this.actionPlan.id);
  }

  /**
   * User rejected the action plan with optional feedback
   */
  public onReject(): void {
    const message = this.rejectMessage.trim() || "I don't want this action plan.";
    this.actionPlanService.rejectPlan(message, this.actionPlan.id);
    this.rejectMessage = "";
  }

  /**
   * Get status label for display
   */
  public getStatusLabel(): string {
    const status = this.actionPlan.status$.value;
    switch (status) {
      case ActionPlanStatus.PENDING:
        return "Pending Approval";
      case ActionPlanStatus.ACCEPTED:
        return "In Progress";
      case ActionPlanStatus.REJECTED:
        return "Rejected";
      case ActionPlanStatus.COMPLETED:
        return "Completed";
      default:
        return status;
    }
  }

  /**
   * Get status color for display
   */
  public getStatusColor(): string {
    const status = this.actionPlan.status$.value;
    switch (status) {
      case ActionPlanStatus.PENDING:
        return "warning";
      case ActionPlanStatus.ACCEPTED:
        return "processing";
      case ActionPlanStatus.REJECTED:
        return "error";
      case ActionPlanStatus.COMPLETED:
        return "success";
      default:
        return "default";
    }
  }

  /**
   * Get progress percentage
   */
  public getProgressPercentage(): number {
    if (this.actionPlan.tasks.length === 0) return 0;
    const completedCount = this.actionPlan.tasks.filter(t => t.completed$.value).length;
    return Math.round((completedCount / this.actionPlan.tasks.length) * 100);
  }
}
