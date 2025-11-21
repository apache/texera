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

import { Component, Input, Output, EventEmitter, OnInit } from "@angular/core";
import { ActionPlan, ActionPlanStatus } from "../../service/action-plan/action-plan.service";

@Component({
  selector: "texera-action-plan-view",
  templateUrl: "./action-plan-view.component.html",
  styleUrls: ["./action-plan-view.component.scss"],
})
export class ActionPlanViewComponent implements OnInit {
  @Input() actionPlan!: ActionPlan;
  @Input() showFeedbackControls: boolean = false;
  @Output() userDecision = new EventEmitter<{
    accepted: boolean;
    message: string;
    createNewActor?: boolean;
    planId?: string;
  }>();

  public rejectMessage: string = "";
  public runInNewAgent: boolean = false;
  public ActionPlanStatus = ActionPlanStatus;

  constructor() {}

  ngOnInit(): void {
    // Component initialization
  }

  /**
   * Handle user acceptance of the action plan.
   */
  public onAccept(): void {
    const userFeedback = this.rejectMessage.trim();
    this.userDecision.emit({
      accepted: true,
      message: `Action plan ${this.actionPlan.id} accepted. Feedback: ${userFeedback}`,
      createNewActor: this.runInNewAgent,
      planId: this.actionPlan.id,
    });
  }

  /**
   * Handle user rejection with optional feedback message.
   */
  public onReject(): void {
    const userFeedback = this.rejectMessage.trim();

    this.userDecision.emit({
      accepted: false,
      message: `Action plan ${this.actionPlan.id} rejected. Feedback: ${userFeedback}`,
      planId: this.actionPlan.id,
    });

    this.rejectMessage = "";
  }

  /**
   * Get display label for current plan status.
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
   * Get display color for current plan status.
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
}
