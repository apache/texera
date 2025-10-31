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

import { Component, Input, Output, EventEmitter, OnInit, OnDestroy } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { ActionPlan, ActionPlanStatus, ActionPlanTask } from "../../service/action-plan/action-plan.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import * as joint from "jointjs";

@UntilDestroy()
@Component({
  selector: "texera-action-plan-view",
  templateUrl: "./action-plan-view.component.html",
  styleUrls: ["./action-plan-view.component.scss"],
})
export class ActionPlanViewComponent implements OnInit, OnDestroy {
  @Input() actionPlan!: ActionPlan;
  @Input() showFeedbackControls: boolean = false; // Show accept/reject buttons
  @Output() userDecision = new EventEmitter<{
    accepted: boolean;
    message: string;
    createNewActor?: boolean;
    planId?: string;
  }>();

  public rejectMessage: string = "";
  public runInNewAgent: boolean = false; // Toggle for running in new agent
  public ActionPlanStatus = ActionPlanStatus; // Expose enum to template

  // Track task completion states
  public taskCompletionStates: { [operatorId: string]: boolean } = {};

  constructor(private workflowActionService: WorkflowActionService) {}

  ngOnInit(): void {
    if (!this.actionPlan) {
      console.error("ActionPlan is not provided!");
      return;
    }

    // Subscribe to task completion changes
    this.actionPlan.tasks.forEach((task, operatorId) => {
      this.taskCompletionStates[operatorId] = task.completed$.value;
      task.completed$.pipe(untilDestroyed(this)).subscribe(completed => {
        this.taskCompletionStates[operatorId] = completed;
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
    // Emit user decision with information about whether to create a new actor
    this.userDecision.emit({
      accepted: true,
      message: `✅ Accepted action plan: "${this.actionPlan.summary}"${this.runInNewAgent ? " (will run in new agent)" : ""}`,
      createNewActor: this.runInNewAgent,
      planId: this.actionPlan.id,
    });
  }

  /**
   * User rejected the action plan with optional feedback
   */
  public onReject(): void {
    const userFeedback = this.rejectMessage.trim() || "I don't want this action plan.";

    // Emit user decision event for chat component to handle
    this.userDecision.emit({
      accepted: false,
      message: `❌ Rejected action plan: "${this.actionPlan.summary}". Feedback: ${userFeedback}`,
      planId: this.actionPlan.id,
    });

    this.rejectMessage = "";
  }

  /**
   * Highlight an operator when clicking on its task
   */
  public highlightOperator(operatorId: string): void {
    // Get the operator from workflow
    const operator = this.workflowActionService.getTexeraGraph().getOperator(operatorId);
    if (!operator) {
      return;
    }

    // Get the joint graph wrapper to access the paper
    const jointGraphWrapper = this.workflowActionService.getJointGraphWrapper();
    if (!jointGraphWrapper) {
      return;
    }

    const paper = jointGraphWrapper.getMainJointPaper();
    const operatorElement = paper.getModelById(operatorId);

    if (operatorElement) {
      // Create a temporary highlight using Joint.js highlight API
      const operatorView = paper.findViewByModel(operatorElement);
      if (operatorView) {
        // Add light blue halo effect using joint.highlighters
        const highlighterNamespace = joint.highlighters;

        // Remove any existing highlight with same name
        highlighterNamespace.mask.remove(operatorView, "action-plan-click");

        // Add new highlight with light blue color
        highlighterNamespace.mask.add(operatorView, "body", "action-plan-click", {
          padding: 10,
          deep: true,
          attrs: {
            stroke: "#69b7ff",
            "stroke-width": 3,
            "stroke-opacity": 0.8,
            fill: "#69b7ff",
            "fill-opacity": 0.1,
          },
        });

        // Remove the highlight after 2 seconds
        setTimeout(() => {
          highlighterNamespace.mask.remove(operatorView, "action-plan-click");
        }, 2000);
      }
    }
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
   * Get tasks as array for template iteration
   */
  public get tasksArray(): ActionPlanTask[] {
    return Array.from(this.actionPlan.tasks.values());
  }

  /**
   * Get progress percentage
   */
  public getProgressPercentage(): number {
    if (this.actionPlan.tasks.size === 0) return 0;
    const tasksArray = Array.from(this.actionPlan.tasks.values());
    const completedCount = tasksArray.filter(t => t.completed$.value).length;
    return Math.round((completedCount / this.actionPlan.tasks.size) * 100);
  }
}
