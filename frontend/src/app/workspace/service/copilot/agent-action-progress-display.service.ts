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

import { Injectable } from "@angular/core";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { ActionPlan, ActionPlanTask, ActionPlanStatus, ActionPlanService } from "../action-plan/action-plan.service";
import { Subject, Subscription } from "rxjs";
import { untilDestroyed } from "@ngneat/until-destroy";

/**
 * Service to display agent action progress on operators
 * Shows agent names and progress indicators on operators during action plan execution
 */
@Injectable({
  providedIn: "root",
})
export class AgentActionProgressDisplayService {
  private currentlyDisplayedPlan: ActionPlan | null = null;
  private hideProgressTimeout: NodeJS.Timeout | null = null;
  private subscriptions: Subscription[] = [];

  constructor(
    private workflowActionService: WorkflowActionService,
    private actionPlanService: ActionPlanService
  ) {
    this.initializeStatusMonitoring();
  }

  /**
   * Initialize monitoring of action plan statuses
   * Automatically shows/hides progress based on plan status
   */
  private initializeStatusMonitoring(): void {
    // Monitor for any accepted plans to show progress
    this.actionPlanService.getActionPlansStream().subscribe(plans => {
      const acceptedPlan = plans.find(plan => plan.status$.value === ActionPlanStatus.ACCEPTED);

      if (acceptedPlan && this.currentlyDisplayedPlan?.id !== acceptedPlan.id) {
        this.showPlanProgress(acceptedPlan);
      } else if (!acceptedPlan && this.currentlyDisplayedPlan) {
        // Check if current plan is completed
        const currentPlan = plans.find(p => p.id === this.currentlyDisplayedPlan?.id);
        if (currentPlan && currentPlan.status$.value === ActionPlanStatus.COMPLETED) {
          this.scheduleHideProgress(5000);
        }
      }
    });
  }

  /**
   * Show progress indicators for a specific action plan
   */
  public showPlanProgress(plan: ActionPlan): void {
    // Clear any existing display
    this.clearProgressDisplay();

    if (this.hideProgressTimeout) {
      clearTimeout(this.hideProgressTimeout);
      this.hideProgressTimeout = null;
    }

    this.currentlyDisplayedPlan = plan;
    const jointWrapper = this.workflowActionService.getJointGraphWrapper();

    // Display progress for each task in the plan
    plan.tasks.forEach((task, operatorId) => {
      if (task.agentId) {
        const agentName = this.getAgentName(task.agentId, plan);

        // Subscribe to task completion status
        const subscription = task.completed$.subscribe(isCompleted => {
          jointWrapper.setAgentActionProgress(operatorId, agentName, isCompleted);
        });

        this.subscriptions.push(subscription);

        // Set initial state
        jointWrapper.setAgentActionProgress(operatorId, agentName, task.completed$.value);
      }
    });
  }

  /**
   * Show a temporary plan progress (e.g., on hover)
   */
  public showTemporaryPlanProgress(plan: ActionPlan): void {
    // Cancel any scheduled hide
    if (this.hideProgressTimeout) {
      clearTimeout(this.hideProgressTimeout);
      this.hideProgressTimeout = null;
    }

    this.showPlanProgress(plan);
  }

  /**
   * Restore the current in-progress plan display
   */
  public restoreCurrentPlanProgress(): void {
    // Find any accepted plan
    const plans = this.actionPlanService.getAllActionPlans();
    const acceptedPlan = plans.find(plan => plan.status$.value === ActionPlanStatus.ACCEPTED);

    if (acceptedPlan) {
      this.showPlanProgress(acceptedPlan);
    } else {
      this.clearProgressDisplay();
    }
  }

  /**
   * Clear all progress indicators
   */
  public clearProgressDisplay(): void {
    const jointWrapper = this.workflowActionService.getJointGraphWrapper();

    // Clear all subscriptions
    this.subscriptions.forEach(sub => sub.unsubscribe());
    this.subscriptions = [];

    // Clear all agent progress indicators
    if (this.currentlyDisplayedPlan) {
      this.currentlyDisplayedPlan.tasks.forEach((task, operatorId) => {
        jointWrapper.clearAgentActionProgress(operatorId);
      });
    }

    this.currentlyDisplayedPlan = null;
  }

  /**
   * Schedule hiding the progress after a delay
   */
  private scheduleHideProgress(delayMs: number): void {
    if (this.hideProgressTimeout) {
      clearTimeout(this.hideProgressTimeout);
    }

    this.hideProgressTimeout = setTimeout(() => {
      this.clearProgressDisplay();
      this.hideProgressTimeout = null;
    }, delayMs);
  }

  /**
   * Get agent name from agent ID
   */
  private getAgentName(agentId: string, plan: ActionPlan): string {
    // If the agent is the plan's creator
    if (agentId === plan.agentId) {
      return plan.agentName;
    }

    // Otherwise, use a default format
    return `Agent ${agentId}`;
  }
}
