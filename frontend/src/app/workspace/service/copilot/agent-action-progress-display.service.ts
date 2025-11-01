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
import { ActionPlan, ActionPlanStatus, ActionPlanService } from "../action-plan/action-plan.service";
import { Subscription } from "rxjs";

/**
 * Service to display agent action progress on operators
 * Shows agent names and progress indicators on operators during action plan execution
 */
@Injectable({
  providedIn: "root",
})
export class AgentActionProgressDisplayService {
  // Track subscriptions per plan ID
  private planSubscriptions: Map<string, Subscription[]> = new Map();

  constructor(
    private workflowActionService: WorkflowActionService,
    private actionPlanService: ActionPlanService
  ) {
    this.initializeMonitoring();
  }

  /**
   * Initialize monitoring of all action plans
   * Shows progress for all active plans
   */
  private initializeMonitoring(): void {
    this.actionPlanService.getActionPlansStream().subscribe(plans => {
      // Get all plan IDs currently being tracked
      const currentPlanIds = new Set(this.planSubscriptions.keys());
      const newPlanIds = new Set(plans.map(p => p.id));

      // Remove plans that no longer exist
      currentPlanIds.forEach(planId => {
        if (!newPlanIds.has(planId)) {
          this.clearPlanProgress(planId);
        }
      });

      // Update or add plans
      plans.forEach(plan => {
        // Only show progress for accepted plans
        if (plan.status$.value === ActionPlanStatus.ACCEPTED) {
          this.showPlanProgress(plan);
        } else {
          this.clearPlanProgress(plan.id);
        }
      });
    });
  }

  /**
   * Show progress indicators for a specific action plan
   */
  private showPlanProgress(plan: ActionPlan): void {
    // Clear existing subscriptions for this plan if any
    this.clearPlanProgress(plan.id);

    const jointWrapper = this.workflowActionService.getJointGraphWrapper();
    const subscriptions: Subscription[] = [];

    // Display progress for each task in the plan
    plan.tasks.forEach((task, operatorId) => {
      if (task.agentId) {
        const agentName = this.getAgentName(task.agentId, plan);

        // Subscribe to task completion status
        const subscription = task.completed$.subscribe(isCompleted => {
          jointWrapper.setAgentActionProgress(operatorId, agentName, isCompleted);
        });

        subscriptions.push(subscription);

        // Set initial state
        jointWrapper.setAgentActionProgress(operatorId, agentName, task.completed$.value);
      }
    });

    // Store subscriptions for this plan
    this.planSubscriptions.set(plan.id, subscriptions);
  }

  /**
   * Clear progress indicators for a specific plan
   */
  private clearPlanProgress(planId: string): void {
    const subscriptions = this.planSubscriptions.get(planId);
    if (subscriptions) {
      // Unsubscribe from all task completion observables
      subscriptions.forEach(sub => sub.unsubscribe());
      this.planSubscriptions.delete(planId);
    }

    // Clear the visual indicators on operators
    const plan = this.actionPlanService.getActionPlan(planId);
    if (plan) {
      const jointWrapper = this.workflowActionService.getJointGraphWrapper();
      plan.tasks.forEach((_task, operatorId) => {
        jointWrapper.clearAgentActionProgress(operatorId);
      });
    }
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
