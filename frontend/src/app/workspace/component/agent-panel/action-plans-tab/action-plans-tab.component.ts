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

import { Component, OnInit, OnDestroy } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { ActionPlan, ActionPlanService } from "../../../service/action-plan/action-plan.service";
import { AgentActionProgressDisplayService } from "../../../service/copilot/agent-action-progress-display.service";

@UntilDestroy()
@Component({
  selector: "texera-action-plans-tab",
  templateUrl: "./action-plans-tab.component.html",
  styleUrls: ["./action-plans-tab.component.scss"],
})
export class ActionPlansTabComponent implements OnInit, OnDestroy {
  public actionPlans: ActionPlan[] = [];

  constructor(
    private actionPlanService: ActionPlanService,
    private agentActionProgressDisplayService: AgentActionProgressDisplayService
  ) {}

  ngOnInit(): void {
    // Subscribe to action plans updates
    this.actionPlanService
      .getActionPlansStream()
      .pipe(untilDestroyed(this))
      .subscribe(plans => {
        // Sort plans by creation date (newest first)
        this.actionPlans = [...plans].sort((a, b) => b.createdAt.getTime() - a.createdAt.getTime());
      });
  }

  ngOnDestroy(): void {
    // Cleanup handled by UntilDestroy decorator
  }

  /**
   * Delete an action plan
   */
  public deleteActionPlan(planId: string, event: Event): void {
    event.stopPropagation();
    if (confirm("Are you sure you want to delete this action plan?")) {
      this.actionPlanService.deleteActionPlan(planId);
    }
  }

  /**
   * Clear all action plans
   */
  public clearAllActionPlans(): void {
    if (confirm("Are you sure you want to clear all action plans?")) {
      this.actionPlanService.clearAllActionPlans();
    }
  }

  /**
   * Handle hovering over an action plan
   */
  public onPlanHover(plan: ActionPlan): void {
    // Show temporary progress for the hovered plan
    this.agentActionProgressDisplayService.showTemporaryPlanProgress(plan);
  }

  /**
   * Handle unhovering from an action plan
   */
  public onPlanUnhover(): void {
    // Restore the current in-progress plan (if any)
    this.agentActionProgressDisplayService.restoreCurrentPlanProgress();
  }
}
