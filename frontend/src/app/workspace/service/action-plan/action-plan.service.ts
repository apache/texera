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
import { Subject, Observable, BehaviorSubject } from "rxjs";
import { filter, take, switchMap, map } from "rxjs/operators";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { OperatorPredicate, OperatorLink } from "../../types/workflow-common.interface";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";

/**
 * Interface for an action plan highlight event
 */
export interface ActionPlanHighlight {
  operatorIds: string[];
  linkIds: string[];
  summary: string;
}

/**
 * Operations performed in an action plan
 */
export interface ActionPlanOperations {
  add: {
    operatorIds: string[];
    linkIds: string[];
  };
  modify: {
    operatorIds: string[];
  };
  delete: {
    operatorIds: string[];
    linkIds: string[];
  };
}

/**
 * Workflow snapshot for saving pre-action-plan state
 */
export interface WorkflowSnapshot {
  operators: OperatorPredicate[]; // Snapshot of operators to restore (for delete operations)
  links: OperatorLink[]; // Snapshot of links to restore (for delete operations)
  operatorProperties: Map<string, any>; // Map of operator ID to original properties (for modify operations)
}

/**
 * Complete Action Plan data structure
 */
export interface ActionPlan {
  id: string; // Unique identifier for the action plan
  agentId: string; // ID of the agent that created this plan
  agentName: string; // Name of the agent
  executorAgentId: string; // ID of the agent that will execute/handle feedback for this plan (can be different from creator)
  summary: string; // Overall summary of the action plan
  operations: ActionPlanOperations; // Operations performed (add/modify/delete)
  createdAt: Date; // Creation timestamp
  operatorIds: string[]; // For highlighting
  linkIds: string[]; // For highlighting
  beforeVersionId?: number; // Workflow version ID before the action plan was applied
  afterVersionId?: number; // Workflow version ID after the action plan was applied
}

/**
 * Service to manage action plans, highlights, and user feedback
 * Handles the interactive flow: show plan -> wait for user decision -> execute -> track progress
 */
@Injectable({
  providedIn: "root",
})
export class ActionPlanService {
  private actionPlanHighlightSubject = new Subject<ActionPlanHighlight>();
  private cleanupSubject = new Subject<void>();

  // Action plan storage
  private actionPlans = new Map<string, ActionPlan>();
  private actionPlansSubject = new BehaviorSubject<ActionPlan[]>([]);
  private pendingActionPlanSubject = new BehaviorSubject<ActionPlan | null>(null);

  // Workflow persisted event stream
  private workflowPersistedSubject = new Subject<number>(); // Emits wid

  private workflowActionService?: WorkflowActionService;

  constructor(private workflowVersionService: WorkflowVersionService) {}

  /**
   * Set the workflow action service (injected later to avoid circular dependency)
   */
  public setWorkflowActionService(service: WorkflowActionService): void {
    this.workflowActionService = service;
  }

  /**
   * Get action plan highlight stream
   */
  public getActionPlanHighlightStream() {
    return this.actionPlanHighlightSubject.asObservable();
  }

  /**
   * Get cleanup stream - emits when user provides feedback (accept/reject)
   */
  public getCleanupStream() {
    return this.cleanupSubject.asObservable();
  }

  /**
   * Get workflow persisted event stream
   */
  public onWorkflowPersisted(): Observable<number> {
    return this.workflowPersistedSubject.asObservable();
  }

  /**
   * Notify that a workflow has been persisted
   * This triggers listeners to update afterVersionId
   */
  public notifyWorkflowPersisted(wid: number): void {
    this.workflowPersistedSubject.next(wid);
  }

  /**
   * Wait for the workflow to be persisted and return the new version ID
   * Returns an Observable that emits once when persistence completes
   */
  public waitForWorkflowPersisted(wid: number): Observable<number | undefined> {
    return this.onWorkflowPersisted().pipe(
      filter(persistedWid => persistedWid === wid),
      take(1), // Auto-cleanup after first event
      switchMap(() => this.workflowVersionService.retrieveVersionsOfWorkflow(wid)),
      map(versions => {
        const afterVersionId = versions.length > 0 ? versions[0].vId : undefined;
        console.log("afterVersionId: ", afterVersionId);
        return afterVersionId;
      })
    );
  }

  /**
   * Get all action plans as observable
   */
  public getActionPlansStream(): Observable<ActionPlan[]> {
    return this.actionPlansSubject.asObservable();
  }

  /**
   * Get pending action plan stream (for showing in agent chat)
   */
  public getPendingActionPlanStream(): Observable<ActionPlan | null> {
    return this.pendingActionPlanSubject.asObservable();
  }

  /**
   * Get all action plans
   */
  public getAllActionPlans(): ActionPlan[] {
    return Array.from(this.actionPlans.values());
  }

  /**
   * Get a specific action plan by ID
   */
  public getActionPlan(id: string): ActionPlan | undefined {
    return this.actionPlans.get(id);
  }

  /**
   * Create a new action plan
   */
  public createActionPlan(
    agentId: string,
    agentName: string,
    summary: string,
    operations: ActionPlanOperations,
    operatorIds: string[],
    linkIds: string[],
    executorAgentId?: string, // Optional: defaults to agentId if not specified
    beforeVersionId?: number, // Optional: workflow version ID before changes
    afterVersionId?: number // Optional: workflow version ID after changes
  ): ActionPlan {
    const id = this.generateId();

    const actionPlan: ActionPlan = {
      id,
      agentId,
      agentName,
      executorAgentId: executorAgentId || agentId, // Default to creator if not specified
      summary,
      operations,
      createdAt: new Date(),
      operatorIds,
      linkIds,
      beforeVersionId,
      afterVersionId,
    };

    this.actionPlans.set(id, actionPlan);
    this.emitActionPlans();
    this.pendingActionPlanSubject.next(actionPlan);

    // Emit highlight event for the workflow editor
    this.actionPlanHighlightSubject.next({ operatorIds, linkIds, summary });

    return actionPlan;
  }

  /**
   * Update the afterVersionId of an existing action plan
   */
  public updateActionPlanAfterVersionId(actionPlanId: string, afterVersionId: number): void {
    const actionPlan = this.actionPlans.get(actionPlanId);
    if (actionPlan) {
      actionPlan.afterVersionId = afterVersionId;
      this.emitActionPlans();
    }
  }

  /**
   * Delete an action plan
   */
  public deleteActionPlan(id: string): boolean {
    if (this.actionPlans.has(id)) {
      this.actionPlans.delete(id);
      this.emitActionPlans();
      return true;
    }
    return false;
  }

  /**
   * Clear all action plans
   */
  public clearAllActionPlans(): void {
    this.actionPlans.clear();
    this.emitActionPlans();
  }

  /**
   * Generate a unique ID for action plans
   */
  private generateId(): string {
    return `action-plan-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
  }

  /**
   * Emit the current list of action plans
   */
  private emitActionPlans(): void {
    this.actionPlansSubject.next(this.getAllActionPlans());
  }

  /**
   * Revert an action plan by restoring the workflow to the version before the action plan
   * This uses the beforeVersionId to restore the workflow state
   */
  public revertActionPlan(actionPlanId: string): boolean {
    const actionPlan = this.actionPlans.get(actionPlanId);
    if (!actionPlan || !this.workflowActionService) {
      console.error(`Cannot revert action plan ${actionPlanId}: plan or workflow service not found`);
      return false;
    }

    // Reversion should be handled by loading the beforeVersionId
    // This is now managed by the WorkflowVersionService
    console.log(`Revert action plan ${actionPlanId} to version ${actionPlan.beforeVersionId}`);
    return true;
  }
}
