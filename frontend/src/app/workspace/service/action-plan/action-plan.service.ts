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
import { UndoRedoService } from "../undo-redo/undo-redo.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { Workflow, WorkflowContent } from "../../../common/type/workflow";

/**
 * Interface for an action plan highlight event
 */
export interface ActionPlanHighlight {
  operatorIds: string[];
  linkIds: string[];
  summary: string;
}

/**
 * Diff structure for operators (reusing workflow-version.service structure)
 */
type DifferentOpIDsList = {
  [key in "modified" | "added" | "deleted"]: string[];
};

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
  // Diff preview state
  private currentDiff: DifferentOpIDsList | null = null;
  private isInPreviewMode = false;
  private previewingVersionId: number | null = null;

  constructor(
    private workflowVersionService: WorkflowVersionService,
    private undoRedoService: UndoRedoService,
    private workflowPersistService: WorkflowPersistService,
    private workflowActionService: WorkflowActionService
) {}

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

    // Only emit to pending stream if both version IDs are set
    // Otherwise, wait for updateActionPlanAfterVersionId to be called
    if (beforeVersionId !== undefined && afterVersionId !== undefined) {
      console.log(`[ActionPlanService] Action plan created with both version IDs, emitting immediately: ${id}`);
      this.pendingActionPlanSubject.next(actionPlan);
    } else {
      console.log(
        `[ActionPlanService] Action plan created without full version IDs (before: ${beforeVersionId}, after: ${afterVersionId}), waiting for update`
      );
    }

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

      // If both version IDs are now set, emit to pending stream
      if (actionPlan.beforeVersionId !== undefined && actionPlan.afterVersionId !== undefined) {
        console.log(
          `[ActionPlanService] Action plan now has both version IDs (before: ${actionPlan.beforeVersionId}, after: ${actionPlan.afterVersionId}), emitting to pending stream`
        );
        this.pendingActionPlanSubject.next(actionPlan);
      }
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

  // ===== DIFF PREVIEW METHODS (using workflow-version.service) =====

  /**
   * Preview diff between any two versions.
   * Saves the AFTER version to temp and displays the BEFORE version on canvas
   * with highlights showing what changed (added/modified elements in green/orange,
   * deleted elements shown as red brackets on connected operators).
   *
   * @param wid Workflow ID
   * @param beforeVersionId Version ID to display on canvas
   * @param afterVersionId Version ID to save as temp (for accept/reject)
   * @returns Observable with diff
   */
  public previewVersionPairDiff(
    wid: number,
    beforeVersionId: number,
    afterVersionId: number
  ): Observable<DifferentOpIDsList> {
    // Fetch both versions
    const beforeWorkflow$ = this.workflowVersionService.retrieveWorkflowByVersion(wid, beforeVersionId);
    const afterWorkflow$ = this.workflowVersionService.retrieveWorkflowByVersion(wid, afterVersionId);

    return beforeWorkflow$.pipe(
      switchMap(beforeWorkflow => {
        return afterWorkflow$.pipe(
          map(afterWorkflow => {
            // Save modification state first
            this.workflowVersionService.saveModificationState();

            // Explicitly set temp workflow to AFTER version (v2) - this is what we'll restore on accept
            this.workflowActionService.setTempWorkflow(afterWorkflow);
            console.log("Before workflow", beforeWorkflow);
            console.log("After workflow", afterWorkflow);
            console.log("Current workflow", this.workflowActionService.getWorkflow());

            // Disable persist and undo/redo before reloading
            this.workflowPersistService.setWorkflowPersistFlag(false);
            this.undoRedoService.disableWorkFlowModification();

            // Display the BEFORE version (v1) on canvas as readonly
            this.workflowActionService.reloadWorkflow(beforeWorkflow);
            this.workflowActionService.disableWorkflowModification();

            // Calculate diff using workflow-version.service (after -> before)
            // This gives: added = elements in before but not after, deleted = elements in after but not before
            const diff = this.workflowVersionService.getWorkflowsDifference(afterWorkflow.content, beforeWorkflow.content);

            // Render highlights using workflow-version.service
            // Highlights on displayed before version: green for removed elements, orange for modified
            // Red brackets on temp after version: for added elements
            this.workflowVersionService.highlightOpVersionDiff(diff);

            // Store the current diff
            this.currentDiff = diff;

            // Track preview state
            this.isInPreviewMode = true;
            this.previewingVersionId = afterVersionId;

            console.log(`Previewing diff from version ${beforeVersionId} to ${afterVersionId}:`, diff);

            return diff;
          })
        );
      })
    );
  }

  /**
   * Preview action plan diff by action plan ID.
   * Fetches the action plan, then saves the AFTER version to temp and displays
   * the BEFORE version on canvas with highlights showing what changed.
   *
   * @param actionPlanId Action plan ID
   * @param wid Workflow ID
   * @returns Observable with diff for cleanup later
   */
  public previewActionPlanDiff(actionPlanId: string, wid: number): Observable<DifferentOpIDsList> {
    const actionPlan = this.actionPlans.get(actionPlanId);
    if (!actionPlan) {
      throw new Error(`Action plan ${actionPlanId} not found`);
    }

    if (!actionPlan.beforeVersionId || !actionPlan.afterVersionId) {
      throw new Error(`Action plan ${actionPlanId} is missing version IDs`);
    }

    console.log(
      `Previewing action plan ${actionPlanId}: v${actionPlan.beforeVersionId} -> v${actionPlan.afterVersionId}`
    );

    // Use the general version pair diff method
    return this.previewVersionPairDiff(wid, actionPlan.beforeVersionId, actionPlan.afterVersionId);
  }

  /**
   * Accept the current action plan.
   * Clears highlights and reloads the after version from temp (which was saved during preview).
   */
  public acceptActionPlan(): void {
    if (!this.currentDiff) {
      return;
    }

    // Clear highlights
    this.workflowVersionService.unhighlightOpVersionDiff(this.currentDiff);

    // Reload the after version from temp (similar to closeReadonlyWorkflowDisplay)
    // Enable modifications first to be able to reload
    this.workflowActionService.enableWorkflowModification();

    // Disable undo/redo to not capture the reload as an action
    this.undoRedoService.disableWorkFlowModification();

    // Reload the after version (v2) from temp
    this.workflowActionService.reloadWorkflow(this.workflowActionService.getTempWorkflow());

    // Clear temp workflow
    this.workflowActionService.resetTempWorkflow();

    // Re-enable undo/redo
    this.undoRedoService.enableWorkFlowModification();

    // Re-enable persist to DB
    this.workflowPersistService.setWorkflowPersistFlag(true);

    // Restore modification state
    this.workflowVersionService.restoreModificationState();

    // Clear preview state
    this.currentDiff = null;
    this.isInPreviewMode = false;
    this.previewingVersionId = null;

    console.log("Action plan accepted - after version loaded from temp");
  }

  /**
   * Reject the current action plan.
   * Keeps the before version (currently displayed) and makes it the new current workflow.
   */
  public rejectActionPlan(): void {
    if (!this.isInPreviewMode) {
      console.error("No action plan preview active to reject");
      return;
    }

    // Clear highlights using workflow-version.service
    if (this.currentDiff) {
      this.workflowVersionService.unhighlightOpVersionDiff(this.currentDiff);
      this.currentDiff = null;
    }

    // Keep the before version (currently displayed) as the new current version
    // Similar to revertToVersion logic in workflow-version.service

    // Clear undo/redo stacks since this is like reverting to a previous version
    this.undoRedoService.clearRedoStack();
    this.undoRedoService.clearUndoStack();

    // Enable workflow modifications (before version is currently readonly)
    this.workflowActionService.enableWorkflowModification();

    // Clear the temp workflow (don't reload it - keep the before version on canvas)
    this.workflowActionService.resetTempWorkflow();

    // Re-enable persist to DB
    this.workflowPersistService.setWorkflowPersistFlag(true);

    // Restore modification state
    this.workflowVersionService.restoreModificationState();

    // Clear preview state
    this.isInPreviewMode = false;
    this.previewingVersionId = null;

    console.log("Action plan rejected - before version kept as current workflow");
  }
}
