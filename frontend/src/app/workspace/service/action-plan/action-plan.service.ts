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
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { OperatorPredicate, OperatorLink } from "../../types/workflow-common.interface";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { UndoRedoService } from "../undo-redo/undo-redo.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { Workflow, WorkflowContent } from "../../../common/type/workflow";
import { WorkflowMetadata } from "../../../dashboard/type/workflow-metadata.interface";

/**
 * Interface for an action plan highlight event
 */
export interface ActionPlanHighlight {
  operatorIds: string[];
  linkIds: string[];
  summary: string;
}

/**
 * Preview state for action plans - unified for both planning mode and timeline review
 */
export interface ActionPlanPreviewState {
  actionPlan: ActionPlan;
  isPending: boolean; // true = waiting for accept/reject, false = reviewing historical (apply/cancel)
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
  workflowMetadata: WorkflowMetadata; // Workflow metadata (wid, name, etc.)
  beforeWorkflowContent: WorkflowContent; // Workflow content before the action plan was applied
  afterWorkflowContent: WorkflowContent; // Workflow content after the action plan was applied
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

  // Unified preview state - replaces the separate pending and preview states
  private previewStateSubject = new BehaviorSubject<ActionPlanPreviewState | null>(null);

  // Diff preview state
  private currentDiff: DifferentOpIDsList | null = null;

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
   * Get all action plans as observable
   */
  public getActionPlansStream(): Observable<ActionPlan[]> {
    return this.actionPlansSubject.asObservable();
  }

  /**
   * Get the unified preview state stream.
   * Emits when an action plan is being previewed (either pending or historical).
   */
  public getPreviewStateStream(): Observable<ActionPlanPreviewState | null> {
    return this.previewStateSubject.asObservable();
  }

  /**
   * Get the current preview state (synchronous access)
   */
  public getPreviewState(): ActionPlanPreviewState | null {
    return this.previewStateSubject.getValue();
  }

  /**
   * Check if currently in preview mode
   */
  public isPreviewActive(): boolean {
    return this.previewStateSubject.getValue() !== null;
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
    workflowMetadata: WorkflowMetadata,
    beforeWorkflowContent: WorkflowContent,
    afterWorkflowContent: WorkflowContent,
    executorAgentId?: string // Optional: defaults to agentId if not specified
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
      workflowMetadata,
      beforeWorkflowContent,
      afterWorkflowContent,
    };

    this.actionPlans.set(id, actionPlan);
    this.emitActionPlans();

    console.log(`[ActionPlanService] Action plan created: ${id}`);

    return actionPlan;
  }

  /**
   * Start previewing an action plan as a pending plan (accept/reject mode).
   * This is called in planning mode when a new action plan is created.
   */
  public startPendingPreview(actionPlanId: string): void {
    const actionPlan = this.actionPlans.get(actionPlanId);
    if (!actionPlan) {
      console.error(`Action plan ${actionPlanId} not found`);
      return;
    }

    this.previewActionPlanDiffInternal(actionPlan);
    this.previewStateSubject.next({ actionPlan, isPending: true });
    console.log(`[ActionPlanService] Started pending preview for action plan: ${actionPlanId}`);
  }

  /**
   * Start previewing an action plan as historical (apply/cancel mode).
   * This is called when clicking on a timeline node.
   */
  public startHistoricalPreview(actionPlanId: string): void {
    const actionPlan = this.actionPlans.get(actionPlanId);
    if (!actionPlan) {
      console.error(`Action plan ${actionPlanId} not found`);
      return;
    }

    this.previewActionPlanDiffInternal(actionPlan);
    this.previewStateSubject.next({ actionPlan, isPending: false });
    console.log(`[ActionPlanService] Started historical preview for action plan: ${actionPlanId}`);
  }

  /**
   * Clear the current preview and optionally apply changes.
   * @param accept If true, apply the after version; if false, restore the before version.
   */
  public endPreview(accept: boolean): void {
    const previewState = this.previewStateSubject.getValue();
    if (!previewState) {
      console.warn("[ActionPlanService] No active preview to end");
      return;
    }

    this.setWorkflowToActionPlanInternal(previewState.actionPlan, !accept);
    this.previewStateSubject.next(null);
    console.log(`[ActionPlanService] Ended preview, accept=${accept}`);
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

  // ===== DIFF PREVIEW INTERNAL METHODS =====

  /**
   * Internal method to display action plan diff on canvas.
   * Displays the AFTER content with highlights showing what changed from BEFORE.
   */
  private previewActionPlanDiffInternal(actionPlan: ActionPlan): void {
    // Calculate diff between BEFORE and AFTER
    const diff = this.workflowVersionService.getWorkflowsDifference(
      actionPlan.beforeWorkflowContent,
      actionPlan.afterWorkflowContent
    );

    // Create AFTER workflow (spread metadata first to prevent overwriting)
    const afterWorkflow: Workflow = { ...actionPlan.workflowMetadata, content: actionPlan.afterWorkflowContent };

    // Save modification state
    this.workflowVersionService.saveModificationState();

    // Disable persist and undo/redo before reloading
    this.workflowPersistService.setWorkflowPersistFlag(false);
    this.undoRedoService.disableWorkFlowModification();

    // Display the AFTER content on canvas as readonly
    this.workflowActionService.reloadWorkflow(afterWorkflow);
    this.workflowActionService.disableWorkflowModification();

    // Render highlights with beforeWorkflowContent for deleted operator brackets
    this.workflowVersionService.highlightOpVersionDiffSimple(diff, actionPlan.beforeWorkflowContent);

    // Store the current diff
    this.currentDiff = diff;
  }

  /**
   * Internal method to set workflow to either before or after state.
   * Clears highlights, reloads the specified version, and enables modifications.
   */
  private setWorkflowToActionPlanInternal(actionPlan: ActionPlan, isBefore: boolean): void {
    // Clear highlights
    if (this.currentDiff) {
      this.workflowVersionService.unhighlightOpVersionDiff(this.currentDiff);
      this.currentDiff = null;
    }

    // Clear undo/redo stacks
    this.undoRedoService.clearRedoStack();
    this.undoRedoService.clearUndoStack();

    // Enable modifications to allow reloading
    this.workflowActionService.enableWorkflowModification();

    // Disable undo/redo to not capture the reload as an action
    this.undoRedoService.disableWorkFlowModification();

    // Reload the selected version (spread metadata first to prevent overwriting)
    const workflowContent = isBefore ? actionPlan.beforeWorkflowContent : actionPlan.afterWorkflowContent;
    const workflow: Workflow = { ...actionPlan.workflowMetadata, content: workflowContent };
    this.workflowActionService.reloadWorkflow(workflow);

    // Re-enable undo/redo
    this.undoRedoService.enableWorkFlowModification();

    // Re-enable persist to DB
    this.workflowPersistService.setWorkflowPersistFlag(true);

    // Restore modification state
    this.workflowVersionService.restoreModificationState();

    console.log(`Action plan ${isBefore ? "rejected - before" : "accepted - after"} version reloaded`);
  }
}
