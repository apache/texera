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
  private pendingActionPlanSubject = new BehaviorSubject<ActionPlan | null>(null);

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

    // Emit to pending stream immediately since we have all the data
    console.log(`[ActionPlanService] Action plan created with workflow contents, emitting to pending stream: ${id}`);
    this.pendingActionPlanSubject.next(actionPlan);

    return actionPlan;
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
   * Preview action plan diff by action plan ID.
   * Displays the AFTER content on canvas (user sees proposed final result)
   * with highlights showing what changed from BEFORE.
   *
   * @param actionPlanId Action plan ID
   */
  public previewActionPlanDiff(actionPlanId: string): void {
    const actionPlan = this.actionPlans.get(actionPlanId);
    if (!actionPlan) {
      throw new Error(`Action plan ${actionPlanId} not found`);
    }

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
   * Set workflow to either before or after state of an action plan.
   * Clears highlights, reloads the specified version, and enables modifications.
   *
   * @param actionPlanId Action plan ID
   * @param isBefore If true, loads before version; if false, loads after version
   */
  public setWorkflowToActionPlan(actionPlanId: string, isBefore: boolean): void {
    const actionPlan = this.actionPlans.get(actionPlanId);
    if (!actionPlan) {
      console.error(`Action plan ${actionPlanId} not found`);
      return;
    }

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

    console.log(`Action plan ${isBefore ? 'rejected - before' : 'accepted - after'} version reloaded`);
  }
}
