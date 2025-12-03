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
 * Interface for an agent action highlight event
 */
export interface AgentActionHighlight {
  operatorIds: string[];
  linkIds: string[];
  summary: string;
}

/**
 * Preview state for agent actions - unified for both planning mode and timeline review
 */
export interface AgentActionPreviewState {
  agentAction: AgentAction;
  isPending: boolean; // true = waiting for accept/reject, false = reviewing historical (apply/cancel)
}

/**
 * Diff structure for operators (reusing workflow-version.service structure)
 */
type DifferentOpIDsList = {
  [key in "modified" | "added" | "deleted"]: string[];
};

/**
 * Operations performed in an agent action
 */
export interface AgentActionOperations {
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
 * Complete Agent Action data structure
 */
export interface AgentAction {
  id: string; // Unique identifier for the agent action
  agentId: string; // ID of the agent that created this action
  agentName: string; // Name of the agent
  executorAgentId: string; // ID of the agent that will execute/handle feedback for this action (can be different from creator)
  summary: string; // Overall summary of the agent action
  operations: AgentActionOperations; // Operations performed (add/modify/delete)
  createdAt: Date; // Creation timestamp
  operatorIds: string[]; // For highlighting
  linkIds: string[]; // For highlighting
  workflowMetadata: WorkflowMetadata; // Workflow metadata (wid, name, etc.)
  beforeWorkflowContent: WorkflowContent; // Workflow content before the agent action was applied
  afterWorkflowContent: WorkflowContent; // Workflow content after the agent action was applied
}

/**
 * Service to manage agent actions, highlights, and user feedback
 * Handles the interactive flow: show action -> wait for user decision -> execute -> track progress
 */
@Injectable({
  providedIn: "root",
})
export class AgentActionService {
  private agentActionHighlightSubject = new Subject<AgentActionHighlight>();
  private cleanupSubject = new Subject<void>();

  // Agent action storage
  private agentActions = new Map<string, AgentAction>();
  private agentActionsSubject = new BehaviorSubject<AgentAction[]>([]);

  // Unified preview state - replaces the separate pending and preview states
  private previewStateSubject = new BehaviorSubject<AgentActionPreviewState | null>(null);

  // Diff preview state
  private currentDiff: DifferentOpIDsList | null = null;

  // Saved workflow content before starting a historical preview (for cancel/restore)
  private savedWorkflowContentBeforePreview: WorkflowContent | null = null;

  constructor(
    private workflowVersionService: WorkflowVersionService,
    private undoRedoService: UndoRedoService,
    private workflowPersistService: WorkflowPersistService,
    private workflowActionService: WorkflowActionService
  ) {}

  /**
   * Get agent action highlight stream
   */
  public getAgentActionHighlightStream() {
    return this.agentActionHighlightSubject.asObservable();
  }

  /**
   * Get cleanup stream - emits when user provides feedback (accept/reject)
   */
  public getCleanupStream() {
    return this.cleanupSubject.asObservable();
  }

  /**
   * Get all agent actions as observable
   */
  public getAgentActionsStream(): Observable<AgentAction[]> {
    return this.agentActionsSubject.asObservable();
  }

  /**
   * Get the unified preview state stream.
   * Emits when an agent action is being previewed (either pending or historical).
   */
  public getPreviewStateStream(): Observable<AgentActionPreviewState | null> {
    return this.previewStateSubject.asObservable();
  }

  /**
   * Get the current preview state (synchronous access)
   */
  public getPreviewState(): AgentActionPreviewState | null {
    return this.previewStateSubject.getValue();
  }

  /**
   * Check if currently in preview mode
   */
  public isPreviewActive(): boolean {
    return this.previewStateSubject.getValue() !== null;
  }

  /**
   * Get all agent actions
   */
  public getAllAgentActions(): AgentAction[] {
    return Array.from(this.agentActions.values());
  }

  /**
   * Get a specific agent action by ID
   */
  public getAgentAction(id: string): AgentAction | undefined {
    return this.agentActions.get(id);
  }

  /**
   * Create a new agent action
   */
  public createAgentAction(
    agentId: string,
    agentName: string,
    summary: string,
    operations: AgentActionOperations,
    operatorIds: string[],
    linkIds: string[],
    workflowMetadata: WorkflowMetadata,
    beforeWorkflowContent: WorkflowContent,
    afterWorkflowContent: WorkflowContent,
    executorAgentId?: string // Optional: defaults to agentId if not specified
  ): AgentAction {
    const id = this.generateId();

    const agentAction: AgentAction = {
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

    this.agentActions.set(id, agentAction);
    this.emitAgentActions();

    console.log(`[AgentActionService] Agent action created: ${id}`);

    return agentAction;
  }

  /**
   * Start previewing an agent action as a pending action (accept/reject mode).
   * This is called in planning mode when a new agent action is created.
   */
  public startPendingPreview(agentActionId: string): void {
    this.startPreview(agentActionId, true);
  }

  /**
   * Start previewing an agent action as historical (apply/cancel mode).
   * This is called when clicking on a timeline node.
   */
  public startHistoricalPreview(agentActionId: string): void {
    this.startPreview(agentActionId, false);
  }

  /**
   * Unified method to start a preview.
   * Always saves the current workflow content before showing the preview.
   */
  private startPreview(agentActionId: string, isPending: boolean): void {
    const agentAction = this.agentActions.get(agentActionId);
    if (!agentAction) {
      console.error(`Agent action ${agentActionId} not found`);
      return;
    }

    // Always save the current workflow content BEFORE showing the preview
    // This is what we'll restore to if the user cancels
    this.savedWorkflowContentBeforePreview = this.workflowActionService.getWorkflowContent();

    this.showPreviewDiff(agentAction);
    this.previewStateSubject.next({ agentAction, isPending });
    console.log(
      `[AgentActionService] Started ${isPending ? "pending" : "historical"} preview for agent action: ${agentActionId}`
    );
  }

  /**
   * End the current preview.
   * @param accept If true, apply the agent action's afterWorkflowContent; if false, restore to saved content.
   */
  public endPreview(accept: boolean): void {
    const previewState = this.previewStateSubject.getValue();
    if (!previewState) {
      console.warn("[AgentActionService] No active preview to end");
      return;
    }

    // Determine which content to load
    const contentToLoad = accept
      ? previewState.agentAction.afterWorkflowContent
      : this.savedWorkflowContentBeforePreview!;

    this.loadWorkflowContent(previewState.agentAction.workflowMetadata, contentToLoad);

    // Clear state
    this.savedWorkflowContentBeforePreview = null;
    this.previewStateSubject.next(null);
    console.log(`[AgentActionService] Ended preview, accept=${accept}`);
  }

  /**
   * Delete an agent action
   */
  public deleteAgentAction(id: string): boolean {
    if (this.agentActions.has(id)) {
      this.agentActions.delete(id);
      this.emitAgentActions();
      return true;
    }
    return false;
  }

  /**
   * Clear all agent actions
   */
  public clearAllAgentActions(): void {
    this.agentActions.clear();
    this.emitAgentActions();
  }

  /**
   * Generate a unique ID for agent actions
   */
  private generateId(): string {
    return `agent-action-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
  }

  /**
   * Emit the current list of agent actions
   */
  private emitAgentActions(): void {
    this.agentActionsSubject.next(this.getAllAgentActions());
  }

  // ===== PREVIEW INTERNAL METHODS =====

  /**
   * Display agent action diff on canvas.
   * Shows the AFTER content with highlights indicating what changed from BEFORE.
   */
  private showPreviewDiff(agentAction: AgentAction): void {
    // Calculate diff between BEFORE and AFTER
    const diff = this.workflowVersionService.getWorkflowsDifference(
      agentAction.beforeWorkflowContent,
      agentAction.afterWorkflowContent
    );

    // Create AFTER workflow
    const afterWorkflow: Workflow = { ...agentAction.workflowMetadata, content: agentAction.afterWorkflowContent };

    // Save modification state
    this.workflowVersionService.saveModificationState();

    // Disable persist and undo/redo before reloading
    this.workflowPersistService.setWorkflowPersistFlag(false);
    this.undoRedoService.disableWorkFlowModification();

    // Display the AFTER content on canvas as readonly, preserving the current viewport
    this.workflowActionService.reloadWorkflow(afterWorkflow, undefined, true);
    this.workflowActionService.disableWorkflowModification();

    // Render highlights with beforeWorkflowContent for deleted operator brackets
    this.workflowVersionService.highlightOpVersionDiffSimple(diff, agentAction.beforeWorkflowContent);

    // Store the current diff
    this.currentDiff = diff;
  }

  /**
   * Load workflow content and restore normal editing state.
   * Clears highlights, loads the specified content, and re-enables modifications.
   */
  private loadWorkflowContent(metadata: WorkflowMetadata, content: WorkflowContent): void {
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

    // Reload the workflow content, preserving the current viewport
    const workflow: Workflow = { ...metadata, content };
    this.workflowActionService.reloadWorkflow(workflow, undefined, true);

    // Re-enable undo/redo
    this.undoRedoService.enableWorkFlowModification();

    // Re-enable persist to DB
    this.workflowPersistService.setWorkflowPersistFlag(true);

    // Restore modification state
    this.workflowVersionService.restoreModificationState();
  }
}
