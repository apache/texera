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
  workflowSnapshot?: WorkflowSnapshot; // Snapshot of workflow state before changes (for planning mode)
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

  private workflowActionService?: WorkflowActionService;

  constructor() {}

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
    workflowSnapshot?: WorkflowSnapshot // Optional: snapshot of workflow state before changes (for planning mode)
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
      workflowSnapshot,
    };

    this.actionPlans.set(id, actionPlan);
    this.emitActionPlans();
    this.pendingActionPlanSubject.next(actionPlan);

    // Emit highlight event for the workflow editor
    this.actionPlanHighlightSubject.next({ operatorIds, linkIds, summary });

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

  /**
   * Revert an action plan by restoring the workflow state from snapshot
   * This method undoes the changes made by an action plan:
   * - Deletes operators that were added
   * - Restores properties of modified operators
   * - Re-adds operators and links that were deleted
   */
  public revertActionPlan(actionPlanId: string): boolean {
    const actionPlan = this.actionPlans.get(actionPlanId);
    if (!actionPlan || !this.workflowActionService) {
      console.error(`Cannot revert action plan ${actionPlanId}: plan or workflow service not found`);
      return false;
    }

    const { operations, workflowSnapshot } = actionPlan;
    const texeraGraph = this.workflowActionService.getTexeraGraph();

    try {
      // STEP 1: Delete operators that were added
      if (operations.add.operatorIds.length > 0) {
        for (const operatorId of operations.add.operatorIds) {
          try {
            if (texeraGraph.hasOperator(operatorId)) {
              this.workflowActionService.deleteOperator(operatorId);
            }
          } catch (error) {
            console.error(`Error deleting added operator ${operatorId}:`, error);
          }
        }
      }

      // STEP 2: Delete links that were added
      if (operations.add.linkIds.length > 0) {
        for (const linkId of operations.add.linkIds) {
          try {
            if (texeraGraph.hasLinkWithID(linkId)) {
              this.workflowActionService.deleteLinkWithID(linkId);
            }
          } catch (error) {
            console.error(`Error deleting added link ${linkId}:`, error);
          }
        }
      }

      // STEP 3: Restore properties of modified operators from snapshot
      if (workflowSnapshot && operations.modify.operatorIds.length > 0) {
        for (const operatorId of operations.modify.operatorIds) {
          const originalProperties = workflowSnapshot.operatorProperties.get(operatorId);
          if (originalProperties && texeraGraph.hasOperator(operatorId)) {
            try {
              this.workflowActionService.setOperatorProperty(operatorId, originalProperties);
            } catch (error) {
              console.error(`Error restoring operator ${operatorId} properties:`, error);
            }
          }
        }
      }

      // STEP 4: Re-add operators that were deleted
      if (workflowSnapshot && operations.delete.operatorIds.length > 0) {
        for (const operator of workflowSnapshot.operators) {
          try {
            // Use a default position for restored operators
            const position = { x: 100, y: 100 };
            this.workflowActionService.addOperator(operator, position);
          } catch (error) {
            console.error(`Error re-adding deleted operator ${operator.operatorID}:`, error);
          }
        }
      }

      // STEP 5: Re-add links that were deleted
      if (workflowSnapshot && operations.delete.linkIds.length > 0) {
        for (const link of workflowSnapshot.links) {
          try {
            this.workflowActionService.addLink(link);
          } catch (error) {
            console.error(`Error re-adding deleted link ${link.linkID}:`, error);
          }
        }
      }

      console.log(`Successfully reverted action plan ${actionPlanId}`);
      return true;
    } catch (error) {
      console.error(`Error reverting action plan ${actionPlanId}:`, error);
      return false;
    }
  }
}
