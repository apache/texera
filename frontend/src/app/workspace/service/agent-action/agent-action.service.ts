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
import { BehaviorSubject, Observable } from "rxjs";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { WorkflowContent } from "../../../common/type/workflow";
import { WorkflowMetadata } from "../../../dashboard/type/workflow-metadata.interface";

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
  execute: {
    operatorIds: string[];
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
  toolCallId?: string; // The tool call ID that produced this action
  parentId?: string; // Parent action ID in the action tree
  actionType?: string; // "tool_call" | "user_request" | "agent_response"
  messageSource?: string; // "chat" | "feedback"
  operatorIds: string[]; // For highlighting
  linkIds: string[]; // For highlighting
  workflowMetadata: WorkflowMetadata; // Workflow metadata (wid, name, etc.)
  beforeWorkflowContent: WorkflowContent; // Workflow content before the agent action was applied
  afterWorkflowContent: WorkflowContent; // Workflow content after the agent action was applied
}

/**
 * Service to manage agent action diff rendering on the canvas.
 * Simplified: no preview mode, just toggle diff highlighting for the current action.
 * Switching between actions changes the head & workflow via WS; this service only handles diff display.
 */
@Injectable({
  providedIn: "root",
})
export class AgentActionService {
  // Whether diff highlighting is currently shown
  private diffVisibleSubject = new BehaviorSubject<boolean>(false);

  // The action currently being diffed
  private currentDiffAction: AgentAction | null = null;

  // Current diff state for cleanup
  private currentDiff: DifferentOpIDsList | null = null;

  constructor(private workflowVersionService: WorkflowVersionService) {}

  /**
   * Get observable for diff visibility state.
   */
  public getDiffVisibleStream(): Observable<boolean> {
    return this.diffVisibleSubject.asObservable();
  }

  /**
   * Check if diff is currently visible.
   */
  public isDiffVisible(): boolean {
    return this.diffVisibleSubject.getValue();
  }

  /**
   * Show diff highlighting for an agent action on the canvas.
   */
  public showDiff(agentAction: AgentAction): void {
    // Clear any existing diff first
    this.clearDiff();

    // Calculate diff between BEFORE and AFTER
    const diff = this.workflowVersionService.getWorkflowsDifference(
      agentAction.beforeWorkflowContent,
      agentAction.afterWorkflowContent
    );

    // Render highlights with beforeWorkflowContent for deleted operator brackets
    this.workflowVersionService.highlightOpVersionDiffSimple(diff, agentAction.beforeWorkflowContent);

    // Store state
    this.currentDiff = diff;
    this.currentDiffAction = agentAction;
    this.diffVisibleSubject.next(true);
  }

  /**
   * Clear diff highlighting from the canvas.
   */
  public clearDiff(): void {
    if (this.currentDiff) {
      this.workflowVersionService.unhighlightOpVersionDiff(this.currentDiff);
      this.currentDiff = null;
    }
    this.currentDiffAction = null;
    this.diffVisibleSubject.next(false);
  }

  /**
   * Toggle diff highlighting for an agent action.
   * If diff is already showing for this action, clear it.
   * If diff is showing for a different action or not showing, show it for the given action.
   */
  public toggleDiff(agentAction: AgentAction): void {
    if (this.diffVisibleSubject.getValue() && this.currentDiffAction?.id === agentAction.id) {
      this.clearDiff();
    } else {
      this.showDiff(agentAction);
    }
  }

  /**
   * Get the action currently being diffed (if any).
   */
  public getCurrentDiffAction(): AgentAction | null {
    return this.currentDiffAction;
  }
}
