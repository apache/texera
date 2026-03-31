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

/**
 * Agent Action Tree — Git-style version history for workflow modifications.
 *
 * Each AgentAction is a node in a tree (no merges, so it's always a tree).
 * Every node has a `parentId` pointing to its predecessor and a `toolCallId`
 * linking it to the ReActStep tool call that created it.
 *
 * A HEAD pointer tracks the current position. New actions are appended as
 * children of HEAD. Checkout moves HEAD to any existing node and returns
 * that node's afterWorkflowContent for restoring WorkflowState.
 */

import { Subject, Observable } from "rxjs";
import type { AgentAction, AgentActionOperations, AgentActionType } from "../types/agent";
import type { WorkflowContent } from "../types/workflow";

// ============================================================================
// Agent Action Tree
// ============================================================================

/** Sentinel ID for the dummy initial action (before any real actions). */
export const INITIAL_ACTION_ID = "agent-action-initial";

export class AgentActionManager {
  /** All action nodes keyed by ID */
  private actions = new Map<string, AgentAction>();

  /** Current position in the tree (like Git HEAD) */
  private head: string = INITIAL_ACTION_ID;

  /** Counter for generating unique IDs */
  private actionCounter = 0;

  /** Maps actionId → messageId (set by texera-agent when creating actions) */
  private actionIdToMessageId = new Map<string, string>();

  /** Stream of newly created actions (for WebSocket broadcasting) */
  private readonly actionSubject = new Subject<AgentAction>();

  /** Stream of HEAD changes (for WebSocket broadcasting) */
  private readonly headChangeSubject = new Subject<{
    headId: string;
    workflowContent?: WorkflowContent;
  }>();

  // ---------- Observables ----------

  getAgentActionStream(): Observable<AgentAction> {
    return this.actionSubject.asObservable();
  }

  getHeadChangeStream(): Observable<{ headId: string; workflowContent?: WorkflowContent }> {
    return this.headChangeSubject.asObservable();
  }

  // ---------- HEAD ----------

  getHead(): string {
    return this.head;
  }

  // ---------- Action ↔ Message mapping ----------

  /** Associate an actionId with the messageId that produced it. */
  setActionMessageId(actionId: string, messageId: string): void {
    this.actionIdToMessageId.set(actionId, messageId);
  }

  /** Get the messageId that produced a given actionId. */
  getMessageIdForAction(actionId: string): string | undefined {
    return this.actionIdToMessageId.get(actionId);
  }

  /**
   * Get the ordered list of messageIds on the ancestor path from root to HEAD (or given node).
   * Preserves chronological order, deduplicates (a message may produce multiple actions).
   */
  getMessageIdsOnPath(actionId?: string): string[] {
    const messageIds: string[] = [];
    const seen = new Set<string>();
    for (const id of this.getAncestorPath(actionId)) {
      const msgId = this.actionIdToMessageId.get(id);
      if (msgId && !seen.has(msgId)) {
        seen.add(msgId);
        messageIds.push(msgId);
      }
    }
    return messageIds;
  }

  /**
   * For each messageId on the ancestor path, return the LAST actionId that belongs to it.
   * This determines the cutoff point: only steps up to this action's step should be visible.
   *
   * Returns a Map<messageId, lastActionId> in chronological messageId order.
   */
  getLastActionPerMessageOnPath(actionId?: string): Map<string, string> {
    const result = new Map<string, string>();
    for (const id of this.getAncestorPath(actionId)) {
      const msgId = this.actionIdToMessageId.get(id);
      if (msgId) {
        result.set(msgId, id); // overwrites — last one wins
      }
    }
    return result;
  }

  // ---------- Create ----------

  createAgentAction(
    agentId: string,
    agentName: string,
    summary: string,
    operations: AgentActionOperations,
    workflowMetadata: { wid?: number; name?: string },
    beforeWorkflowContent: WorkflowContent,
    afterWorkflowContent: WorkflowContent,
    executorAgentId?: string,
    toolCallId?: string,
    actionType?: AgentActionType,
    messageSource?: "chat" | "feedback"
  ): AgentAction {
    const id = this.generateId();

    const agentAction: AgentAction = {
      id,
      agentId,
      agentName,
      executorAgentId: executorAgentId || agentId,
      summary,
      operations,
      createdAt: new Date(),
      toolCallId,
      parentId: this.head,
      actionType: actionType || "tool_call",
      messageSource,
      workflowMetadata,
      beforeWorkflowContent,
      afterWorkflowContent,
    };

    this.actions.set(id, agentAction);
    this.head = id;

    this.actionSubject.next(agentAction);
    console.log(`[AgentActionTree] Created action: ${id} (parent: ${agentAction.parentId || "root"}) - ${summary}`);

    return agentAction;
  }

  // ---------- Checkout ----------

  /**
   * Move HEAD to the given action and return its afterWorkflowContent.
   * Returns null if the action doesn't exist.
   */
  checkout(actionId: string): { workflowContent?: WorkflowContent } | null {
    // Allow checkout to the dummy initial action (no workflow content)
    if (actionId === INITIAL_ACTION_ID) {
      this.head = INITIAL_ACTION_ID;
      this.headChangeSubject.next({ headId: INITIAL_ACTION_ID });
      console.log(`[AgentActionTree] Checkout to initial state`);
      return {};
    }

    const action = this.actions.get(actionId);
    if (!action?.afterWorkflowContent) return null;

    this.head = actionId;
    this.headChangeSubject.next({ headId: actionId, workflowContent: action.afterWorkflowContent });
    console.log(`[AgentActionTree] Checkout to: ${actionId}`);

    return { workflowContent: action.afterWorkflowContent };
  }

  // ---------- Tree traversal ----------

  /**
   * Walk from the given node (default: HEAD) to the root.
   * Returns action IDs ordered root → … → node.
   */
  getAncestorPath(actionId?: string): string[] {
    const target = actionId ?? this.head;
    if (!target) return [];
    if (target === INITIAL_ACTION_ID) return [INITIAL_ACTION_ID];

    // Walk from target to root, collecting real action IDs
    const chain: string[] = [];
    let current: string | undefined = target;
    while (current && current !== INITIAL_ACTION_ID) {
      chain.unshift(current);
      current = this.actions.get(current)?.parentId;
    }
    // Prepend the initial action so results stored there are visible
    return [INITIAL_ACTION_ID, ...chain];
  }

  /**
   * Collect all toolCallIds on the ancestor path from root to HEAD (or given node).
   */
  getToolCallIdsOnPath(actionId?: string): Set<string> {
    const ids = new Set<string>();
    for (const id of this.getAncestorPath(actionId)) {
      const tc = this.actions.get(id)?.toolCallId;
      if (tc) ids.add(tc);
    }
    return ids;
  }

  // ---------- Basic accessors ----------

  getAgentAction(id: string): AgentAction | undefined {
    return this.actions.get(id);
  }

  getAllAgentActions(): AgentAction[] {
    return Array.from(this.actions.values());
  }

  deleteAgentAction(id: string): boolean {
    return this.actions.delete(id);
  }

  clearAllAgentActions(): void {
    this.actions.clear();
    this.actionIdToMessageId.clear();
    this.head = INITIAL_ACTION_ID;
  }

  // ---------- Internals ----------

  private generateId(): string {
    return `agent-action-${++this.actionCounter}-${Date.now()}`;
  }

  destroy(): void {
    this.actionSubject.complete();
    this.headChangeSubject.complete();
    this.actions.clear();
  }
}
