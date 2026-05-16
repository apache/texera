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

export type ConversationRole = "user" | "agent";

export interface ConversationMessage {
  role: ConversationRole;
  content: string;
  timestamp: number;
}

/**
 * A conversation is scoped to a (workflowId, agentId) pair. Each agent has its
 * own independent history within a workflow; switching agent or workflow
 * yields a different list. The agent identity is also denormalized onto the
 * entry so historic rows survive custom-agent deletion.
 */
export interface Conversation {
  id: string;
  workflowId: number;
  /** "default" or custom-agent id used to create this conversation. */
  agentId: string;
  agentName: string;
  agentIcon: string;
  title: string;
  messages: ConversationMessage[];
  /** Backend agent runtime id last used for this conversation. */
  lastBackendAgentId?: string;
  /** True if any message contained workflow-generation tool calls. */
  workflowGenerated: boolean;
  createdAt: number;
  updatedAt: number;
}

const KEY_PREFIX = "texera.workflowConversations.v1.";

function storageKey(workflowId: number, agentId: string): string {
  return `${KEY_PREFIX}${workflowId}.${agentId}`;
}

function cacheKey(workflowId: number, agentId: string): string {
  return `${workflowId}|${agentId}`;
}

export interface NewConversationInput {
  workflowId: number;
  agentId: string;
  agentName: string;
  agentIcon: string;
}

@Injectable({ providedIn: "root" })
export class ConversationService {
  private cache = new Map<string, BehaviorSubject<Conversation[]>>();

  public list$(workflowId: number, agentId: string): Observable<Conversation[]> {
    return this.subject(workflowId, agentId).asObservable();
  }

  public list(workflowId: number, agentId: string): Conversation[] {
    return this.subject(workflowId, agentId).value;
  }

  public get(workflowId: number, agentId: string, conversationId: string): Conversation | undefined {
    return this.subject(workflowId, agentId).value.find(c => c.id === conversationId);
  }

  public create(input: NewConversationInput): Conversation {
    const now = Date.now();
    const conversation: Conversation = {
      id: `conv-${now.toString(36)}-${Math.random().toString(36).slice(2, 8)}`,
      workflowId: input.workflowId,
      agentId: input.agentId,
      agentName: input.agentName,
      agentIcon: input.agentIcon,
      title: "New conversation",
      messages: [],
      workflowGenerated: false,
      createdAt: now,
      updatedAt: now,
    };
    const next = [conversation, ...this.subject(input.workflowId, input.agentId).value];
    this.persist(input.workflowId, input.agentId, next);
    return conversation;
  }

  public appendMessage(
    workflowId: number,
    agentId: string,
    conversationId: string,
    role: ConversationRole,
    content: string,
    workflowGenerated: boolean = false
  ): Conversation | undefined {
    const all = this.subject(workflowId, agentId).value;
    const idx = all.findIndex(c => c.id === conversationId);
    if (idx === -1) return undefined;
    const target = all[idx];
    const isFirstUserMessage = role === "user" && target.messages.length === 0;
    const updated: Conversation = {
      ...target,
      title: isFirstUserMessage ? this.deriveTitle(content) : target.title,
      messages: [...target.messages, { role, content, timestamp: Date.now() }],
      workflowGenerated: target.workflowGenerated || workflowGenerated,
      updatedAt: Date.now(),
    };
    const next = [...all];
    next[idx] = updated;
    next.sort((a, b) => b.updatedAt - a.updatedAt);
    this.persist(workflowId, agentId, next);
    return updated;
  }

  public setBackendAgentId(
    workflowId: number,
    agentId: string,
    conversationId: string,
    backendAgentId: string
  ): void {
    const all = this.subject(workflowId, agentId).value;
    const idx = all.findIndex(c => c.id === conversationId);
    if (idx === -1) return;
    const next = [...all];
    next[idx] = { ...all[idx], lastBackendAgentId: backendAgentId, updatedAt: Date.now() };
    this.persist(workflowId, agentId, next);
  }

  public delete(workflowId: number, agentId: string, conversationId: string): void {
    const next = this.subject(workflowId, agentId).value.filter(c => c.id !== conversationId);
    this.persist(workflowId, agentId, next);
  }

  /**
   * Look up a conversation across every agent bucket for the given workflow,
   * matching on its last bound backend agent runtime id. Used when an external
   * entry point (e.g. dashboard activation link) hands us a backend agent id
   * and we don't know which agent bucket the conversation lives in.
   */
  public findByBackendAgentId(workflowId: number, backendAgentId: string): Conversation | undefined {
    const prefix = `${KEY_PREFIX}${workflowId}.`;
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (!key || !key.startsWith(prefix)) continue;
      const agentId = key.slice(prefix.length);
      const found = this.subject(workflowId, agentId).value.find(c => c.lastBackendAgentId === backendAgentId);
      if (found) return found;
    }
    return undefined;
  }

  private deriveTitle(firstMessage: string): string {
    const trimmed = firstMessage.trim().replace(/\s+/g, " ");
    if (trimmed.length <= 60) return trimmed || "New conversation";
    return trimmed.slice(0, 57) + "...";
  }

  private subject(workflowId: number, agentId: string): BehaviorSubject<Conversation[]> {
    const ck = cacheKey(workflowId, agentId);
    if (!this.cache.has(ck)) {
      this.cache.set(ck, new BehaviorSubject<Conversation[]>(this.read(workflowId, agentId)));
    }
    return this.cache.get(ck)!;
  }

  private read(workflowId: number, agentId: string): Conversation[] {
    try {
      const raw = localStorage.getItem(storageKey(workflowId, agentId));
      if (!raw) return [];
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return [];
      return (parsed as Conversation[]).sort((a, b) => b.updatedAt - a.updatedAt);
    } catch {
      return [];
    }
  }

  private persist(workflowId: number, agentId: string, conversations: Conversation[]): void {
    localStorage.setItem(storageKey(workflowId, agentId), JSON.stringify(conversations));
    this.subject(workflowId, agentId).next(conversations);
  }
}
