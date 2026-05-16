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
import { BehaviorSubject, Observable, combineLatest, interval, merge, of } from "rxjs";
import { distinctUntilChanged, map, switchMap } from "rxjs/operators";
import * as Y from "yjs";
import { v4 as uuid } from "uuid";
import { UserService } from "../../../common/service/user/user.service";
import { CoeditorState } from "../../../common/type/user";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import {
  ChatMessage,
  CollaborationTab,
  OnlineUserSnapshot,
  OperatorComment,
  OperatorCommentThread,
} from "./collaboration.types";

const CHAT_Y_KEY = "texera-collab-chat";
const COMMENTS_Y_KEY = "texera-collab-op-comments";
const IDLE_THRESHOLD_MS = 5 * 60 * 1000;
const PRESENCE_TICK_MS = 5000;

/**
 * CollaborationService owns the data layer for the Collaboration Suite:
 *   - team chat (Y.Array<ChatMessage>)
 *   - operator comment threads (Y.Array<OperatorComment>)
 *   - presence snapshot derived from the existing Yjs awareness protocol
 *
 * All three features piggyback on the SharedModel that Texera already maintains
 * for real-time co-editing, so they sync across browser tabs for free.
 *
 * The service also owns UI state (panel open/closed, active tab) so multiple
 * components — menu button, context menu, badges — can drive the same panel.
 */
@Injectable({ providedIn: "root" })
export class CollaborationService {
  private readonly chatSubject = new BehaviorSubject<ChatMessage[]>([]);
  private readonly commentsSubject = new BehaviorSubject<OperatorComment[]>([]);
  private readonly openSubject = new BehaviorSubject<boolean>(false);
  private readonly activeTabSubject = new BehaviorSubject<CollaborationTab>("chat");
  private readonly awarenessTickSubject = new BehaviorSubject<number>(Date.now());
  private readonly lastActivityByClient = new Map<string, number>();

  private chatArr?: Y.Array<ChatMessage>;
  private commentsArr?: Y.Array<OperatorComment>;
  private chatObserver?: () => void;
  private commentsObserver?: () => void;
  private awarenessObserver?: () => void;

  constructor(
    private workflowActionService: WorkflowActionService,
    private userService: UserService
  ) {
    this.attach();
    this.workflowActionService.getTexeraGraph().newYDocLoadedSubject.subscribe(() => this.attach());
    // Tick periodically so idle status updates without external events.
    interval(PRESENCE_TICK_MS).subscribe(() => this.awarenessTickSubject.next(Date.now()));
  }

  // ───────────────────── attach / lifecycle ─────────────────────

  private attach(): void {
    const graph = this.workflowActionService.getTexeraGraph();
    const sharedModel = graph?.sharedModel;
    if (!sharedModel) return;

    this.detach();

    this.chatArr = sharedModel.yDoc.getArray<ChatMessage>(CHAT_Y_KEY);
    this.commentsArr = sharedModel.yDoc.getArray<OperatorComment>(COMMENTS_Y_KEY);

    this.chatSubject.next(this.chatArr.toArray());
    this.commentsSubject.next(this.commentsArr.toArray());

    this.chatObserver = () => this.chatSubject.next(this.chatArr!.toArray());
    this.commentsObserver = () => this.commentsSubject.next(this.commentsArr!.toArray());
    this.chatArr.observe(this.chatObserver);
    this.commentsArr.observe(this.commentsObserver);

    this.lastActivityByClient.clear();
    this.bumpAwarenessActivity();
    this.awarenessObserver = () => this.bumpAwarenessActivity();
    sharedModel.awareness.on("change", this.awarenessObserver);
    this.awarenessTickSubject.next(Date.now());
  }

  private detach(): void {
    if (this.chatArr && this.chatObserver) this.chatArr.unobserve(this.chatObserver);
    if (this.commentsArr && this.commentsObserver) this.commentsArr.unobserve(this.commentsObserver);
    const sharedModel = this.workflowActionService.getTexeraGraph()?.sharedModel;
    if (sharedModel && this.awarenessObserver) sharedModel.awareness.off("change", this.awarenessObserver);
    this.chatObserver = undefined;
    this.commentsObserver = undefined;
    this.awarenessObserver = undefined;
  }

  private bumpAwarenessActivity(): void {
    const states = this.getAwarenessStates();
    const now = Date.now();
    for (const [clientId, state] of states) {
      const key = clientId.toString();
      if (state?.isActive) this.lastActivityByClient.set(key, now);
      else if (!this.lastActivityByClient.has(key)) this.lastActivityByClient.set(key, now);
    }
    this.awarenessTickSubject.next(now);
  }

  // ───────────────────── panel state ─────────────────────

  public open$(): Observable<boolean> {
    return this.openSubject.asObservable();
  }

  public isOpen(): boolean {
    return this.openSubject.value;
  }

  public activeTab$(): Observable<CollaborationTab> {
    return this.activeTabSubject.asObservable();
  }

  public openPanel(tab?: CollaborationTab): void {
    if (tab) this.activeTabSubject.next(tab);
    this.openSubject.next(true);
  }

  public closePanel(): void {
    this.openSubject.next(false);
  }

  public togglePanel(tab?: CollaborationTab): void {
    if (this.openSubject.value) this.closePanel();
    else this.openPanel(tab);
  }

  public setActiveTab(tab: CollaborationTab): void {
    this.activeTabSubject.next(tab);
  }

  // ───────────────────── chat ─────────────────────

  public chat$(): Observable<ChatMessage[]> {
    return this.chatSubject.asObservable();
  }

  public sendChat(content: string): void {
    if (!this.chatArr || !content.trim()) return;
    const user = this.userService.getCurrentUser();
    const msg: ChatMessage = {
      id: uuid(),
      userId: user?.uid ?? "anon",
      userName: user?.name ?? "Anonymous",
      color: user?.color ?? "#888",
      content: content.trim(),
      timestamp: Date.now(),
      kind: "user",
    };
    this.chatArr.push([msg]);
  }

  public postSystemMessage(content: string): void {
    if (!this.chatArr || !content.trim()) return;
    const msg: ChatMessage = {
      id: uuid(),
      userId: "system",
      userName: "System",
      color: "#7f8c8d",
      content: content.trim(),
      timestamp: Date.now(),
      kind: "system",
    };
    this.chatArr.push([msg]);
  }

  // ───────────────────── operator comments ─────────────────────

  public comments$(): Observable<OperatorComment[]> {
    return this.commentsSubject.asObservable();
  }

  public threads$(): Observable<OperatorCommentThread[]> {
    return this.commentsSubject.pipe(map(comments => this.buildThreads(comments)));
  }

  /** Threads for a single operator, including resolved ones (UI decides whether to hide). */
  public threadsForOperator$(operatorId: string): Observable<OperatorCommentThread[]> {
    return this.threads$().pipe(map(threads => threads.filter(t => t.operatorId === operatorId)));
  }

  public unresolvedCountsByOperator$(): Observable<Map<string, number>> {
    return this.commentsSubject.pipe(
      map(comments => {
        const counts = new Map<string, number>();
        for (const c of comments) {
          if (c.parentId || c.resolved) continue;
          counts.set(c.operatorId, (counts.get(c.operatorId) ?? 0) + 1);
        }
        return counts;
      })
    );
  }

  public addComment(operatorId: string, content: string, parentId?: string): void {
    if (!this.commentsArr || !content.trim()) return;
    const user = this.userService.getCurrentUser();
    const comment: OperatorComment = {
      id: uuid(),
      operatorId,
      parentId,
      content: content.trim(),
      userId: user?.uid ?? "anon",
      userName: user?.name ?? "Anonymous",
      color: user?.color ?? "#888",
      timestamp: Date.now(),
      resolved: parentId ? undefined : false,
    };
    this.commentsArr.push([comment]);
  }

  public toggleResolveThread(rootCommentId: string): void {
    if (!this.commentsArr) return;
    const sharedModel = this.workflowActionService.getTexeraGraph().sharedModel;
    const list = this.commentsArr.toArray();
    const idx = list.findIndex(c => c.id === rootCommentId);
    if (idx < 0) return;
    sharedModel.transact(() => {
      const updated: OperatorComment = { ...list[idx], resolved: !list[idx].resolved };
      this.commentsArr!.delete(idx, 1);
      this.commentsArr!.insert(idx, [updated]);
    });
  }

  private buildThreads(comments: OperatorComment[]): OperatorCommentThread[] {
    const roots: OperatorComment[] = [];
    const replies: OperatorComment[] = [];
    for (const c of comments) {
      if (c.parentId) replies.push(c);
      else roots.push(c);
    }
    const threads: OperatorCommentThread[] = roots.map(root => ({
      operatorId: root.operatorId,
      root,
      replies: replies
        .filter(r => r.parentId === root.id)
        .sort((a, b) => a.timestamp - b.timestamp),
      resolved: !!root.resolved,
    }));
    return threads.sort((a, b) => a.root.timestamp - b.root.timestamp);
  }

  // ───────────────────── presence ─────────────────────

  /**
   * Online users snapshot derived from the existing Yjs awareness state.
   * Includes the local user as `isLocal: true`.
   */
  public onlineUsers$(): Observable<OnlineUserSnapshot[]> {
    return merge(
      this.awarenessTickSubject,
      this.workflowActionService.getTexeraGraph().newYDocLoadedSubject
    ).pipe(
      map(() => this.buildOnlineUsers()),
      distinctUntilChanged((a, b) => this.serializeSnapshots(a) === this.serializeSnapshots(b))
    );
  }

  private buildOnlineUsers(): OnlineUserSnapshot[] {
    const sharedModel = this.workflowActionService.getTexeraGraph()?.sharedModel;
    if (!sharedModel) return [];
    const states = this.getAwarenessStates();
    const localClientId = sharedModel.clientId;
    const now = Date.now();
    const out: OnlineUserSnapshot[] = [];
    for (const [clientId, state] of states) {
      if (!state || !state.user) continue;
      const key = clientId.toString();
      const lastActivity = this.lastActivityByClient.get(key) ?? now;
      const isIdle = !state.isActive || now - lastActivity > IDLE_THRESHOLD_MS;
      out.push({
        clientId: key,
        name: state.user.name ?? "Anonymous",
        color: state.user.color ?? "#888",
        isLocal: key === localClientId,
        isActive: !!state.isActive,
        isIdle,
        lastActivity,
        currentlyEditing: state.currentlyEditing,
        highlighted: state.highlighted,
      });
    }
    return out.sort((a, b) => (a.isLocal ? -1 : b.isLocal ? 1 : a.name.localeCompare(b.name)));
  }

  private serializeSnapshots(list: OnlineUserSnapshot[]): string {
    return list
      .map(u => `${u.clientId}:${u.isActive ? 1 : 0}:${u.isIdle ? 1 : 0}:${u.currentlyEditing ?? ""}`)
      .join("|");
  }

  private getAwarenessStates(): Map<number, CoeditorState> {
    const sharedModel = this.workflowActionService.getTexeraGraph()?.sharedModel;
    if (!sharedModel) return new Map();
    return sharedModel.awareness.getStates() as Map<number, CoeditorState>;
  }
}
