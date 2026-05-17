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

import { AfterViewChecked, Component, ElementRef, OnInit, ViewChild } from "@angular/core";
import { AsyncPipe, DatePipe, NgClass, NgFor, NgIf, NgSwitch, NgSwitchCase } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzTagComponent } from "ng-zorro-antd/tag";
import { NzTooltipModule } from "ng-zorro-antd/tooltip";
import { combineLatest, Observable } from "rxjs";
import { map } from "rxjs/operators";
import { CollaborationService } from "../../service/collaboration/collaboration.service";
import {
  ChatMessage,
  CollaborationTab,
  OnlineUserSnapshot,
  OperatorCommentThread,
} from "../../service/collaboration/collaboration.types";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";

interface CommentsTabViewModel {
  byOperator: Array<{
    operatorId: string;
    operatorName: string;
    threads: OperatorCommentThread[];
  }>;
}

@UntilDestroy()
@Component({
  selector: "texera-collaboration-panel",
  templateUrl: "./collaboration-panel.component.html",
  styleUrls: ["./collaboration-panel.component.scss"],
  imports: [
    AsyncPipe,
    DatePipe,
    FormsModule,
    NgClass,
    NgFor,
    NgIf,
    NgSwitch,
    NgSwitchCase,
    NzButtonComponent,
    NzIconDirective,
    NzTagComponent,
    NzTooltipModule,
  ],
})
export class CollaborationPanelComponent implements OnInit, AfterViewChecked {
  @ViewChild("chatScroll") chatScrollRef?: ElementRef<HTMLDivElement>;

  public readonly open$: Observable<boolean>;
  public readonly activeTab$: Observable<CollaborationTab>;
  public readonly chatMessages$: Observable<ChatMessage[]>;
  public readonly online$: Observable<OnlineUserSnapshot[]>;
  public readonly comments$: Observable<CommentsTabViewModel>;

  public chatDraft = "";
  public replyDraftByThread: Record<string, string> = {};
  public showResolved = false;
  public draftOperatorId$: Observable<string | null>;
  public newThreadDraft = "";

  private prevChatLength = 0;

  constructor(
    public collab: CollaborationService,
    private workflowActionService: WorkflowActionService
  ) {
    this.open$ = this.collab.open$();
    this.activeTab$ = this.collab.activeTab$();
    this.chatMessages$ = this.collab.chat$();
    this.online$ = this.collab.onlineUsers$();
    this.comments$ = combineLatest([this.collab.threads$(), this.collab.draftThreadOperatorId$()]).pipe(
      map(([threads, draftId]) => this.groupThreads(threads, draftId))
    );
    this.draftOperatorId$ = this.collab.draftThreadOperatorId$();
  }

  ngOnInit(): void {
    this.chatMessages$.pipe(untilDestroyed(this)).subscribe(msgs => {
      if (msgs.length !== this.prevChatLength) {
        this.prevChatLength = msgs.length;
        setTimeout(() => this.scrollChatToBottom(), 0);
      }
    });
  }

  ngAfterViewChecked(): void {
    // No-op; chat scroll handled in subscription.
  }

  public selectTab(tab: CollaborationTab): void {
    this.collab.setActiveTab(tab);
  }

  public close(): void {
    this.collab.closePanel();
  }

  // ───────────────────── chat ─────────────────────

  public sendChat(): void {
    const text = this.chatDraft.trim();
    if (!text) return;
    this.collab.sendChat(text);
    this.chatDraft = "";
  }

  public onChatKeydown(event: KeyboardEvent): void {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      this.sendChat();
    }
  }

  private scrollChatToBottom(): void {
    const el = this.chatScrollRef?.nativeElement;
    if (el) el.scrollTop = el.scrollHeight;
  }

  // ───────────────────── comments ─────────────────────

  public goToOperator(operatorId: string): void {
    if (!this.workflowActionService.getTexeraGraph().hasOperator(operatorId)) return;
    this.workflowActionService.highlightOperators(false, operatorId);
  }

  public submitReply(thread: OperatorCommentThread): void {
    const text = (this.replyDraftByThread[thread.root.id] ?? "").trim();
    if (!text) return;
    this.collab.addComment(thread.operatorId, text, thread.root.id);
    this.replyDraftByThread[thread.root.id] = "";
  }

  public onReplyKeydown(event: KeyboardEvent, thread: OperatorCommentThread): void {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      this.submitReply(thread);
    }
  }

  public toggleResolve(thread: OperatorCommentThread): void {
    this.collab.toggleResolveThread(thread.root.id);
  }

  public submitNewThread(operatorId: string): void {
    const text = this.newThreadDraft.trim();
    if (!text) return;
    this.collab.addComment(operatorId, text);
    this.newThreadDraft = "";
    this.collab.clearDraftThreadOperator();
  }

  public cancelNewThread(): void {
    this.newThreadDraft = "";
    this.collab.clearDraftThreadOperator();
  }

  public onNewThreadKeydown(event: KeyboardEvent, operatorId: string): void {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      this.submitNewThread(operatorId);
    } else if (event.key === "Escape") {
      this.cancelNewThread();
    }
  }

  public trackThread = (_: number, thread: OperatorCommentThread) => thread.root.id;
  public trackReply = (_: number, reply: { id: string }) => reply.id;
  public trackMessage = (_: number, msg: ChatMessage) => msg.id;
  public trackUser = (_: number, u: OnlineUserSnapshot) => u.clientId;
  public trackOperatorGroup = (_: number, g: { operatorId: string }) => g.operatorId;

  public initial(name: string): string {
    return (name || "?").trim().charAt(0).toUpperCase() || "?";
  }

  public operatorDisplayName(operatorId: string): string {
    const op = this.workflowActionService.getTexeraGraph().getOperator(operatorId);
    return op?.customDisplayName || op?.operatorType || operatorId;
  }

  private groupThreads(threads: OperatorCommentThread[], draftOperatorId: string | null): CommentsTabViewModel {
    const groups = new Map<string, OperatorCommentThread[]>();
    for (const t of threads) {
      const list = groups.get(t.operatorId) ?? [];
      list.push(t);
      groups.set(t.operatorId, list);
    }
    if (draftOperatorId && !groups.has(draftOperatorId)) {
      groups.set(draftOperatorId, []);
    }
    const entries = Array.from(groups.entries()).map(([operatorId, ts]) => ({
      operatorId,
      operatorName: this.operatorDisplayName(operatorId),
      threads: ts,
    }));
    if (draftOperatorId) {
      entries.sort((a, b) => {
        if (a.operatorId === draftOperatorId) return -1;
        if (b.operatorId === draftOperatorId) return 1;
        return 0;
      });
    }
    return { byOperator: entries };
  }

  // ───────────────────── online tab helpers ─────────────────────

  public onlineDescription(user: OnlineUserSnapshot): string {
    if (user.isIdle) {
      const mins = Math.floor((Date.now() - user.lastActivity) / 60000);
      return mins > 0 ? `Idle (${mins} min)` : "Idle";
    }
    if (user.currentlyEditing) {
      const name = this.operatorDisplayName(user.currentlyEditing);
      return `Editing: ${name}`;
    }
    if (user.highlighted && user.highlighted.length > 0) {
      const name = this.operatorDisplayName(user.highlighted[0]);
      return `Viewing: ${name}`;
    }
    return "Active";
  }
}
