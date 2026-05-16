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

import { Component, HostListener, Input, OnDestroy, OnInit, OnChanges, SimpleChanges } from "@angular/core";
import { FormsModule } from "@angular/forms";
import { Subscription } from "rxjs";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzResizeEvent, NzResizableDirective, NzResizeHandlesComponent } from "ng-zorro-antd/resizable";
import { AgentService, AgentInfo } from "../../../service/agent/agent.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { calculateTotalTranslate3d } from "../../../../common/util/panel-dock";
import { NgIf, NgClass, NgFor, DatePipe } from "@angular/common";
import { NzButtonComponent, NzButtonModule } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzIconDirective, NzIconModule } from "ng-zorro-antd/icon";
import { CdkDrag, CdkDragHandle } from "@angular/cdk/drag-drop";
import { NzMenuDirective, NzMenuItemComponent } from "ng-zorro-antd/menu";
import { NzSelectModule } from "ng-zorro-antd/select";
import { NzTagModule } from "ng-zorro-antd/tag";
import { NzEmptyModule } from "ng-zorro-antd/empty";
import { NzPopconfirmModule } from "ng-zorro-antd/popconfirm";
import { AgentChatComponent } from "./agent-chat/agent-chat.component";
import { CustomAgentService } from "../../../../dashboard/service/user/custom-agent/custom-agent.service";
import { CustomAgent } from "../../../../dashboard/type/custom-agent.interface";
import { Conversation, ConversationService } from "../../../service/agent/conversation.service";
import { ReActStep } from "../../../service/agent/agent-types";

const DEFAULT_AGENT_KEY = "default";
const DEFAULT_AGENT_NAME = "Default Agent";
const DEFAULT_AGENT_ICON = "🤖";

@UntilDestroy()
@Component({
  selector: "texera-agent-panel",
  templateUrl: "agent-panel.component.html",
  styleUrls: ["agent-panel.component.scss"],
  imports: [
    NgIf,
    NgFor,
    NgClass,
    DatePipe,
    FormsModule,
    NzButtonComponent,
    NzButtonModule,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NzTooltipDirective,
    NzIconDirective,
    NzIconModule,
    CdkDrag,
    CdkDragHandle,
    NzResizableDirective,
    NzResizeHandlesComponent,
    NzMenuDirective,
    NzMenuItemComponent,
    NzSelectModule,
    NzTagModule,
    NzEmptyModule,
    NzPopconfirmModule,
    AgentChatComponent,
  ],
})
export class AgentPanelComponent implements OnInit, OnDestroy, OnChanges {
  protected readonly window = window;
  protected readonly DEFAULT_AGENT_KEY = DEFAULT_AGENT_KEY;
  private static readonly MIN_PANEL_WIDTH = 400;
  private static readonly MIN_PANEL_HEIGHT = 450;

  /** Optional backend agent ID to activate when the panel loads (legacy entry point). */
  @Input() agentIdToActivate?: string;

  // Panel dimensions / position
  width: number = 0;
  height = Math.max(AgentPanelComponent.MIN_PANEL_HEIGHT, window.innerHeight * 0.7);
  id = -1;
  dragPosition = { x: 0, y: 0 };
  returnPosition = { x: 0, y: 0 };
  isDocked = true;

  // Two-view state
  public viewMode: "list" | "chat" = "list";

  /** The agent selected in the dropdown — used as the agent for the NEXT new conversation. */
  public selectedAgentKey: string = DEFAULT_AGENT_KEY;

  // Data sources
  public customAgents: CustomAgent[] = [];
  public conversations: Conversation[] = [];
  /** Current workflow id; undefined when no workflow is open or workflow is unsaved. */
  public currentWorkflowId?: number;

  // Active chat state
  public activeConversation: Conversation | null = null;
  public activeBackendAgent: AgentInfo | null = null;
  public isStartingConversation = false;

  // Cached defaults
  private defaultModelType: string | null = null;
  private allBackendAgents: AgentInfo[] = [];
  private persistedStepIds = new Set<string>();
  private listSubscription?: Subscription;

  constructor(
    private agentService: AgentService,
    private workflowActionService: WorkflowActionService,
    private notificationService: NotificationService,
    private customAgentService: CustomAgentService,
    private conversationService: ConversationService
  ) {}

  ngOnInit(): void {
    this.loadPanelSettings();

    this.customAgentService
      .list$()
      .pipe(untilDestroyed(this))
      .subscribe(agents => (this.customAgents = agents));

    // Seed and track current workflow id; reload conversation list on every change.
    this.currentWorkflowId = this.workflowActionService.getWorkflowMetadata().wid;
    this.reloadConversations();
    this.workflowActionService
      .workflowMetaDataChanged()
      .pipe(untilDestroyed(this))
      .subscribe(meta => {
        const newWid = meta?.wid;
        if (newWid === this.currentWorkflowId) return;
        this.currentWorkflowId = newWid;
        // Drop any active chat — it belongs to the previous workflow.
        this.resetActiveChat();
        this.reloadConversations();
      });

    this.agentService
      .fetchModelTypes()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: models => {
          if (models.length > 0 && !this.defaultModelType) {
            this.defaultModelType = models[0].id;
          }
        },
        error: () => {
          // surfaced when user attempts to start a conversation
        },
      });

    this.agentService.agentChange$.pipe(untilDestroyed(this)).subscribe(() => this.refreshBackendAgents());
    this.refreshBackendAgents();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["agentIdToActivate"] && this.agentIdToActivate) {
      this.activateFromBackendAgentId(this.agentIdToActivate);
      this.agentIdToActivate = undefined;
    }
  }

  @HostListener("window:beforeunload")
  ngOnDestroy(): void {
    this.deactivateCurrentBackendAgent();
    this.savePanelSettings();
  }

  // ---------- Panel open/close ----------

  public openPanel(): void {
    if (this.width === 0) {
      this.width = AgentPanelComponent.MIN_PANEL_WIDTH;
    } else {
      this.width = 0;
      this.isDocked = true;
    }
  }

  // ---------- Selector (controls which agent the next new conversation uses) ----------

  public selectedAgentLabel(): string {
    if (this.selectedAgentKey === DEFAULT_AGENT_KEY) return `${DEFAULT_AGENT_ICON} ${DEFAULT_AGENT_NAME}`;
    const agent = this.customAgents.find(a => a.id === this.selectedAgentKey);
    return agent ? `${agent.icon} ${agent.name}` : `${DEFAULT_AGENT_ICON} ${DEFAULT_AGENT_NAME}`;
  }

  public selectedAgentDescription(): string {
    if (this.selectedAgentKey === DEFAULT_AGENT_KEY) return "Built-in Texera assistant";
    return this.customAgents.find(a => a.id === this.selectedAgentKey)?.description ?? "";
  }

  public selectedCustomAgent(): CustomAgent | undefined {
    return this.customAgents.find(a => a.id === this.selectedAgentKey);
  }

  public onSelectAgentKey(key: string): void {
    if (key === this.selectedAgentKey) return;
    this.selectedAgentKey = key;
    // Conversations are scoped by (workflowId, agentId), so switching the
    // selected agent shows a different list and invalidates the active chat.
    this.resetActiveChat();
    this.reloadConversations();
  }

  private resetActiveChat(): void {
    this.viewMode = "list";
    this.activeConversation = null;
    this.deactivateCurrentBackendAgent();
  }

  // ---------- Conversation list (view 1) ----------

  private reloadConversations(): void {
    this.listSubscription?.unsubscribe();
    if (this.currentWorkflowId === undefined) {
      this.conversations = [];
      return;
    }
    this.listSubscription = this.conversationService
      .list$(this.currentWorkflowId, this.selectedAgentKey)
      .pipe(untilDestroyed(this))
      .subscribe(list => (this.conversations = list));
  }

  public openConversation(conversation: Conversation): void {
    // Keep the selector in sync with whatever agent owns this conversation,
    // so the visible list matches the open chat.
    if (conversation.agentId !== this.selectedAgentKey) {
      this.selectedAgentKey = conversation.agentId;
      this.reloadConversations();
    }
    this.activeConversation = conversation;
    this.viewMode = "chat";
    this.persistedStepIds.clear();
    this.attachBackendAgent(conversation);
  }

  public newConversation(): void {
    if (this.isStartingConversation) return;
    if (this.currentWorkflowId === undefined) {
      this.notificationService.warning("Save the workflow first to start a conversation.");
      return;
    }
    if (!this.defaultModelType) {
      this.notificationService.error("No LLM models available. Check LiteLLM is running.");
      return;
    }
    this.isStartingConversation = true;
    const customAgent = this.selectedCustomAgent();
    const conversation = this.conversationService.create({
      workflowId: this.currentWorkflowId,
      agentId: this.selectedAgentKey,
      agentName: customAgent ? customAgent.name : DEFAULT_AGENT_NAME,
      agentIcon: customAgent ? customAgent.icon || DEFAULT_AGENT_ICON : DEFAULT_AGENT_ICON,
    });
    this.activeConversation = conversation;
    this.viewMode = "chat";
    this.persistedStepIds.clear();
    this.createBackendAgentForConversation(conversation, customAgent);
  }

  public deleteConversation(conversation: Conversation): void {
    this.conversationService.delete(conversation.workflowId, conversation.agentId, conversation.id);
    if (this.activeConversation?.id === conversation.id) {
      this.activeConversation = null;
      this.deactivateCurrentBackendAgent();
      this.viewMode = "list";
    }
  }

  public conversationPreview(c: Conversation): string {
    if (c.messages.length === 0) return "No messages yet";
    return c.messages[0].content;
  }

  // ---------- Chat view (view 2) ----------

  public backToList(): void {
    this.viewMode = "list";
  }

  private refreshBackendAgents(): void {
    this.agentService
      .getAllAgents()
      .pipe(untilDestroyed(this))
      .subscribe(agents => (this.allBackendAgents = agents));
  }

  private attachBackendAgent(conversation: Conversation): void {
    const existing = conversation.lastBackendAgentId
      ? this.allBackendAgents.find(a => a.id === conversation.lastBackendAgentId)
      : undefined;
    if (existing) {
      this.deactivateCurrentBackendAgent();
      this.activeBackendAgent = existing;
      this.agentService.activateAgent(existing.id);
      this.subscribeToStepsForPersistence(existing.id, conversation);
    } else {
      const customAgent = this.customAgentService.list().find(a => a.id === conversation.agentId);
      this.createBackendAgentForConversation(conversation, customAgent);
    }
  }

  private createBackendAgentForConversation(conversation: Conversation, customAgent: CustomAgent | undefined): void {
    // Custom agents carry their own model; the built-in default agent falls
    // back to whatever LiteLLM offered first via /models.
    const modelType = customAgent?.model ?? this.defaultModelType;
    if (!modelType) {
      this.isStartingConversation = false;
      return;
    }
    if (conversation.workflowId !== this.currentWorkflowId) {
      // Sanity check: never attach a backend agent to a conversation that
      // belongs to a different workflow than the one currently open.
      this.isStartingConversation = false;
      return;
    }
    const customAgentName = customAgent ? `${customAgent.icon} ${customAgent.name}` : undefined;

    this.deactivateCurrentBackendAgent();
    this.agentService
      .createAgent(modelType, customAgentName, conversation.workflowId, customAgent)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: agentInfo => {
          this.activeBackendAgent = agentInfo;
          this.agentService.activateAgent(agentInfo.id);
          this.conversationService.setBackendAgentId(
            conversation.workflowId,
            conversation.agentId,
            conversation.id,
            agentInfo.id
          );
          this.activeConversation = { ...conversation, lastBackendAgentId: agentInfo.id };
          this.subscribeToStepsForPersistence(agentInfo.id, conversation);
          this.isStartingConversation = false;
          this.refreshBackendAgents();
        },
        error: () => {
          this.isStartingConversation = false;
          this.viewMode = "list";
          this.activeConversation = null;
        },
      });
  }

  private deactivateCurrentBackendAgent(): void {
    if (this.activeBackendAgent) {
      this.agentService.deactivateAgent(this.activeBackendAgent.id);
      this.activeBackendAgent = null;
    }
  }

  /**
   * Subscribe to ReAct steps for the active backend agent and persist them to
   * the active conversation. Each step is persisted at most once.
   */
  private subscribeToStepsForPersistence(backendAgentId: string, conversation: Conversation): void {
    this.agentService
      .getReActStepsObservable(backendAgentId)
      .pipe(untilDestroyed(this))
      .subscribe((steps: ReActStep[]) => {
        for (const step of steps) {
          if (this.persistedStepIds.has(step.id)) continue;
          if (!step.content) continue;
          const role = step.role === "user" ? "user" : "agent";
          const generatedWorkflow = Boolean(step.toolCalls && step.toolCalls.length > 0);
          this.conversationService.appendMessage(
            conversation.workflowId,
            conversation.agentId,
            conversation.id,
            role,
            step.content,
            generatedWorkflow
          );
          this.persistedStepIds.add(step.id);
        }
      });
  }

  private activateFromBackendAgentId(backendAgentId: string): void {
    if (this.width === 0) {
      this.width = AgentPanelComponent.MIN_PANEL_WIDTH;
    }
    if (this.currentWorkflowId === undefined) return;
    const found = this.conversationService.findByBackendAgentId(this.currentWorkflowId, backendAgentId);
    if (found) {
      this.openConversation(found);
    }
  }

  // ---------- Panel layout (resize/drag/persist) ----------

  onResize({ width, height }: NzResizeEvent): void {
    cancelAnimationFrame(this.id);
    this.id = requestAnimationFrame(() => {
      this.width = width!;
      this.height = height!;
    });
  }

  handleDragStart(): void {
    this.isDocked = false;
  }

  private loadPanelSettings(): void {
    const savedWidth = localStorage.getItem("agent-panel-width");
    const savedHeight = localStorage.getItem("agent-panel-height");
    const savedStyle = localStorage.getItem("agent-panel-style");
    const savedDocked = localStorage.getItem("agent-panel-docked");

    if (savedDocked === "false" && savedWidth) {
      const parsedWidth = Number(savedWidth);
      if (!isNaN(parsedWidth) && parsedWidth >= AgentPanelComponent.MIN_PANEL_WIDTH) {
        this.width = parsedWidth;
      }
    }

    if (savedHeight) {
      const parsedHeight = Number(savedHeight);
      if (!isNaN(parsedHeight) && parsedHeight >= AgentPanelComponent.MIN_PANEL_HEIGHT) {
        this.height = parsedHeight;
      }
    }

    if (savedStyle) {
      const container = document.getElementById("agent-container");
      if (container) {
        container.style.cssText = savedStyle;
        const translates = container.style.transform;
        const [xOffset, yOffset] = calculateTotalTranslate3d(translates);
        this.returnPosition = { x: -xOffset, y: -yOffset };
        this.isDocked = this.dragPosition.x === this.returnPosition.x && this.dragPosition.y === this.returnPosition.y;
      }
    }
  }

  private savePanelSettings(): void {
    localStorage.setItem("agent-panel-width", String(this.width));
    localStorage.setItem("agent-panel-height", String(this.height));
    localStorage.setItem("agent-panel-docked", String(this.width === 0));

    const container = document.getElementById("agent-container");
    if (container) {
      localStorage.setItem("agent-panel-style", container.style.cssText);
    }
  }
}
