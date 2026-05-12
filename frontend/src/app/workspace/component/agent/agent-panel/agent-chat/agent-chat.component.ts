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

import {
  Component,
  ViewChild,
  ElementRef,
  Input,
  OnInit,
  AfterViewChecked,
  ChangeDetectorRef,
  OnDestroy,
  OnChanges,
  SimpleChanges,
} from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Subject } from "rxjs";
import { distinctUntilChanged, filter, pairwise, startWith, takeUntil } from "rxjs/operators";
import { AgentState, ReActStep } from "../../../../service/agent/agent-types";
import { AgentInfo, AgentService } from "../../../../service/agent/agent.service";
import { WorkflowActionService } from "../../../../service/workflow-graph/model/workflow-action.service";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { WorkflowPersistService } from "../../../../../common/service/workflow-persist/workflow-persist.service";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NgIf, NgFor } from "@angular/common";
import { MarkdownComponent } from "ngx-markdown";
import { NzSpinComponent } from "ng-zorro-antd/spin";
import {
  NzInputDirective,
  NzAutosizeDirective,
  NzInputGroupComponent,
  NzInputGroupWhitSuffixOrPrefixDirective,
} from "ng-zorro-antd/input";
import { FormsModule } from "@angular/forms";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ReActStepDetailModalComponent } from "../react-step-detail-modal/react-step-detail-modal.component";
import { NzModalComponent, NzModalContentDirective } from "ng-zorro-antd/modal";
import { NzTabsComponent, NzTabComponent } from "ng-zorro-antd/tabs";
import { NzInputNumberComponent } from "ng-zorro-antd/input-number";
import { NzTagComponent } from "ng-zorro-antd/tag";
import { NzSwitchComponent } from "ng-zorro-antd/switch";

@UntilDestroy()
@Component({
  selector: "texera-agent-chat",
  templateUrl: "agent-chat.component.html",
  styleUrls: ["agent-chat.component.scss"],
  imports: [
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzTooltipDirective,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    NgIf,
    NgFor,
    MarkdownComponent,
    NzSpinComponent,
    NzInputDirective,
    FormsModule,
    NzAutosizeDirective,
    NzWaveDirective,
    ReActStepDetailModalComponent,
    NzModalComponent,
    NzModalContentDirective,
    NzTabsComponent,
    NzTabComponent,
    NzInputNumberComponent,
    NzTagComponent,
    NzInputGroupComponent,
    NzInputGroupWhitSuffixOrPrefixDirective,
    NzSwitchComponent,
  ],
})
export class AgentChatComponent implements OnInit, AfterViewChecked, OnDestroy, OnChanges {
  @Input() agentInfo!: AgentInfo;
  @Input() isActive: boolean = false;
  @ViewChild("messageContainer", { static: false }) messageContainer?: ElementRef;
  @ViewChild("messageInput", { static: false }) messageInput?: ElementRef;

  /** All steps (for timeline rendering) */
  public agentResponses: ReActStep[] = [];
  /** Steps on the HEAD path only (for chat rendering) */
  public visibleSteps: ReActStep[] = [];
  public currentMessage = "";
  private shouldScrollToBottom = false;
  public isDetailsModalVisible = false;
  public selectedResponse: ReActStep | null = null;
  public hoveredMessageIndex: number | null = null;
  public isSystemInfoModalVisible = false;
  public systemPrompt: string = "";
  public availableTools: Array<{ name: string; description: string; inputSchema: any }> = [];
  public agentState: AgentState = AgentState.UNAVAILABLE;

  // Current HEAD step ID in the version tree
  public currentHeadId: string | null = null;

  // System info modal state
  public settingsMaxCharLimit = 20000; // Default max characters for operator results
  public settingsMaxCellCharLimit = 4000; // Default max characters per cell
  public settingsToolTimeoutSeconds = 120; // 2 minutes default
  public settingsExecutionTimeoutMinutes = 10; // 10 minutes default
  public settingsMaxSteps = 10; // Default max steps per message
  public settingsAllowedOperatorTypes: string[] = []; // Allowed operator types for general mode
  public allAvailableOperatorTypes: Array<{ type: string; description: string }> = []; // All operator types from backend
  public operatorTypeSearchQuery = ""; // Search filter for operator types

  // Track if we disabled auto-persist so we can re-enable it on destroy
  private disabledAutoPersist = false;

  // Subject to control workflow subscription lifecycle
  private stopWorkflowSubscription$ = new Subject<void>();

  constructor(
    private agentService: AgentService,
    private workflowActionService: WorkflowActionService,
    private notificationService: NotificationService,
    private cdr: ChangeDetectorRef,
    private workflowPersistService: WorkflowPersistService
  ) {}

  ngOnInit(): void {
    if (!this.agentInfo) {
      return;
    }

    // Ensure workflow polling is started if we have a workflowId
    // This handles agents created via API that weren't created through the UI
    const workflowId = this.agentInfo.delegate?.workflowId;
    if (workflowId) {
      this.agentService.ensureWorkflowPolling(this.agentInfo.id, workflowId);
    }

    // Get the current state from manager service
    this.agentService
      .getAgentState(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(state => {
        this.agentState = state;
        // Immediately trigger change detection to show the current state
        this.cdr.detectChanges();
      });

    // Then subscribe to agent state changes (BehaviorSubject will immediately emit current value)
    this.agentService
      .getAgentStateObservable(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(state => {
        this.agentState = state;
        // Force immediate change detection
        this.cdr.detectChanges();
      });

    // Subscribe to ReActSteps
    this.agentService
      .getReActStepsObservable(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(steps => {
        const previousLength = this.visibleSteps.length;
        this.agentResponses = steps;
        this.updateVisibleSteps();
        this.shouldScrollToBottom = true;

        // Automatically highlight the latest visible step
        if (this.visibleSteps.length > 0) {
          const latestIndex = this.visibleSteps.length - 1;
          const previousLatestIndex = previousLength - 1;

          if (
            this.hoveredMessageIndex === null ||
            this.hoveredMessageIndex === previousLatestIndex ||
            this.hoveredMessageIndex >= this.visibleSteps.length
          ) {
            this.setHoveredMessage(latestIndex);
          }
        }

        // Trigger change detection
        this.cdr.detectChanges();
      });

    // Subscribe to HEAD changes
    this.agentService
      .getHeadIdObservable(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(headId => {
        this.currentHeadId = headId;
        this.updateVisibleSteps();
        this.cdr.detectChanges();
      });

    // Subscribe to agent state changes to manage auto-persist
    // Disable auto-persist when agent is GENERATING, re-enable when AVAILABLE
    this.agentService
      .getAgentStateObservable(this.agentInfo.id)
      .pipe(startWith(AgentState.UNAVAILABLE), pairwise(), untilDestroyed(this))
      .subscribe(([previousState, currentState]) => {
        // When agent starts generating, disable auto-persist
        if (currentState === AgentState.GENERATING && previousState !== AgentState.GENERATING) {
          this.workflowPersistService.setWorkflowPersistFlag(false);
          this.disabledAutoPersist = true;
        }

        // When agent finishes (becomes AVAILABLE from GENERATING/STOPPING), re-enable auto-persist
        if (
          currentState === AgentState.AVAILABLE &&
          (previousState === AgentState.GENERATING || previousState === AgentState.STOPPING)
        ) {
          this.workflowPersistService.setWorkflowPersistFlag(true);
          this.disabledAutoPersist = false;
        }
      });

    // Note: Workflow subscription is started/stopped via ngOnChanges based on isActive
    // This prevents automatic workflow switching when multiple agents are running

    // Start workflow subscription if already active
    if (this.isActive) {
      this.startWorkflowSubscription();
    }

    // Subscribe to scroll-to-step requests
    this.agentService.scrollToStep$.pipe(untilDestroyed(this)).subscribe(({ agentId, messageId, stepId }) => {
      if (agentId === this.agentInfo.id) {
        this.scrollToStep(messageId, stepId);
      }
    });
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["isActive"]) {
      if (this.isActive) {
        this.startWorkflowSubscription();
      } else {
        this.stopWorkflowSubscription();
      }
    }
  }

  /**
   * Start subscribing to workflow changes from the agent.
   * Only called when this agent tab is active.
   */
  private startWorkflowSubscription(): void {
    if (!this.agentInfo) {
      return;
    }

    // Stop any existing subscription first
    this.stopWorkflowSubscription$.next();

    this.agentService
      .getWorkflowObservable(this.agentInfo.id)
      .pipe(
        filter(workflow => workflow !== null),
        distinctUntilChanged((prev, curr) => {
          // Compare workflow content to avoid unnecessary reloads
          if (!prev || !curr) return false;
          return JSON.stringify(prev.content) === JSON.stringify(curr.content);
        }),
        takeUntil(this.stopWorkflowSubscription$),
        untilDestroyed(this)
      )
      .subscribe(workflow => {
        if (workflow) {
          this.workflowActionService.reloadWorkflow(workflow, false, false);
        }
      });
  }

  /**
   * Stop subscribing to workflow changes.
   * Called when switching away from this agent tab.
   */
  private stopWorkflowSubscription(): void {
    this.stopWorkflowSubscription$.next();
  }

  ngOnDestroy(): void {
    // Stop workflow subscription
    this.stopWorkflowSubscription$.next();
    this.stopWorkflowSubscription$.complete();

    // Re-enable auto-persist if we disabled it
    if (this.disabledAutoPersist) {
      this.workflowPersistService.setWorkflowPersistFlag(true);
    }
  }

  ngAfterViewChecked(): void {
    if (this.shouldScrollToBottom) {
      this.scrollToBottom();
      this.shouldScrollToBottom = false;
    }
  }

  public setHoveredMessage(index: number | null): void {
    // When unhovered (null), automatically revert to latest step
    if (index === null && this.visibleSteps.length > 0) {
      index = this.visibleSteps.length - 1;
    }

    this.hoveredMessageIndex = index;
    const hoveredStep = index !== null && index >= 0 ? this.visibleSteps[index] : null;
    this.agentService.setHoveredMessage(this.agentInfo.id, hoveredStep);
  }

  public showResponseDetails(response: ReActStep): void {
    this.selectedResponse = response;
    this.isDetailsModalVisible = true;
  }

  public closeDetailsModal(): void {
    this.isDetailsModalVisible = false;
    this.selectedResponse = null;
  }

  public showSystemInfo(): void {
    this.refreshSystemInfo();
    this.isSystemInfoModalVisible = true;
  }

  /**
   * Refresh system info from the agent.
   */
  private refreshSystemInfo(): void {
    this.agentService
      .getSystemInfo(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(systemInfo => {
        this.systemPrompt = systemInfo.systemPrompt;
        this.availableTools = systemInfo.tools;
      });

    // Fetch settings from server
    this.agentService
      .getAgentSettings(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(settings => {
        this.settingsMaxCharLimit = settings.maxOperatorResultCharLimit ?? 20000;
        this.settingsMaxCellCharLimit = settings.maxOperatorResultCellCharLimit ?? 4000;
        this.settingsToolTimeoutSeconds = settings.toolTimeoutSeconds ?? 120;
        this.settingsExecutionTimeoutMinutes = settings.executionTimeoutMinutes ?? 10;
        this.settingsMaxSteps = settings.maxSteps ?? 10;
        this.settingsAllowedOperatorTypes = settings.allowedOperatorTypes ?? [];
      });

    // Fetch all available operator types
    this.agentService
      .getAvailableOperatorTypes(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(types => {
        this.allAvailableOperatorTypes = types.sort((a, b) => a.type.localeCompare(b.type));
      });
  }

  public closeSystemInfoModal(): void {
    this.isSystemInfoModalVisible = false;
  }

  public getToolResult(response: ReActStep, toolCallIndex: number): any {
    if (!response.toolResults || toolCallIndex >= response.toolResults.length) {
      return null;
    }
    const toolResult = response.toolResults[toolCallIndex];
    return toolResult.output || toolResult.result || toolResult;
  }

  public getToolOperatorAccess(
    response: ReActStep,
    toolCallIndex: number
  ): { viewedOperatorIds: string[]; modifiedOperatorIds: string[] } | null {
    if (!response.operatorAccess) {
      return null;
    }
    return response.operatorAccess.get(toolCallIndex) || null;
  }

  public hasOperatorAccess(response: ReActStep): boolean {
    return !!response.operatorAccess && response.operatorAccess.size > 0;
  }

  public sendMessage(): void {
    if (!this.currentMessage.trim() || !this.canSendMessage()) {
      return;
    }

    const userMessage = this.currentMessage.trim();
    this.currentMessage = "";

    // Fire-and-forget; responses stream in via the WebSocket subscription.
    this.agentService.sendMessage(this.agentInfo.id, userMessage);
  }

  /**
   * Check if messages can be sent (only when agent is available).
   */
  public canSendMessage(): boolean {
    return this.agentState === AgentState.AVAILABLE;
  }

  /**
   * Get the NG-ZORRO icon type based on current agent state.
   */
  public getStateIcon(): string {
    switch (this.agentState) {
      case AgentState.AVAILABLE:
        return "check-circle";
      case AgentState.GENERATING:
      case AgentState.STOPPING:
        return "sync";
      case AgentState.UNAVAILABLE:
      default:
        return "close-circle";
    }
  }

  /**
   * Get the icon color based on current agent state.
   */
  public getStateIconColor(): string {
    switch (this.agentState) {
      case AgentState.AVAILABLE:
        return "#52c41a";
      case AgentState.GENERATING:
      case AgentState.STOPPING:
        return "#1890ff";
      case AgentState.UNAVAILABLE:
      default:
        return "#ff4d4f";
    }
  }

  /**
   * Get the tooltip text for the state icon.
   */
  public getStateTooltip(): string {
    switch (this.agentState) {
      case AgentState.AVAILABLE:
        return "Agent is ready";
      case AgentState.GENERATING:
        return "Agent is generating response...";
      case AgentState.STOPPING:
        return "Agent is stopping...";
      case AgentState.UNAVAILABLE:
        return "Agent is unavailable";
      default:
        return "Agent status unknown";
    }
  }

  public onEnterPress(event: KeyboardEvent): void {
    if (!event.shiftKey) {
      event.preventDefault();
      this.sendMessage();
    }
  }

  private scrollToBottom(): void {
    if (this.messageContainer) {
      const element = this.messageContainer.nativeElement;
      element.scrollTop = element.scrollHeight;
    }
  }

  public stopGeneration(): void {
    this.agentService.stopGeneration(this.agentInfo.id);
  }

  public clearMessages(): void {
    this.agentService.clearMessages(this.agentInfo.id);
  }

  /**
   * Export the ReAct steps as a JSON file.
   * Fetches steps from the backend to get clean JSON (without Map objects).
   */
  public exportReActSteps(): void {
    if (this.visibleSteps.length === 0) {
      this.notificationService.warning("No ReAct steps to export");
      return;
    }

    this.agentService
      .getReActSteps(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: (steps: ReActStep[]) => {
          // Convert steps to plain objects (handle Map -> object for operatorAccess)
          const exportSteps = steps.map(step => {
            const plain: any = { ...step };
            if (step.operatorAccess) {
              const accessObj: Record<string, any> = {};
              step.operatorAccess.forEach((value, key) => {
                accessObj[key] = value;
              });
              plain.operatorAccess = accessObj;
            }
            return plain;
          });

          const exportData = {
            agentId: this.agentInfo.id,
            agentName: this.agentInfo.name,
            modelType: this.agentInfo.modelType,
            exportedAt: new Date().toISOString(),
            stepCount: exportSteps.length,
            steps: exportSteps,
          };

          const jsonString = JSON.stringify(exportData, null, 2);
          const blob = new Blob([jsonString], { type: "application/json" });
          const url = URL.createObjectURL(blob);

          const link = document.createElement("a");
          link.href = url;
          link.download = `${this.agentInfo.name}-react-steps-${new Date().toISOString().slice(0, 19).replace(/:/g, "-")}.json`;
          document.body.appendChild(link);
          link.click();
          document.body.removeChild(link);

          URL.revokeObjectURL(url);

          this.notificationService.success(`Exported ${exportSteps.length} ReAct steps`);
        },
        error: (err: unknown) => {
          console.error("Failed to export ReAct steps:", err);
          this.notificationService.error("Failed to export ReAct steps");
        },
      });
  }

  public isGenerating(): boolean {
    return this.agentState === AgentState.GENERATING;
  }

  public isAvailable(): boolean {
    return this.agentState === AgentState.AVAILABLE;
  }

  public isConnected(): boolean {
    return this.agentState !== AgentState.UNAVAILABLE;
  }

  public isStopping(): boolean {
    return this.agentState === AgentState.STOPPING;
  }

  /**
   * Recompute visibleSteps: only steps on the ancestor path from root to HEAD.
   */
  private updateVisibleSteps(): void {
    if (!this.currentHeadId || this.agentResponses.length === 0) {
      this.visibleSteps = this.agentResponses;
      return;
    }
    const stepMap = new Map(this.agentResponses.map(s => [s.id, s]));
    const ancestorIds = new Set<string>();
    let current: string | undefined = this.currentHeadId;
    while (current) {
      ancestorIds.add(current);
      current = stepMap.get(current)?.parentId;
    }
    this.visibleSteps = this.agentResponses.filter(s => ancestorIds.has(s.id));
  }

  /**
   * Scroll chat messages to a specific step index.
   */
  private scrollToMessage(stepIndex: number): void {
    if (!this.messageContainer) {
      return;
    }

    const container = this.messageContainer.nativeElement;
    const messages = container.querySelectorAll(".message");

    if (stepIndex >= 0 && stepIndex < messages.length) {
      messages[stepIndex].scrollIntoView({ behavior: "smooth", block: "center" });
    }
  }

  /**
   * Save the max character limit.
   */
  public saveMaxCharLimit(): void {
    this.agentService
      .updateAgentSettings(this.agentInfo.id, {
        maxOperatorResultCharLimit: this.settingsMaxCharLimit,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => this.notificationService.success("Max character limit saved"),
        error: () => {}, // Error already handled by service
      });
  }

  /**
   * Save the max cell character limit.
   */
  public saveMaxCellCharLimit(): void {
    this.agentService
      .updateAgentSettings(this.agentInfo.id, {
        maxOperatorResultCellCharLimit: this.settingsMaxCellCharLimit,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => this.notificationService.success("Max cell character limit saved"),
        error: () => {}, // Error already handled by service
      });
  }

  /**
   * Save the tool execution timeout.
   */
  public saveToolTimeout(): void {
    this.agentService
      .updateAgentSettings(this.agentInfo.id, {
        toolTimeoutSeconds: this.settingsToolTimeoutSeconds,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => this.notificationService.success("Tool timeout saved"),
        error: () => {}, // Error already handled by service
      });
  }

  /**
   * Save the workflow execution timeout.
   */
  public saveExecutionTimeout(): void {
    this.agentService
      .updateAgentSettings(this.agentInfo.id, {
        executionTimeoutMinutes: this.settingsExecutionTimeoutMinutes,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => this.notificationService.success("Execution timeout saved"),
        error: () => {}, // Error already handled by service
      });
  }

  /**
   * Save the max steps per message setting.
   */
  public saveMaxSteps(): void {
    this.agentService
      .updateAgentSettings(this.agentInfo.id, {
        maxSteps: this.settingsMaxSteps,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => this.notificationService.success("Max steps saved"),
        error: () => {}, // Error already handled by service
      });
  }

  /**
   * Toggle an operator type in the allowed list and save.
   */
  public toggleOperatorType(operatorType: string, enabled: boolean): void {
    if (enabled) {
      if (!this.settingsAllowedOperatorTypes.includes(operatorType)) {
        this.settingsAllowedOperatorTypes = [...this.settingsAllowedOperatorTypes, operatorType];
      }
    } else {
      this.settingsAllowedOperatorTypes = this.settingsAllowedOperatorTypes.filter(t => t !== operatorType);
    }
    this.saveAllowedOperatorTypes();
  }

  /**
   * Check if an operator type is enabled (in allowed list).
   */
  public isOperatorTypeEnabled(operatorType: string): boolean {
    return this.settingsAllowedOperatorTypes.includes(operatorType);
  }

  /**
   * Enable all operator types.
   */
  public enableAllOperatorTypes(): void {
    this.settingsAllowedOperatorTypes = this.allAvailableOperatorTypes.map(op => op.type);
    this.saveAllowedOperatorTypes();
  }

  /**
   * Deselect all operator types.
   */
  public deselectAllOperatorTypes(): void {
    this.settingsAllowedOperatorTypes = [];
    this.saveAllowedOperatorTypes();
  }

  /**
   * Get filtered operator types based on search query.
   */
  public getFilteredOperatorTypes(): Array<{ type: string; description: string }> {
    if (!this.operatorTypeSearchQuery) {
      return this.allAvailableOperatorTypes;
    }
    const query = this.operatorTypeSearchQuery.toLowerCase();
    return this.allAvailableOperatorTypes.filter(
      op => op.type.toLowerCase().includes(query) || op.description.toLowerCase().includes(query)
    );
  }

  /**
   * Save allowed operator types to backend.
   */
  private saveAllowedOperatorTypes(): void {
    this.agentService
      .updateAgentSettings(this.agentInfo.id, {
        allowedOperatorTypes: this.settingsAllowedOperatorTypes,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          const count = this.settingsAllowedOperatorTypes.length;
          this.notificationService.success(count === 0 ? "All operators enabled" : `${count} operators enabled`);
        },
        error: () => {},
      });
  }

  /**
   * Scroll to a specific step in the chat by messageId and stepId.
   */
  private scrollToStep(messageId: string, stepId: number): void {
    // Find the step index in visibleSteps
    const stepIndex = this.visibleSteps.findIndex(step => step.messageId === messageId && step.stepId === stepId);

    if (stepIndex >= 0) {
      this.scrollToMessage(stepIndex);
      // Highlight the message briefly
      this.setHoveredMessage(stepIndex);
    }
  }
}
