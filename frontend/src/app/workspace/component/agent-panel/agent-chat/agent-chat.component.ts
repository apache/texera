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
  OnDestroy,
  AfterViewChecked,
  ChangeDetectorRef,
  ChangeDetectionStrategy,
} from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { CopilotState, ReActStep, CopilotMessageStats } from "../../../service/copilot/texera-copilot";
import { AgentInfo, TexeraCopilotManagerService } from "../../../service/copilot/texera-copilot-manager.service";
import { ActionPlan, ActionPlanService } from "../../../service/action-plan/action-plan.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { WorkflowVersionService } from "../../../../dashboard/service/user/workflow-version/workflow-version.service";

@UntilDestroy()
@Component({
  selector: "texera-agent-chat",
  templateUrl: "agent-chat.component.html",
  styleUrls: ["agent-chat.component.scss"],
})
export class AgentChatComponent implements OnInit, OnDestroy, AfterViewChecked {
  @Input() agentInfo!: AgentInfo;
  @ViewChild("messageContainer", { static: false }) messageContainer?: ElementRef;
  @ViewChild("messageInput", { static: false }) messageInput?: ElementRef;

  public agentResponses: ReActStep[] = [];
  public currentMessage = "";
  public pendingActionPlan: ActionPlan | null = null;
  private shouldScrollToBottom = false;
  public planningMode = false;
  public isDetailsModalVisible = false;
  public selectedResponse: ReActStep | null = null;
  public hoveredMessageIndex: number | null = null;
  public isSystemInfoModalVisible = false;
  public systemPrompt: string = "";
  public availableTools: Array<{ name: string; description: string; inputSchema: any }> = [];
  public agentState: CopilotState = CopilotState.UNAVAILABLE;
  public isStatsModalVisible = false;
  public messageStats: CopilotMessageStats[] = [];
  public isWaitingForActionPlanApproval = false;
  public pendingActionPlanId?: string;

  constructor(
    private actionPlanService: ActionPlanService,
    private copilotManagerService: TexeraCopilotManagerService,
    private workflowActionService: WorkflowActionService,
    private notificationService: NotificationService,
    private cdr: ChangeDetectorRef,
    private workflowVersionService: WorkflowVersionService
  ) {}

  ngOnInit(): void {
    if (!this.agentInfo) {
      return;
    }

    this.planningMode = this.copilotManagerService.getPlanningMode(this.agentInfo.id);

    // First, get the current state synchronously to ensure we have it immediately
    const agent = this.agentInfo.instance;
    if (agent) {
      const currentState = agent.getState();
      this.agentState = currentState;
      // Immediately trigger change detection to show the current state
      this.cdr.detectChanges();
    }

    // Then subscribe to agent state changes (BehaviorSubject will immediately emit current value)
    this.copilotManagerService
      .getAgentStateObservable(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(state => {
        this.agentState = state;
        // Force immediate change detection
        this.cdr.detectChanges();
      });

    // Subscribe to ReActSteps
    this.copilotManagerService
      .getReActStepsObservable(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(steps => {
        const previousLength = this.agentResponses.length;
        this.agentResponses = steps;
        this.shouldScrollToBottom = true;

        // Automatically highlight the latest ReAct step
        if (steps.length > 0) {
          const latestIndex = steps.length - 1;
          const previousLatestIndex = previousLength - 1;

          // Auto-highlight the latest if:
          // 1. No message is currently hovered, OR
          // 2. We were hovering the previous latest (so update to new latest)
          if (
            this.hoveredMessageIndex === null ||
            this.hoveredMessageIndex === previousLatestIndex ||
            this.hoveredMessageIndex >= steps.length
          ) {
            this.setHoveredMessage(latestIndex);
          }
        }

        // Trigger change detection
        this.cdr.detectChanges();
      });

    // Subscribe to pending action plans (only emitted when both version IDs are set)
    this.actionPlanService
      .getPendingActionPlanStream()
      .pipe(untilDestroyed(this))
      .subscribe(plan => {
        if (plan && plan.agentId === this.agentInfo.id) {
          this.pendingActionPlan = plan;
          this.shouldScrollToBottom = true;

          console.log("[Agent Chat] Received pending action plan with workflow contents", plan);

          // Try to show diff preview
          this.tryShowActionPlanDiff(plan);
        } else if (plan === null || (plan && plan.agentId !== this.agentInfo.id)) {
          this.pendingActionPlan = null;
        }
        this.cdr.detectChanges();
      });

    // Subscribe to message stats changes
    this.copilotManagerService
      .getMessageStatsObservable(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(statsMap => {
        this.messageStats = Array.from(statsMap.values());
        this.cdr.detectChanges();
      });

    // Subscribe to action plan approval state
    this.copilotManagerService
      .getActionPlanApprovalObservable(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(approvalState => {
        this.isWaitingForActionPlanApproval = approvalState.isWaitingForApproval;
        this.pendingActionPlanId = approvalState.actionPlanId;
        this.cdr.detectChanges();
      });
  }

  ngOnDestroy(): void {
    // Cleanup handled by @UntilDestroy
  }

  ngAfterViewChecked(): void {
    if (this.shouldScrollToBottom) {
      this.scrollToBottom();
      this.shouldScrollToBottom = false;
    }
  }

  public setHoveredMessage(index: number | null): void {
    // When unhovered (null), automatically revert to latest step
    if (index === null && this.agentResponses.length > 0) {
      index = this.agentResponses.length - 1;
    }

    this.hoveredMessageIndex = index;
    // Notify the copilot service about the hovered message
    const hoveredStep = index !== null && index >= 0 ? this.agentResponses[index] : null;
    this.copilotManagerService.setHoveredMessage(this.agentInfo.id, hoveredStep);
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
    this.copilotManagerService.getSystemInfo(this.agentInfo.id).subscribe(systemInfo => {
      this.systemPrompt = systemInfo.systemPrompt;
      this.availableTools = systemInfo.tools;
      this.isSystemInfoModalVisible = true;
    });
  }

  public closeSystemInfoModal(): void {
    this.isSystemInfoModalVisible = false;
  }

  public showStatsModal(): void {
    this.isStatsModalVisible = true;
  }

  public closeStatsModal(): void {
    this.isStatsModalVisible = false;
  }

  public formatJson(data: any): string {
    return JSON.stringify(data, null, 2);
  }

  public getExecutionTime(stat: CopilotMessageStats): string {
    if (!stat.endTime) {
      return "Running...";
    }
    const duration = stat.endTime.getTime() - stat.startTime.getTime();
    const seconds = Math.floor(duration / 1000);
    const ms = duration % 1000;
    return `${seconds}.${ms.toString().padStart(3, "0")}s`;
  }

  public getStatusColor(status: string): string {
    switch (status) {
      case "completed":
        return "#52c41a";
      case "running":
        return "#1890ff";
      case "error":
        return "#ff4d4f";
      case "stopped":
        return "#faad14";
      default:
        return "#8c8c8c";
    }
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

  public getTotalInputTokens(): number {
    for (let i = this.agentResponses.length - 1; i >= 0; i--) {
      const response = this.agentResponses[i];
      if (response.usage?.inputTokens !== undefined) {
        return response.usage.inputTokens;
      }
    }
    return 0;
  }

  public getTotalOutputTokens(): number {
    for (let i = this.agentResponses.length - 1; i >= 0; i--) {
      const response = this.agentResponses[i];
      if (response.usage?.outputTokens !== undefined) {
        return response.usage.outputTokens;
      }
    }
    return 0;
  }

  /**
   * Send a message to the agent via the copilot manager service.
   */
  public sendMessage(): void {
    if (!this.currentMessage.trim() || !this.canSendMessage()) {
      return;
    }

    const userMessage = this.currentMessage.trim();
    this.currentMessage = "";

    // Send to copilot via manager service (fire-and-forget)
    this.copilotManagerService.sendMessage(this.agentInfo.id, userMessage);
  }

  /**
   * Check if messages can be sent (only when agent is available).
   */
  public canSendMessage(): boolean {
    return this.agentState === CopilotState.AVAILABLE;
  }

  /**
   * Get the state icon URL based on current agent state.
   * Uses the same icons as workflow operators for consistency.
   */
  public getStateIconUrl(): string {
    return this.agentState === CopilotState.AVAILABLE ? "assets/svg/done.svg" : "assets/gif/loading.gif";
  }

  /**
   * Get the tooltip text for the state icon.
   */
  public getStateTooltip(): string {
    switch (this.agentState) {
      case CopilotState.AVAILABLE:
        return "Agent is ready";
      case CopilotState.GENERATING:
        return "Agent is generating response...";
      case CopilotState.STOPPING:
        return "Agent is stopping...";
      case CopilotState.UNAVAILABLE:
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
    this.copilotManagerService.stopGeneration(this.agentInfo.id);
  }

  public clearMessages(): void {
    this.copilotManagerService.clearMessages(this.agentInfo.id);
  }

  public isGenerating(): boolean {
    return this.agentState === CopilotState.GENERATING;
  }

  public isAvailable(): boolean {
    return this.agentState === CopilotState.AVAILABLE;
  }

  public isConnected(): boolean {
    return this.agentState !== CopilotState.UNAVAILABLE;
  }

  public isStopping(): boolean {
    return this.agentState === CopilotState.STOPPING;
  }

  public onPlanningModeChange(value: boolean): void {
    this.copilotManagerService.setPlanningMode(this.agentInfo.id, value);
  }

  /**
   * Try to show action plan diff preview if conditions are met
   */
  private tryShowActionPlanDiff(plan: ActionPlan): void {
    // Show diff preview in planning mode
    if (!this.planningMode) {
      console.log("[Agent Chat] Not in planning mode, skipping diff preview");
      return;
    }

    console.log("[Agent Chat] Showing action plan diff preview");
    try {
      this.actionPlanService.previewActionPlanDiff(plan.id);
      console.log("[Agent Chat] Action plan diff preview displayed");
    } catch (err) {
      console.error("[Agent Chat] Failed to show action plan preview:", err);
    }
  }

  /**
   * Approve the pending action plan
   */
  public onApproveActionPlan(): void {
    // Accept the action plan if in planning mode (clears highlights, keeps after version)
    if (this.planningMode) {
      console.log("[Agent Chat] Accepting action plan (clearing highlights, keeping after version)");
      this.actionPlanService.acceptActionPlan();
    }

    // Construct the approval message
    const feedback = this.currentMessage.trim();
    const message = feedback
      ? `I approve this action plan. Additional feedback: ${feedback}`
      : "I approve this action plan. Please proceed with execution.";

    // Send message via manager service (this will automatically clear approval state)
    this.copilotManagerService.sendMessage(this.agentInfo.id, message);
    this.currentMessage = "";
  }

  /**
   * Reject the pending action plan
   */
  public onRejectActionPlan(): void {
    // Reject the action plan if in planning mode (clears highlights, reverts to original workflow)
    if (this.planningMode) {
      console.log("[Agent Chat] Rejecting action plan (reverting to original workflow)");
      this.actionPlanService.rejectActionPlan();
    }

    // Construct the rejection message
    const feedback = this.currentMessage.trim();
    const message = feedback
      ? `I reject this action plan. Reason: ${feedback}`
      : "I reject this action plan. Please revise your approach.";

    // Send message via manager service (this will automatically clear approval state)
    this.copilotManagerService.sendMessage(this.agentInfo.id, message);
    this.currentMessage = "";
  }
}
