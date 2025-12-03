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

import { Component, ViewChild, ElementRef, Input, OnInit, AfterViewChecked, ChangeDetectorRef } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { CopilotState, ReActStep, CopilotMessageStats } from "../../../service/copilot/texera-copilot";
import { AgentInfo, TexeraCopilotManagerService } from "../../../service/copilot/texera-copilot-manager.service";
import {
  AgentAction,
  AgentActionService,
  AgentActionPreviewState,
} from "../../../service/agent-action/agent-action.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { WorkflowVersionService } from "../../../../dashboard/service/user/workflow-version/workflow-version.service";
import {
  ToolGroup,
  TOOL_GROUP_CONFIGS,
  getToolGroup,
  getToolColor,
  getToolGroupConfig,
} from "../../../service/copilot/tool/tool-groups";

/**
 * Represents a single node in the tool call timeline.
 */
export interface TimelineNode {
  id: string;
  toolName: string;
  toolGroup: ToolGroup;
  color: string;
  stepIndex: number;
  toolCallIndex: number;
  messageId: string;
  timestamp: Date;
}

@UntilDestroy()
@Component({
  selector: "texera-agent-chat",
  templateUrl: "agent-chat.component.html",
  styleUrls: ["agent-chat.component.scss"],
})
export class AgentChatComponent implements OnInit, AfterViewChecked {
  @Input() agentInfo!: AgentInfo;
  @ViewChild("messageContainer", { static: false }) messageContainer?: ElementRef;
  @ViewChild("messageInput", { static: false }) messageInput?: ElementRef;
  @ViewChild("timelineContainer", { static: false }) timelineContainer?: ElementRef;

  public agentResponses: ReActStep[] = [];
  public currentMessage = "";
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

  // Timeline-related properties
  public timelineNodes: TimelineNode[] = [];
  public hoveredTimelineNodeId: string | null = null;
  public toolGroupConfigs = TOOL_GROUP_CONFIGS;
  public ToolGroup = ToolGroup;

  // Unified agent action preview state
  public previewState: AgentActionPreviewState | null = null;

  constructor(
    private agentActionService: AgentActionService,
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

        // Rebuild timeline nodes whenever responses change
        this.buildTimelineNodes();

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

    // Subscribe to unified preview state
    this.agentActionService
      .getPreviewStateStream()
      .pipe(untilDestroyed(this))
      .subscribe(state => {
        // Only show preview UI if the agent action belongs to this agent
        if (state && state.agentAction.agentId === this.agentInfo.id) {
          this.previewState = state;
          this.shouldScrollToBottom = true;
          console.log("[Agent Chat] Preview state updated", state);
        } else {
          this.previewState = null;
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
  }

  // Cleanup handled by @UntilDestroy decorator

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
    this.copilotManagerService
      .getSystemInfo(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(systemInfo => {
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
   * Get the NG-ZORRO icon type based on current agent state.
   */
  public getStateIcon(): string {
    switch (this.agentState) {
      case CopilotState.AVAILABLE:
        return "check-circle";
      case CopilotState.GENERATING:
      case CopilotState.STOPPING:
        return "sync";
      case CopilotState.UNAVAILABLE:
      default:
        return "close-circle";
    }
  }

  /**
   * Get the icon color based on current agent state.
   */
  public getStateIconColor(): string {
    switch (this.agentState) {
      case CopilotState.AVAILABLE:
        return "#52c41a";
      case CopilotState.GENERATING:
      case CopilotState.STOPPING:
        return "#1890ff";
      case CopilotState.UNAVAILABLE:
      default:
        return "#ff4d4f";
    }
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
   * Accept the agent action (for pending mode) or Apply (for historical mode)
   */
  public onAcceptAgentAction(): void {
    if (!this.previewState) {
      return;
    }

    // End preview and apply the changes
    this.agentActionService.endPreview(true);

    // In pending mode, send approval message to continue the agent
    if (this.previewState.isPending) {
      const feedback = this.currentMessage.trim();
      const message = feedback
        ? `I approve this agent action. Additional feedback: ${feedback}`
        : "I approve this agent action. Please proceed with execution.";
      this.copilotManagerService.sendMessage(this.agentInfo.id, message);
      this.currentMessage = "";
    }
  }

  /**
   * Reject the agent action (for pending mode) or Cancel (for historical mode)
   */
  public onRejectAgentAction(): void {
    if (!this.previewState) {
      return;
    }

    // End preview and reject the changes (restore to before state)
    this.agentActionService.endPreview(false);

    // In pending mode, send rejection message to the agent
    if (this.previewState.isPending) {
      const feedback = this.currentMessage.trim();
      const message = feedback
        ? `I reject this agent action. Reason: ${feedback}`
        : "I reject this agent action. Please revise your approach.";
      this.copilotManagerService.sendMessage(this.agentInfo.id, message);
      this.currentMessage = "";
    }
  }

  // =====================
  // Timeline Methods
  // =====================

  /**
   * Build timeline nodes from agent responses.
   * Each tool call becomes a node in the timeline.
   */
  private buildTimelineNodes(): void {
    const nodes: TimelineNode[] = [];

    this.agentResponses.forEach((step, stepIndex) => {
      if (step.toolCalls && step.toolCalls.length > 0) {
        step.toolCalls.forEach((toolCall, toolCallIndex) => {
          const toolName = toolCall.toolName || "unknown";
          const toolGroup = getToolGroup(toolName);
          const node: TimelineNode = {
            id: `${step.messageId}-${stepIndex}-${toolCallIndex}`,
            toolName,
            toolGroup,
            color: getToolColor(toolName),
            stepIndex,
            toolCallIndex,
            messageId: step.messageId,
            timestamp: step.timestamp,
          };
          nodes.push(node);
        });
      }
    });

    this.timelineNodes = nodes;
  }

  /**
   * Check if a timeline node belongs to the currently hovered message.
   */
  public isNodeHighlighted(node: TimelineNode): boolean {
    if (this.hoveredMessageIndex === null) {
      return false;
    }
    return node.stepIndex === this.hoveredMessageIndex;
  }

  /**
   * Handle mouse enter on timeline node.
   * Scrolls chat to the corresponding message on hover.
   */
  public onTimelineNodeHover(node: TimelineNode): void {
    this.hoveredTimelineNodeId = node.id;
    // Highlight the corresponding message
    this.setHoveredMessage(node.stepIndex);
    // Scroll chat to the message
    this.scrollToMessage(node.stepIndex);
  }

  /**
   * Handle mouse leave on timeline node.
   */
  public onTimelineNodeLeave(): void {
    this.hoveredTimelineNodeId = null;
  }

  /**
   * Get the icon for a timeline node based on its group.
   */
  public getTimelineNodeIcon(node: TimelineNode): string {
    return getToolGroupConfig(node.toolGroup).icon;
  }

  /**
   * Handle click on a timeline node.
   * Shows agent action preview for Modify group nodes.
   */
  public onTimelineNodeClick(node: TimelineNode): void {
    // For Modify group nodes, show the agent action preview
    if (node.toolGroup === ToolGroup.MODIFY) {
      this.showAgentActionPreviewForNode(node);
    }
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
   * Show agent action preview for a Modify group timeline node.
   * Finds the agent action associated with the tool call and displays diff preview.
   */
  private showAgentActionPreviewForNode(node: TimelineNode): void {
    const step = this.agentResponses[node.stepIndex];
    if (!step || !step.toolCalls || node.toolCallIndex >= step.toolCalls.length) {
      console.log("[Timeline] No step or tool calls found for node", node);
      return;
    }

    const toolCall = step.toolCalls[node.toolCallIndex];
    const toolResult = step.toolResults?.[node.toolCallIndex];

    console.log("[Timeline] Looking for agent action in tool call:", toolCall.toolName, {
      toolCall,
      toolResult,
      nodeTimestamp: node.timestamp,
    });

    // Try to extract agent action ID from the tool result
    let agentActionId: string | null = null;

    if (toolResult) {
      // Check if result contains agent action ID directly
      if (typeof toolResult === "object" && toolResult.agentActionId) {
        agentActionId = toolResult.agentActionId;
      } else if (typeof toolResult === "object" && toolResult.id) {
        agentActionId = toolResult.id;
      } else if (typeof toolResult === "string") {
        // Try to parse JSON result
        try {
          const parsed = JSON.parse(toolResult);
          agentActionId = parsed.agentActionId || parsed.id;
        } catch {
          // Not JSON, check for ID pattern in string
          const match = toolResult.match(/agent-action-[\w-]+/);
          if (match) {
            agentActionId = match[0];
          }
        }
      }
    }

    // Also check tool call input for agent action ID
    if (!agentActionId && toolCall.input) {
      try {
        const input = typeof toolCall.input === "string" ? JSON.parse(toolCall.input) : toolCall.input;
        agentActionId = input.agentActionId || input.id;
      } catch {
        // Ignore parse errors
      }
    }

    // Fallback: Find agent action by matching timestamp (closest before or at the node timestamp)
    if (!agentActionId) {
      const allActions = this.agentActionService.getAllAgentActions();
      console.log("[Timeline] No agent action ID found, matching by timestamp. Actions:", allActions.length);

      if (allActions.length > 0) {
        const nodeTime = node.timestamp.getTime();

        // Sort actions by creation time (oldest first)
        const sortedActions = [...allActions].sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime());

        // Find actions created before or at the node timestamp
        const actionsBeforeNode = sortedActions.filter(a => a.createdAt.getTime() <= nodeTime);

        if (actionsBeforeNode.length > 0) {
          // Get the latest action that was created before or at the node timestamp
          agentActionId = actionsBeforeNode[actionsBeforeNode.length - 1].id;
        } else {
          // If no actions before, use the first (oldest) action
          agentActionId = sortedActions[0].id;
        }

        console.log("[Timeline] Found agent action by timestamp:", agentActionId);
      }
    }

    if (agentActionId) {
      console.log("[Timeline] Previewing agent action:", agentActionId);
      this.previewAgentAction(agentActionId);
    } else {
      console.log("[Timeline] No agent action found to preview");
      this.notificationService.warning("No agent action found for this operation");
    }
  }

  /**
   * Preview an agent action by ID (historical mode - from timeline click).
   */
  public previewAgentAction(agentActionId: string): void {
    try {
      this.agentActionService.startHistoricalPreview(agentActionId);
    } catch (err) {
      console.error("Failed to preview agent action:", err);
      this.notificationService.error("Failed to preview agent action");
    }
  }

  // =====================
  // Agent Action Navigation
  // =====================

  /**
   * Get all agent actions for this agent sorted by creation time (chronological order).
   */
  private getAgentActionsForAgent(): AgentAction[] {
    return this.agentActionService
      .getAllAgentActions()
      .filter(action => action.agentId === this.agentInfo.id)
      .sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime());
  }

  /**
   * Get the index of the current preview agent action in the sorted list.
   */
  private getCurrentAgentActionIndex(): number {
    if (!this.previewState) {
      return -1;
    }
    const actions = this.getAgentActionsForAgent();
    return actions.findIndex(a => a.id === this.previewState!.agentAction.id);
  }

  /**
   * Check if there is a previous agent action to navigate to.
   */
  public hasPreviousAgentAction(): boolean {
    return this.getCurrentAgentActionIndex() > 0;
  }

  /**
   * Check if there is a next agent action to navigate to.
   */
  public hasNextAgentAction(): boolean {
    const actions = this.getAgentActionsForAgent();
    const currentIndex = this.getCurrentAgentActionIndex();
    return currentIndex >= 0 && currentIndex < actions.length - 1;
  }

  /**
   * Navigate to the previous agent action in chronological order.
   */
  public navigateToPreviousAgentAction(): void {
    if (!this.hasPreviousAgentAction()) {
      return;
    }
    const actions = this.getAgentActionsForAgent();
    const currentIndex = this.getCurrentAgentActionIndex();
    const previousAction = actions[currentIndex - 1];

    // End current preview without applying changes, then start new preview
    this.agentActionService.endPreview(false);
    this.agentActionService.startHistoricalPreview(previousAction.id);
  }

  /**
   * Navigate to the next agent action in chronological order.
   */
  public navigateToNextAgentAction(): void {
    if (!this.hasNextAgentAction()) {
      return;
    }
    const actions = this.getAgentActionsForAgent();
    const currentIndex = this.getCurrentAgentActionIndex();
    const nextAction = actions[currentIndex + 1];

    // End current preview without applying changes, then start new preview
    this.agentActionService.endPreview(false);
    this.agentActionService.startHistoricalPreview(nextAction.id);
  }

  /**
   * Get the current agent action position string (e.g., "2 / 5").
   */
  public getAgentActionPositionLabel(): string {
    const actions = this.getAgentActionsForAgent();
    const currentIndex = this.getCurrentAgentActionIndex();
    if (currentIndex < 0 || actions.length === 0) {
      return "";
    }
    return `${currentIndex + 1} / ${actions.length}`;
  }
}
