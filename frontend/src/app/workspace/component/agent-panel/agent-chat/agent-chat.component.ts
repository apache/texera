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
  QueryList,
  ViewChildren,
} from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { CopilotState, ReActStep, CopilotMessageStats } from "../../../service/copilot/texera-copilot";
import { AgentInfo, TexeraCopilotManagerService } from "../../../service/copilot/texera-copilot-manager.service";
import { ActionPlan, ActionPlanService } from "../../../service/action-plan/action-plan.service";
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
export class AgentChatComponent implements OnInit, OnDestroy, AfterViewChecked {
  @Input() agentInfo!: AgentInfo;
  @ViewChild("messageContainer", { static: false }) messageContainer?: ElementRef;
  @ViewChild("messageInput", { static: false }) messageInput?: ElementRef;
  @ViewChild("timelineContainer", { static: false }) timelineContainer?: ElementRef;
  @ViewChildren("timelineNode") timelineNodeElements?: QueryList<ElementRef>;

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

  // Timeline-related properties
  public timelineNodes: TimelineNode[] = [];
  public hoveredTimelineNodeId: string | null = null;
  public toolGroupConfigs = TOOL_GROUP_CONFIGS;
  public ToolGroup = ToolGroup;

  // Action plan preview state (for timeline node clicks)
  public isPreviewingActionPlan = false;
  public previewingActionPlanId: string | null = null;
  public previewingActionPlan: ActionPlan | null = null;

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

    // Scroll timeline to show the highlighted nodes
    if (index !== null) {
      this.scrollTimelineToStep(index);
    }
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
    // Accept the action plan if in planning mode
    if (this.planningMode && this.pendingActionPlan) {
      this.actionPlanService.setWorkflowToActionPlan(this.pendingActionPlan.id, false);
    }

    // Construct the approval message
    const feedback = this.currentMessage.trim();
    const message = feedback
      ? `I approve this action plan. Additional feedback: ${feedback}`
      : "I approve this action plan. Please proceed with execution.";

    // Send message via manager service
    this.copilotManagerService.sendMessage(this.agentInfo.id, message);
    this.currentMessage = "";
  }

  /**
   * Reject the pending action plan
   */
  public onRejectActionPlan(): void {
    // Reject the action plan if in planning mode
    if (this.planningMode && this.pendingActionPlan) {
      this.actionPlanService.setWorkflowToActionPlan(this.pendingActionPlan.id, true);
    }

    // Construct the rejection message
    const feedback = this.currentMessage.trim();
    const message = feedback
      ? `I reject this action plan. Reason: ${feedback}`
      : "I reject this action plan. Please revise your approach.";

    // Send message via manager service
    this.copilotManagerService.sendMessage(this.agentInfo.id, message);
    this.currentMessage = "";
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
   * Get timeline nodes for a specific step index.
   */
  public getTimelineNodesForStep(stepIndex: number): TimelineNode[] {
    return this.timelineNodes.filter(node => node.stepIndex === stepIndex);
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
   * Scroll timeline to show nodes for the hovered message.
   */
  private scrollTimelineToStep(stepIndex: number): void {
    if (!this.timelineContainer || !this.timelineNodeElements) {
      return;
    }

    // Find the first node for this step
    const nodesForStep = this.getTimelineNodesForStep(stepIndex);
    if (nodesForStep.length === 0) {
      return;
    }

    const nodeIndex = this.timelineNodes.findIndex(n => n.stepIndex === stepIndex);
    if (nodeIndex === -1) {
      return;
    }

    const nodeElements = this.timelineNodeElements.toArray();
    if (nodeIndex < nodeElements.length) {
      const element = nodeElements[nodeIndex].nativeElement;
      element.scrollIntoView({ behavior: "smooth", block: "center" });
    }
  }

  /**
   * Get the tooltip text for a timeline node.
   */
  public getTimelineNodeTooltip(node: TimelineNode): string {
    const groupConfig = getToolGroupConfig(node.toolGroup);
    return `${node.toolName}\nGroup: ${groupConfig.group}\nStep: ${node.stepIndex}`;
  }

  /**
   * Get the icon for a timeline node based on its group.
   */
  public getTimelineNodeIcon(node: TimelineNode): string {
    return getToolGroupConfig(node.toolGroup).icon;
  }

  /**
   * Handle click on a timeline node.
   * Shows action plan preview for Modify group nodes.
   */
  public onTimelineNodeClick(node: TimelineNode): void {
    // For Modify group nodes, show the action plan preview
    if (node.toolGroup === ToolGroup.MODIFY) {
      this.showActionPlanPreviewForNode(node);
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
   * Show action plan preview for a Modify group timeline node.
   * Finds the action plan associated with the tool call and displays diff preview.
   */
  private showActionPlanPreviewForNode(node: TimelineNode): void {
    const step = this.agentResponses[node.stepIndex];
    if (!step || !step.toolCalls || node.toolCallIndex >= step.toolCalls.length) {
      console.log("[Timeline] No step or tool calls found for node", node);
      return;
    }

    const toolCall = step.toolCalls[node.toolCallIndex];
    const toolResult = step.toolResults?.[node.toolCallIndex];

    console.log("[Timeline] Looking for action plan in tool call:", toolCall.toolName, { toolCall, toolResult, nodeTimestamp: node.timestamp });

    // Try to extract action plan ID from the tool result
    let actionPlanId: string | null = null;

    if (toolResult) {
      // Check if result contains action plan ID directly
      if (typeof toolResult === "object" && toolResult.actionPlanId) {
        actionPlanId = toolResult.actionPlanId;
      } else if (typeof toolResult === "object" && toolResult.id) {
        actionPlanId = toolResult.id;
      } else if (typeof toolResult === "string") {
        // Try to parse JSON result
        try {
          const parsed = JSON.parse(toolResult);
          actionPlanId = parsed.actionPlanId || parsed.id;
        } catch {
          // Not JSON, check for ID pattern in string
          const match = toolResult.match(/action-plan-[\w-]+/);
          if (match) {
            actionPlanId = match[0];
          }
        }
      }
    }

    // Also check tool call input for action plan ID
    if (!actionPlanId && toolCall.input) {
      try {
        const input = typeof toolCall.input === "string" ? JSON.parse(toolCall.input) : toolCall.input;
        actionPlanId = input.actionPlanId || input.id;
      } catch {
        // Ignore parse errors
      }
    }

    // Fallback: Find action plan by matching timestamp (closest before or at the node timestamp)
    if (!actionPlanId) {
      const allPlans = this.actionPlanService.getAllActionPlans();
      console.log("[Timeline] No action plan ID found, matching by timestamp. Plans:", allPlans.length);

      if (allPlans.length > 0) {
        const nodeTime = node.timestamp.getTime();

        // Sort plans by creation time (oldest first)
        const sortedPlans = [...allPlans].sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime());

        // Find plans created before or at the node timestamp
        const plansBeforeNode = sortedPlans.filter(p => p.createdAt.getTime() <= nodeTime);

        if (plansBeforeNode.length > 0) {
          // Get the latest plan that was created before or at the node timestamp
          actionPlanId = plansBeforeNode[plansBeforeNode.length - 1].id;
        } else {
          // If no plans before, use the first (oldest) plan
          actionPlanId = sortedPlans[0].id;
        }

        console.log("[Timeline] Found action plan by timestamp:", actionPlanId);
      }
    }

    if (actionPlanId) {
      console.log("[Timeline] Previewing action plan:", actionPlanId);
      this.previewActionPlan(actionPlanId);
    } else {
      console.log("[Timeline] No action plan found to preview");
      this.notificationService.warning("No action plan found for this operation");
    }
  }

  /**
   * Preview an action plan by ID.
   * Shows the diff view and changes input area to Rollback/Cancel mode.
   */
  public previewActionPlan(actionPlanId: string): void {
    const actionPlan = this.actionPlanService.getActionPlan(actionPlanId);
    if (!actionPlan) {
      this.notificationService.warning("Action plan not found");
      return;
    }

    try {
      // Show diff preview
      this.actionPlanService.previewActionPlanDiff(actionPlanId);

      // Set preview state
      this.isPreviewingActionPlan = true;
      this.previewingActionPlanId = actionPlanId;
      this.previewingActionPlan = actionPlan;

      this.cdr.detectChanges();
    } catch (err) {
      console.error("Failed to preview action plan:", err);
      this.notificationService.error("Failed to preview action plan");
    }
  }

  /**
   * Rollback: Apply the after version of the action plan.
   */
  public onRollbackActionPlan(): void {
    if (!this.previewingActionPlanId) {
      return;
    }

    this.actionPlanService.setWorkflowToActionPlan(this.previewingActionPlanId, false);
    this.clearActionPlanPreview();
  }

  /**
   * Cancel: Clear preview and restore original workflow state.
   */
  public onCancelActionPlanPreview(): void {
    if (!this.previewingActionPlanId) {
      return;
    }

    // Restore to before version (cancel means don't apply changes)
    this.actionPlanService.setWorkflowToActionPlan(this.previewingActionPlanId, true);
    this.clearActionPlanPreview();
  }

  /**
   * Clear the action plan preview state.
   */
  private clearActionPlanPreview(): void {
    this.isPreviewingActionPlan = false;
    this.previewingActionPlanId = null;
    this.previewingActionPlan = null;
    this.cdr.detectChanges();
  }

  /**
   * Get a summary of the action plan operations for display.
   */
  public getActionPlanOperationsSummary(): string {
    if (!this.previewingActionPlan) {
      return "";
    }

    const ops = this.previewingActionPlan.operations;
    const parts: string[] = [];

    if (ops.add.operatorIds.length > 0) {
      parts.push(`+${ops.add.operatorIds.length} op`);
    }
    if (ops.add.linkIds.length > 0) {
      parts.push(`+${ops.add.linkIds.length} link`);
    }
    if (ops.modify.operatorIds.length > 0) {
      parts.push(`~${ops.modify.operatorIds.length} op`);
    }
    if (ops.delete.operatorIds.length > 0) {
      parts.push(`-${ops.delete.operatorIds.length} op`);
    }
    if (ops.delete.linkIds.length > 0) {
      parts.push(`-${ops.delete.linkIds.length} link`);
    }

    return parts.join(", ");
  }
}
