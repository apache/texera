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

import { Component, ViewChild, ElementRef, Input, OnInit, AfterViewChecked } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { CopilotState, AgentUIMessage } from "../../../service/copilot/texera-copilot";
import { AgentInfo, TexeraCopilotManagerService } from "../../../service/copilot/texera-copilot-manager.service";
import { ActionPlan, ActionPlanService } from "../../../service/action-plan/action-plan.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";

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

  public agentResponses: AgentUIMessage[] = [];
  public currentMessage = "";
  public pendingActionPlan: ActionPlan | null = null;
  private shouldScrollToBottom = false;
  public planningMode = false;
  public isDetailsModalVisible = false;
  public selectedResponse: AgentUIMessage | null = null;
  public hoveredMessageIndex: number | null = null;
  public isSystemInfoModalVisible = false;
  public systemPrompt: string = "";
  public availableTools: Array<{ name: string; description: string; inputSchema: any }> = [];

  constructor(
    private actionPlanService: ActionPlanService,
    private copilotManagerService: TexeraCopilotManagerService,
    private workflowActionService: WorkflowActionService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    if (!this.agentInfo) {
      return;
    }

    this.planningMode = this.copilotManagerService.getPlanningMode(this.agentInfo.id);

    // Subscribe to agent responses
    this.copilotManagerService
      .getAgentResponsesObservable(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(responses => {
        this.agentResponses = responses;
        this.shouldScrollToBottom = true;
      });

    // Subscribe to pending action plans
    this.actionPlanService
      .getPendingActionPlanStream()
      .pipe(untilDestroyed(this))
      .subscribe(plan => {
        if (plan && plan.agentId === this.agentInfo.id) {
          this.pendingActionPlan = plan;
          this.shouldScrollToBottom = true;
        } else if (plan === null || (plan && plan.agentId !== this.agentInfo.id)) {
          this.pendingActionPlan = null;
        }
      });
  }

  ngAfterViewChecked(): void {
    if (this.shouldScrollToBottom) {
      this.scrollToBottom();
      this.shouldScrollToBottom = false;
    }
  }

  public setHoveredMessage(index: number | null): void {
    this.hoveredMessageIndex = index;
  }

  public showResponseDetails(response: AgentUIMessage): void {
    this.selectedResponse = response;
    this.isDetailsModalVisible = true;
  }

  public closeDetailsModal(): void {
    this.isDetailsModalVisible = false;
    this.selectedResponse = null;
  }

  public showSystemInfo(): void {
    const systemInfo = this.copilotManagerService.getSystemInfo(this.agentInfo.id);
    this.systemPrompt = systemInfo.systemPrompt;
    this.availableTools = systemInfo.tools;
    this.isSystemInfoModalVisible = true;
  }

  public closeSystemInfoModal(): void {
    this.isSystemInfoModalVisible = false;
  }

  public formatJson(data: any): string {
    return JSON.stringify(data, null, 2);
  }

  public getToolResult(response: AgentUIMessage, toolCallIndex: number): any {
    if (!response.toolResults || toolCallIndex >= response.toolResults.length) {
      return null;
    }
    const toolResult = response.toolResults[toolCallIndex];
    return toolResult.output || toolResult.result || toolResult;
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
    if (!this.currentMessage.trim() || this.isGenerating()) {
      return;
    }

    const userMessage = this.currentMessage.trim();
    this.currentMessage = "";

    // Send to copilot via manager service
    this.copilotManagerService
      .sendMessage(this.agentInfo.id, userMessage)
      .pipe(untilDestroyed(this))
      .subscribe({
        error: (error: unknown) => {
          this.notificationService.error(`Error sending message: ${error}`);
        },
      });
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
    return this.copilotManagerService.getAgentState(this.agentInfo.id) === CopilotState.GENERATING;
  }

  public isStopping(): boolean {
    return this.copilotManagerService.getAgentState(this.agentInfo.id) === CopilotState.STOPPING;
  }

  public isAvailable(): boolean {
    return this.copilotManagerService.getAgentState(this.agentInfo.id) === CopilotState.AVAILABLE;
  }

  public isConnected(): boolean {
    return this.copilotManagerService.isAgentConnected(this.agentInfo.id);
  }

  public onPlanningModeChange(value: boolean): void {
    this.copilotManagerService.setPlanningMode(this.agentInfo.id, value);
  }

  /**
   * Handle user decision on action plan (acceptance or rejection).
   */
  public onUserDecision(decision: {
    accepted: boolean;
    message: string;
    createNewActor?: boolean;
    planId?: string;
  }): void {
    this.pendingActionPlan = null;

    if (decision.planId) {
      if (decision.accepted) {
        this.actionPlanService.acceptPlan(decision.planId);

        if (decision.createNewActor) {
          this.copilotManagerService
            .createAgent("claude-3.7", `Actor for Plan ${decision.planId}`)
            .then(newAgent => {
              const initialMessage = `Please work on action plan with id: ${decision.planId}`;
              this.copilotManagerService
                .sendMessage(newAgent.id, initialMessage)
                .pipe(untilDestroyed(this))
                .subscribe({
                  next: () => {
                    this.notificationService.info(`Actor agent started for plan: ${decision.planId}`);
                  },
                  error: (error: unknown) => {
                    this.notificationService.error(`Error starting actor agent: ${error}`);
                  },
                });
            })
            .catch((error: unknown) => {
              this.notificationService.error(`Failed to create actor agent: ${error}`);
            });
        } else {
          const executionMessage = "I have accepted your action plan. Please proceed with executing it.";
          this.copilotManagerService
            .sendMessage(this.agentInfo.id, executionMessage)
            .pipe(untilDestroyed(this))
            .subscribe({
              error: (error: unknown) => {
                this.notificationService.error(`Error sending acceptance message: ${error}`);
              },
            });
        }
      } else {
        const feedbackMatch = decision.message.match(/Feedback: (.+)$/);
        const userFeedback = feedbackMatch ? feedbackMatch[1] : "I don't want this action plan.";

        const actionPlan = this.actionPlanService.getActionPlan(decision.planId);
        if (actionPlan) {
          this.workflowActionService.deleteOperatorsAndLinks(actionPlan.operatorIds);
        }

        this.actionPlanService.rejectPlan(userFeedback, decision.planId);

        const rejectionMessage = `I have rejected your action plan. Feedback: ${userFeedback}`;
        this.copilotManagerService
          .sendMessage(this.agentInfo.id, rejectionMessage)
          .pipe(untilDestroyed(this))
          .subscribe({
            error: (error: unknown) => {
              this.notificationService.error(`Error sending rejection feedback: ${error}`);
            },
          });
      }
    }
  }
}
