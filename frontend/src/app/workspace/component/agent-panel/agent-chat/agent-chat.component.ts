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
import { CopilotState, ReActStep } from "../../../service/copilot/texera-copilot";
import { AgentInfo, TexeraCopilotManagerService } from "../../../service/copilot/texera-copilot-manager.service";
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

  public responses: ReActStep[] = [];
  public currentMessage = "";
  private shouldScrollToBottom = false;
  public isDetailsModalVisible = false;
  public selectedResponse: ReActStep | null = null;
  public hoveredMessageIndex: number | null = null;
  public isSystemInfoModalVisible = false;
  public systemPrompt: string = "";
  public availableTools: Array<{ name: string; description: string; inputSchema: any }> = [];
  public agentState: CopilotState = CopilotState.UNAVAILABLE;

  constructor(
    private copilotManagerService: TexeraCopilotManagerService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    if (!this.agentInfo) {
      return;
    }

    // Subscribe to agent responses
    this.copilotManagerService
      .getReActStepsObservable(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(responses => {
        this.responses = responses;
        this.shouldScrollToBottom = true;
      });

    // Subscribe to agent state changes
    this.copilotManagerService
      .getAgentStateObservable(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(state => {
        this.agentState = state;
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

  public formatJson(data: any): string {
    return JSON.stringify(data, null, 2);
  }

  public getToolResult(response: ReActStep, toolCallIndex: number): any {
    if (!response.toolResults || toolCallIndex >= response.toolResults.length) {
      return null;
    }
    const toolResult = response.toolResults[toolCallIndex];
    return toolResult.output || toolResult.result || toolResult;
  }

  public getReActStepOperatorAccess(
    response: ReActStep,
    toolCallIndex: number
  ): { viewedOperatorIds: string[]; modifiedOperatorIds: string[] } | null {
    if (!response.toolResults || toolCallIndex >= response.toolResults.length) {
      return null;
    }
    const toolResult = response.toolResults[toolCallIndex];
    const result = toolResult.output || toolResult.result || toolResult;

    // Check if the result has operator access information
    if (result && (result.viewedOperatorIds || result.modifiedOperatorIds)) {
      return {
        viewedOperatorIds: result.viewedOperatorIds || [],
        modifiedOperatorIds: result.modifiedOperatorIds || [],
      };
    }

    return null;
  }

  public getTotalInputTokens(): number {
    // Iterate in reverse to find the most recent usage (already sorted by timestamp)
    for (let i = this.responses.length - 1; i >= 0; i--) {
      if (this.responses[i].usage?.inputTokens !== undefined) {
        return this.responses[i].usage!.inputTokens!;
      }
    }
    return 0;
  }

  public getTotalOutputTokens(): number {
    // Iterate in reverse to find the most recent usage (already sorted by timestamp)
    for (let i = this.responses.length - 1; i >= 0; i--) {
      if (this.responses[i].usage?.outputTokens !== undefined) {
        return this.responses[i].usage!.outputTokens!;
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
    this.copilotManagerService.stopGeneration(this.agentInfo.id).pipe(untilDestroyed(this)).subscribe();
  }

  public clearMessages(): void {
    this.copilotManagerService.clearMessages(this.agentInfo.id).pipe(untilDestroyed(this)).subscribe();
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
}
