// agent-chat.component.ts
import { Component, ViewChild, ElementRef, Input, OnInit, AfterViewChecked } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { CopilotState, AgentResponse } from "../../../service/copilot/texera-copilot";
import { AgentInfo, TexeraCopilotManagerService } from "../../../service/copilot/texera-copilot-manager.service";
import { DomSanitizer, SafeHtml } from "@angular/platform-browser";
import { ActionPlan, ActionPlanService } from "../../../service/action-plan/action-plan.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";

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

  public agentResponses: AgentResponse[] = []; // Populated from observable subscription
  public currentMessage = "";
  public pendingActionPlan: ActionPlan | null = null;
  private shouldScrollToBottom = false;

  constructor(
    private sanitizer: DomSanitizer,
    private actionPlanService: ActionPlanService,
    private copilotManagerService: TexeraCopilotManagerService,
    private workflowActionService: WorkflowActionService
  ) {}

  ngOnInit(): void {
    console.log("AgentChatComponent ngOnInit - agentInfo:", this.agentInfo);

    if (!this.agentInfo) {
      console.error("AgentInfo is not provided!");
      return;
    }

    // Subscribe to agent responses stream from the manager service
    this.copilotManagerService
      .getAgentResponsesObservable(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(responses => {
        console.log(`AgentChatComponent for ${this.agentInfo.id} received ${responses.length} responses`);
        console.log(responses);
        this.agentResponses = responses;
        this.shouldScrollToBottom = true;
      });

    // Subscribe to pending action plans
    this.actionPlanService
      .getPendingActionPlanStream()
      .pipe(untilDestroyed(this))
      .subscribe(plan => {
        // Only show plans from this agent
        if (plan && plan.agentId === this.agentInfo.id) {
          this.pendingActionPlan = plan;
          this.shouldScrollToBottom = true;
        } else if (plan === null || (plan && plan.agentId !== this.agentInfo.id)) {
          // Clear pending plan if it's null or belongs to another agent
          this.pendingActionPlan = null;
        }
      });

    console.log("AgentChatComponent initialized successfully");
  }

  ngAfterViewChecked(): void {
    if (this.shouldScrollToBottom) {
      this.scrollToBottom();
      this.shouldScrollToBottom = false;
    }
  }

  /**
   * Format message content to display markdown-like text
   */
  public formatMessageContent(content: string): SafeHtml {
    const text = content || "";

    // Simple markdown-like formatting
    let formatted = text
      // Bold: **text**
      .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
      // Code: `code`
      .replace(/`(.+?)`/g, "<code>$1</code>")
      // Line breaks
      .replace(/\n/g, "<br>");

    return this.sanitizer.sanitize(1, formatted) || "";
  }

  /**
   * Format tool calls to show name and args
   */
  public formatToolCalls(toolCalls: any[]): string {
    return toolCalls
      .map(call => {
        const name = call.toolName || "unknown";
        const args = JSON.stringify(call.args || {}, null, 2);
        return `Tool: ${name}\nArgs: ${args}`;
      })
      .join("\n\n");
  }

  /**
   * Format tool results to show name and result
   */
  public formatToolResults(toolResults: any[]): string {
    return toolResults
      .map(result => {
        const name = result.toolName || "unknown";
        const resultData = JSON.stringify(result.result || result, null, 2);
        return `Tool: ${name}\nResult: ${resultData}`;
      })
      .join("\n\n");
  }

  /**
   * Get total input tokens across all responses
   */
  public getTotalInputTokens(): number {
    return this.agentResponses.reduce((total, response) => {
      const inputTokens = response.usage?.inputTokens || 0;
      return total + inputTokens;
    }, 0);
  }

  /**
   * Get total output tokens across all responses
   */
  public getTotalOutputTokens(): number {
    return this.agentResponses.reduce((total, response) => {
      const outputTokens = response.usage?.outputTokens || 0;
      return total + outputTokens;
    }, 0);
  }

  /**
   * Send a message to the agent
   * Messages are automatically updated via the messages$ observable
   */
  public sendMessage(): void {
    if (!this.currentMessage.trim() || this.isGenerating()) {
      return;
    }

    const userMessage = this.currentMessage.trim();
    this.currentMessage = "";

    // Send to copilot via manager service
    // Messages are automatically updated via the observable subscription
    this.copilotManagerService
      .sendMessage(this.agentInfo.id, userMessage)
      .pipe(untilDestroyed(this))
      .subscribe({
        error: (error: unknown) => {
          console.error("Error sending message:", error);
        },
      });
  }

  /**
   * Handle Enter key press in textarea
   */
  public onEnterPress(event: KeyboardEvent): void {
    if (!event.shiftKey) {
      event.preventDefault();
      this.sendMessage();
    }
  }

  /**
   * Scroll messages container to bottom
   */
  private scrollToBottom(): void {
    if (this.messageContainer) {
      const element = this.messageContainer.nativeElement;
      element.scrollTop = element.scrollHeight;
    }
  }

  /**
   * Stop the current generation
   */
  public stopGeneration(): void {
    this.copilotManagerService.stopGeneration(this.agentInfo.id);
  }

  /**
   * Clear message history
   */
  public clearMessages(): void {
    this.copilotManagerService.clearMessages(this.agentInfo.id);
  }

  /**
   * Check if copilot is currently generating
   */
  public isGenerating(): boolean {
    return this.copilotManagerService.getAgentState(this.agentInfo.id) === CopilotState.GENERATING;
  }

  /**
   * Check if copilot is currently stopping
   */
  public isStopping(): boolean {
    return this.copilotManagerService.getAgentState(this.agentInfo.id) === CopilotState.STOPPING;
  }

  /**
   * Check if copilot is available (can send messages)
   */
  public isAvailable(): boolean {
    return this.copilotManagerService.getAgentState(this.agentInfo.id) === CopilotState.AVAILABLE;
  }

  /**
   * Check if agent is connected
   */
  public isConnected(): boolean {
    return this.copilotManagerService.isAgentConnected(this.agentInfo.id);
  }

  /**
   * Handle user decision on action plan
   */
  public onUserDecision(decision: {
    accepted: boolean;
    message: string;
    createNewActor?: boolean;
    planId?: string;
  }): void {
    // Clear the pending action plan since user has made a decision
    this.pendingActionPlan = null;

    // Handle plan acceptance or rejection
    if (decision.planId) {
      if (decision.accepted) {
        // Register plan acceptance
        this.actionPlanService.acceptPlan(decision.planId);

        // If user chose to run in new agent, create one (non-blocking)
        if (decision.createNewActor) {
          // Create new actor agent
          this.copilotManagerService
            .createAgent("claude-3.7", `Actor for Plan ${decision.planId}`)
            .then(newAgent => {
              // Send the initial message to the new agent (also non-blocking)
              const initialMessage = `Please work on action plan with id: ${decision.planId}`;
              this.copilotManagerService
                .sendMessage(newAgent.id, initialMessage)
                .pipe(untilDestroyed(this))
                .subscribe({
                  next: () => {
                    console.log(`Actor agent started for plan: ${decision.planId}`);
                  },
                  error: (error: unknown) => {
                    console.error("Error starting actor agent:", error);
                  },
                });
            })
            .catch(error => {
              console.error("Failed to create actor agent:", error);
            });
        } else {
          // If NOT creating new actor, send feedback and trigger execution on current agent
          const executionMessage = "I have accepted your action plan. Please proceed with executing it.";
          this.copilotManagerService
            .sendMessage(this.agentInfo.id, executionMessage)
            .pipe(untilDestroyed(this))
            .subscribe({
              error: (error: unknown) => {
                console.error("Error sending acceptance message:", error);
              },
            });
        }
      } else {
        // Extract feedback from rejection message
        const feedbackMatch = decision.message.match(/Feedback: (.+)$/);
        const userFeedback = feedbackMatch ? feedbackMatch[1] : "I don't want this action plan.";

        // Get the action plan to find operators to delete
        const actionPlan = this.actionPlanService.getActionPlan(decision.planId);
        if (actionPlan) {
          // Delete the created operators and links
          this.workflowActionService.deleteOperatorsAndLinks(actionPlan.operatorIds);
          console.log(`Deleted ${actionPlan.operatorIds.length} operators from rejected action plan`);
        }

        // Register plan rejection
        this.actionPlanService.rejectPlan(userFeedback, decision.planId);

        // Send rejection feedback to planner agent as a new message
        const rejectionMessage = `I have rejected your action plan. Feedback: ${userFeedback}`;
        this.copilotManagerService
          .sendMessage(this.agentInfo.id, rejectionMessage)
          .pipe(untilDestroyed(this))
          .subscribe({
            error: (error: unknown) => {
              console.error("Error sending rejection feedback:", error);
            },
          });
      }
    }
  }
}
