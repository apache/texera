// agent-chat.component.ts
import { Component, OnDestroy, ViewChild, ElementRef, Input, OnInit, AfterViewChecked } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { TexeraCopilot, AgentResponse, CopilotState } from "../../../service/copilot/texera-copilot";
import { AgentInfo } from "../../../service/copilot/texera-copilot-manager.service";
import { DomSanitizer, SafeHtml } from "@angular/platform-browser";
import { ActionPlan, ActionPlanService } from "../../../service/action-plan/action-plan.service";
import { ModelMessage } from "ai";

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

  public showToolResults = false;
  public messages: ModelMessage[] = [];
  public currentMessage = "";
  public pendingActionPlan: ActionPlan | null = null;
  private copilotService!: TexeraCopilot;
  private shouldScrollToBottom = false;

  constructor(
    private sanitizer: DomSanitizer,
    private actionPlanService: ActionPlanService
  ) {}

  ngOnInit(): void {
    console.log("AgentChatComponent ngOnInit - agentInfo:", this.agentInfo);

    if (!this.agentInfo) {
      console.error("AgentInfo is not provided!");
      return;
    }

    this.copilotService = this.agentInfo.instance;

    if (!this.copilotService) {
      console.error("CopilotService instance is not available!");
      return;
    }

    // Load existing message history or add initial greeting
    if (this.agentInfo.messageHistory && this.agentInfo.messageHistory.length > 0) {
      // Restore existing messages
      this.messages = [...this.agentInfo.messageHistory];
      this.shouldScrollToBottom = true;
    } else {
      // Add initial greeting message for new agent
      const greeting: ModelMessage = {
        role: "assistant",
        content: `Hi! I'm ${this.agentInfo.name}. I can help you build and modify workflows.`,
      };
      this.messages.push(greeting);
      // Save the greeting to the persistent history
      this.agentInfo.messageHistory.push(greeting);
    }

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

  ngOnDestroy(): void {
    // Cleanup when component is destroyed
  }

  /**
   * Format message content to display markdown-like text
   */
  public formatMessageContent(content: string | any[]): SafeHtml {
    // Handle array content (from ModelMessage)
    let text: string;
    if (Array.isArray(content)) {
      // Extract text from content array (handle text parts)
      text = content
        .filter((part: any) => part.type === "text" || typeof part === "string")
        .map((part: any) => (typeof part === "string" ? part : part.text || ""))
        .join(" ");
    } else {
      text = content || "";
    }

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
   * Send a message to the agent
   */
  public sendMessage(): void {
    if (!this.currentMessage.trim() || this.isGenerating()) {
      return;
    }

    const userMessage = this.currentMessage.trim();
    this.currentMessage = "";

    // Add user message to chat and persistent history
    const userMsg: ModelMessage = {
      role: "user",
      content: userMessage,
    };
    this.messages.push(userMsg);
    this.agentInfo.messageHistory.push(userMsg);
    this.shouldScrollToBottom = true;

    // Send to copilot
    let currentAiMessage = "";
    this.copilotService
      .sendMessage(userMessage)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: (response: AgentResponse) => {
          if (response.type === "trace") {
            // Format and add trace message
            const traceText = this.formatToolTrace(response);
            if (traceText) {
              const traceMsg: ModelMessage = {
                role: "assistant",
                content: traceText,
              };
              this.messages.push(traceMsg);
              this.agentInfo.messageHistory.push(traceMsg);
              this.shouldScrollToBottom = true;
            }
          } else if (response.type === "response") {
            currentAiMessage = response.content;
            if (response.isDone && currentAiMessage) {
              const aiMsg: ModelMessage = {
                role: "assistant",
                content: currentAiMessage,
              };
              this.messages.push(aiMsg);
              this.agentInfo.messageHistory.push(aiMsg);
              this.shouldScrollToBottom = true;
            }
          }
        },
        error: (error: unknown) => {
          console.error("Error sending message:", error);
          const errorMsg: ModelMessage = {
            role: "assistant",
            content: `Error: ${error || "Unknown error occurred"}`,
          };
          this.messages.push(errorMsg);
          this.agentInfo.messageHistory.push(errorMsg);
          this.shouldScrollToBottom = true;
        },
        complete: () => {
          const currentState = this.copilotService.getState();
          if (currentState === CopilotState.STOPPING) {
            const stoppedMsg: ModelMessage = {
              role: "assistant",
              content: "_Generation stopped._",
            };
            this.messages.push(stoppedMsg);
            this.agentInfo.messageHistory.push(stoppedMsg);
            this.shouldScrollToBottom = true;
          }
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
   * Format tool trace for display
   */
  private formatToolTrace(response: AgentResponse): string {
    if (!response.toolCalls || response.toolCalls.length === 0) {
      return "";
    }

    let output = "";
    if (response.content && response.content.trim()) {
      output += `💭 **Agent:** ${response.content}\n\n`;
    }

    const traces = response.toolCalls.map((tc: any, index: number) => {
      const args = tc.args || tc.arguments || tc.parameters || tc.input || {};
      let argsDisplay = "";
      if (Object.keys(args).length > 0) {
        argsDisplay = Object.entries(args)
          .map(([key, value]) => `  **${key}:** \`${JSON.stringify(value)}\``)
          .join("\n");
      } else {
        argsDisplay = "  *(no parameters)*";
      }

      let toolTrace = `🔧 **${tc.toolName}**\n${argsDisplay}`;

      if (this.showToolResults && response.toolResults && response.toolResults[index]) {
        const result = response.toolResults[index];
        const resultOutput = result.output || result.result || {};

        if (resultOutput.success === false) {
          toolTrace += `\n  ❌ **Error:** ${resultOutput.error || "Unknown error"}`;
        } else if (resultOutput.success === true) {
          toolTrace += `\n  ✅ **Success:** ${resultOutput.message || "Operation completed"}`;
        } else {
          toolTrace += `\n  **Result:** \`${JSON.stringify(resultOutput)}\``;
        }
      }

      return toolTrace;
    });

    output += traces.join("\n\n");
    return output;
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
    this.copilotService.stopGeneration();
  }

  /**
   * Clear message history
   */
  public clearMessages(): void {
    this.copilotService.clearMessages();
    this.messages = [];
    this.agentInfo.messageHistory = []; // Clear persistent history
    // Add greeting message back
    const greeting: ModelMessage = {
      role: "assistant",
      content: `Hi! I'm ${this.agentInfo.name}. I can help you build and modify workflows.`,
    };
    this.messages.push(greeting);
    this.agentInfo.messageHistory.push(greeting);
  }

  /**
   * Check if copilot is currently generating
   */
  public isGenerating(): boolean {
    return this.copilotService.getState() === CopilotState.GENERATING;
  }

  /**
   * Check if copilot is currently stopping
   */
  public isStopping(): boolean {
    return this.copilotService.getState() === CopilotState.STOPPING;
  }

  /**
   * Check if copilot is available (can send messages)
   */
  public isAvailable(): boolean {
    return this.copilotService.getState() === CopilotState.AVAILABLE;
  }

  /**
   * Check if agent is connected
   */
  public isConnected(): boolean {
    return this.copilotService?.isConnected() ?? false;
  }

  /**
   * Handle user decision on action plan
   */
  public onUserDecision(decision: { accepted: boolean; message: string }): void {
    // Add the user's decision as a user message in the chat
    const decisionMsg: ModelMessage = {
      role: "user",
      content: decision.message,
    };
    this.messages.push(decisionMsg);
    this.agentInfo.messageHistory.push(decisionMsg);
    this.shouldScrollToBottom = true;

    // Clear the pending action plan since user has made a decision
    this.pendingActionPlan = null;
  }
}
