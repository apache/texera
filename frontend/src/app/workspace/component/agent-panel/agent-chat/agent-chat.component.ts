// agent-chat.component.ts
import { Component, OnDestroy, ViewChild, ElementRef, Input, OnInit, AfterViewChecked } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { TexeraCopilot, AgentResponse, CopilotState } from "../../../service/copilot/texera-copilot";
import { AgentInfo } from "../../../service/copilot/texera-copilot-manager.service";
import { DomSanitizer, SafeHtml } from "@angular/platform-browser";

interface ChatMessage {
  role: "user" | "ai";
  text: string;
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

  public showToolResults = false;
  public messages: ChatMessage[] = [];
  public currentMessage = "";
  private copilotService!: TexeraCopilot;
  private shouldScrollToBottom = false;

  constructor(private sanitizer: DomSanitizer) {}

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

    // Add initial greeting message
    this.messages.push({
      role: "ai",
      text: `Hi! I'm ${this.agentInfo.name}. I can help you build and modify workflows.`,
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
  public formatMessageContent(text: string): SafeHtml {
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

    // Add user message to chat
    this.messages.push({
      role: "user",
      text: userMessage,
    });
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
              this.messages.push({
                role: "ai",
                text: traceText,
              });
              this.shouldScrollToBottom = true;
            }
          } else if (response.type === "response") {
            currentAiMessage = response.content;
            if (response.isDone && currentAiMessage) {
              this.messages.push({
                role: "ai",
                text: currentAiMessage,
              });
              this.shouldScrollToBottom = true;
            }
          }
        },
        error: (error: unknown) => {
          console.error("Error sending message:", error);
          this.messages.push({
            role: "ai",
            text: `Error: ${error || "Unknown error occurred"}`,
          });
          this.shouldScrollToBottom = true;
        },
        complete: () => {
          const currentState = this.copilotService.getState();
          if (currentState === CopilotState.STOPPING) {
            this.messages.push({
              role: "ai",
              text: "_Generation stopped._",
            });
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
    // Add greeting message back
    this.messages.push({
      role: "ai",
      text: "Hi! I'm Texera Agent. I can help you build and modify workflows.",
    });
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
}
