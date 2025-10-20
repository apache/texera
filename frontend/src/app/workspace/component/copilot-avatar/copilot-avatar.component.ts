// copilot-avatar.component.ts
import { Component, OnInit, ViewChild, ElementRef } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { TexeraCopilot, AgentResponse } from "../../service/copilot/texera-copilot";

@UntilDestroy()
@Component({
  selector: "texera-copilot-avatar",
  templateUrl: "copilot-avatar.component.html",
  styleUrls: ["copilot-avatar.component.scss"],
})
export class CopilotAvatarComponent implements OnInit {
  @ViewChild("deepChat", { static: false }) deepChatElement?: ElementRef;

  public isVisible = true;
  public isChatVisible = false;
  public isConnected = false;
  public isProcessing = false;
  private isInitialized = false;

  // Deep-chat configuration
  public deepChatConfig = {
    connect: {
      handler: (body: any, signals: any) => {
        const last = body?.messages?.[body.messages.length - 1];
        const userText: string = typeof last?.text === "string" ? last.text : "";

        // Send message to copilot and process AgentResponse
        this.copilotService
          .sendMessage(userText)
          .pipe(untilDestroyed(this))
          .subscribe({
            next: (response: AgentResponse) => {
              // Format the response based on type
              let displayText = "";

              if (response.type === "trace") {
                // Format tool traces
                displayText = this.formatToolTrace(response);
                // Add trace message via addMessage API
                if (displayText && this.deepChatElement?.nativeElement?.addMessage) {
                  this.deepChatElement.nativeElement.addMessage({ role: "ai", text: displayText });
                }
              } else if (response.type === "response") {
                // For final response, signal completion with the content
                // This will let deep-chat handle adding the message
                if (response.isDone) {
                  signals.onResponse({ text: response.content });
                }
              }
            },
            error: (e: unknown) => {
              signals.onResponse({ error: e ?? "Unknown error" });
            },
          });
      },
    },
    demo: false,
    introMessage: { text: "Hi! I'm Texera Copilot. I can help you build and modify workflows." },
    textInput: { placeholder: { text: "Ask me anything about workflows..." } },
  };

  /**
   * Format tool trace for display
   */
  private formatToolTrace(response: AgentResponse): string {
    if (!response.toolCalls || response.toolCalls.length === 0) {
      return "";
    }

    const traces = response.toolCalls.map(tc => {
      const result = response.toolResults?.find(tr => tr.toolCallId === tc.toolCallId);
      return `🔧 ${tc.toolName}(${JSON.stringify(tc.args, null, 2)})\n→ ${JSON.stringify(result?.result, null, 2)}`;
    });

    return `[Tool Trace]\n${traces.join("\n\n")}`;
  }

  constructor(public copilotService: TexeraCopilot) {}

  ngOnInit(): void {
    // Update connection status
    this.updateConnectionStatus();
  }

  public async toggleChat(): Promise<void> {
    // Initialize copilot on first toggle if not already initialized
    if (!this.isInitialized) {
      try {
        await this.copilotService.initialize();
        this.isInitialized = true;
        this.updateConnectionStatus();
      } catch (error) {
        console.error("Failed to initialize copilot:", error);
        return;
      }
    }

    this.isChatVisible = !this.isChatVisible;
  }

  public toggleVisibility(): void {
    this.isVisible = !this.isVisible;

    // If hiding and copilot is initialized, disconnect
    if (!this.isVisible && this.isInitialized) {
      this.copilotService.disconnect().then(() => {
        this.isInitialized = false;
        this.updateConnectionStatus();
      });
    }
  }

  private updateConnectionStatus(): void {
    this.isConnected = this.copilotService.isConnected();
  }

  public getStatusColor(): string {
    if (!this.isConnected) return "red";
    if (this.isProcessing) return "orange";
    return "green";
  }
  public getStatusTooltip(): string {
    if (!this.isConnected) return "Disconnected";
    if (this.isProcessing) return "Processing...";
    return "Connected";
  }
}
