// copilot-chat.component.ts
import { Component, OnDestroy, ViewChild, ElementRef } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { TexeraCopilot, AgentResponse, CopilotState } from "../../service/copilot/texera-copilot";
import { CopilotCoeditorService } from "../../service/copilot/copilot-coeditor.service";

@UntilDestroy()
@Component({
  selector: "texera-copilot-chat",
  templateUrl: "copilot-chat.component.html",
  styleUrls: ["copilot-chat.component.scss"],
})
export class CopilotChatComponent implements OnDestroy {
  @ViewChild("deepChat", { static: false }) deepChatElement?: ElementRef;

  public isChatVisible = false; // Whether chat panel is shown at all
  public isExpanded = true; // Whether chat content is expanded or minimized
  public showToolResults = false; // Whether to show tool call results
  public isConnected = false;
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
              if (response.type === "trace") {
                // Append tool trace message
                if (response.toolCalls && response.toolCalls.length > 0) {
                  const displayText = this.formatToolTrace(response);
                  if (displayText && this.deepChatElement?.nativeElement?.addMessage) {
                    this.deepChatElement.nativeElement.addMessage({ role: "ai", text: displayText });
                  }
                }
              } else if (response.type === "response") {
                if (response.isDone) {
                  // Final signal - clear loading indicator
                  signals.onResponse({ text: "" });
                } else {
                  // Append accumulated text as a new message
                  if (response.content && this.deepChatElement?.nativeElement?.addMessage) {
                    this.deepChatElement.nativeElement.addMessage({ role: "ai", text: response.content });
                  }
                }
              }
            },
            error: (e: unknown) => {
              signals.onResponse({ error: e ?? "Unknown error" });
            },
            complete: () => {
              // Observable complete
            },
          });
      },
    },
    demo: false,
    introMessage: { text: "Hi! I'm Texera Copilot. I can help you build and modify workflows." },
    textInput: { placeholder: { text: "Ask me anything about workflows..." } },
    requestBodyLimits: { maxMessages: -1 }, // Allow unlimited message history
  };

  /**
   * Format tool trace for display with markdown
   */
  private formatToolTrace(response: AgentResponse): string {
    if (!response.toolCalls || response.toolCalls.length === 0) {
      return "";
    }

    let output = "";

    // Format each tool call
    const traces = response.toolCalls.map((tc: any, index: number) => {
      // Handle tool call chunk (from onChunk) or full tool call (from onStepFinish)
      const toolName = tc.toolName || tc.name || "unknown";
      const args = tc.args || tc.arguments || {};

      // Format args nicely
      let argsDisplay = "";
      if (Object.keys(args).length > 0) {
        argsDisplay = "\n" + Object.entries(args)
          .map(([key, value]) => `  **${key}:** \`${JSON.stringify(value)}\``)
          .join("\n");
      }

      let toolTrace = `🔧 **${toolName}**${argsDisplay}`;

      // Add tool result if available and enabled
      if (this.showToolResults && response.toolResults && response.toolResults[index]) {
        const result = response.toolResults[index];
        const resultOutput = result.output || result.result || {};

        // Format result based on success/error
        if (resultOutput.success === false) {
          toolTrace += `\n❌ **Error:** ${resultOutput.error || "Unknown error"}`;
        } else if (resultOutput.success === true) {
          toolTrace += `\n✅ **Success:** ${resultOutput.message || "Operation completed"}`;
        } else {
          toolTrace += `\n**Result:** \`${JSON.stringify(resultOutput)}\``;
        }
      }

      return toolTrace;
    });

    output += traces.join("\n\n");

    // Add token usage if available
    if (response.usage) {
      const inputTokens = response.usage.inputTokens || 0;
      const outputTokens = response.usage.outputTokens || 0;
      output += `\n\n📊 **Tokens:** ${inputTokens} input, ${outputTokens} output`;
    }

    return output;
  }

  constructor(
    public copilotService: TexeraCopilot,
    private copilotCoeditorService: CopilotCoeditorService
  ) {}

  ngOnDestroy(): void {
    // Cleanup when component is destroyed
    this.disconnect();
  }

  /**
   * Connect to copilot - called from menu button
   * Registers copilot as coeditor and shows chat
   */
  public async connect(): Promise<void> {
    if (this.isInitialized) return;

    try {
      // Register copilot as virtual coeditor
      this.copilotCoeditorService.register();

      // Initialize copilot service
      await this.copilotService.initialize();

      this.isInitialized = true;
      this.isChatVisible = true; // Show chat panel on connect
      this.isExpanded = true; // Expand chat content by default
      this.updateConnectionStatus();
      console.log("Copilot connected and registered as coeditor");
    } catch (error) {
      console.error("Failed to connect copilot:", error);
      this.copilotCoeditorService.unregister();
    }
  }

  /**
   * Disconnect from copilot - called from menu button
   * Unregisters copilot and clears all messages
   */
  public disconnect(): void {
    if (!this.isInitialized) return;

    // Unregister copilot coeditor
    this.copilotCoeditorService.unregister();

    // Disconnect copilot service (this clears the connection)
    this.copilotService.disconnect();

    // Clear messages by resetting the message history
    // The copilot service will need to be re-initialized next time
    this.isInitialized = false;
    this.isChatVisible = false;
    this.updateConnectionStatus();
    console.log("Copilot disconnected and messages cleared");
  }

  /**
   * Check if copilot is currently connected
   */
  public isActive(): boolean {
    return this.isInitialized;
  }

  /**
   * Toggle expand/collapse of chat content (keeps header visible)
   */
  public toggleExpand(): void {
    this.isExpanded = !this.isExpanded;
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

    // Clear deep-chat UI messages
    if (this.deepChatElement?.nativeElement?.clearMessages) {
      this.deepChatElement.nativeElement.clearMessages(true);
    }
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
   * Get the copilot coeditor object (for displaying in UI)
   */
  public getCopilot() {
    return this.copilotCoeditorService.getCopilot();
  }

  private updateConnectionStatus(): void {
    this.isConnected = this.copilotService.isConnected();
  }
}
