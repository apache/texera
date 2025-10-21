// copilot-chat.component.ts
import { Component, OnInit, OnDestroy, ViewChild, ElementRef } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { TexeraCopilot, AgentResponse } from "../../service/copilot/texera-copilot";
import { CopilotCoeditorService } from "../../service/copilot/copilot-coeditor.service";

@UntilDestroy()
@Component({
  selector: "texera-copilot-chat",
  templateUrl: "copilot-chat.component.html",
  styleUrls: ["copilot-chat.component.scss"],
})
export class CopilotChatComponent implements OnInit, OnDestroy {
  @ViewChild("deepChat", { static: false }) deepChatElement?: ElementRef;

  public isChatVisible = false; // Whether chat panel is shown at all
  public isExpanded = true; // Whether chat content is expanded or minimized
  public showToolResults = false; // Whether to show tool call results
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
   * Format tool trace for display with markdown
   */
  private formatToolTrace(response: AgentResponse): string {
    if (!response.toolCalls || response.toolCalls.length === 0) {
      return "";
    }

    // Include agent's thinking/text if available
    let output = "";
    if (response.content && response.content.trim()) {
      output += `💭 **Agent:** ${response.content}\n\n`;
    }

    // Format each tool call - show tool name, parameters, and optionally results
    const traces = response.toolCalls.map((tc: any, index: number) => {
      // Log the actual structure to debug
      console.log("Tool call structure:", tc);

      // Try multiple possible property names for arguments
      const args = tc.args || tc.arguments || tc.parameters || tc.input || {};

      // Format args nicely
      let argsDisplay = "";
      if (Object.keys(args).length > 0) {
        argsDisplay = Object.entries(args)
          .map(([key, value]) => `  **${key}:** \`${JSON.stringify(value)}\``)
          .join("\n");
      } else {
        argsDisplay = "  *(no parameters)*";
      }

      let toolTrace = `🔧 **${tc.toolName}**\n${argsDisplay}`;

      // Add tool result if showToolResults is enabled
      if (this.showToolResults && response.toolResults && response.toolResults[index]) {
        const result = response.toolResults[index];
        const resultOutput = result.output || result.result || {};

        // Format result based on success/error
        if (resultOutput.success === false) {
          toolTrace += `\n  ❌ **Error:** ${resultOutput.error || "Unknown error"}`;
        } else if (resultOutput.success === true) {
          toolTrace += `\n  ✅ **Success:** ${resultOutput.message || "Operation completed"}`;
          // Include additional result details if present
          const details = Object.entries(resultOutput)
            .filter(([key]) => key !== "success" && key !== "message")
            .map(([key, value]) => `  **${key}:** \`${JSON.stringify(value)}\``)
            .join("\n");
          if (details) {
            toolTrace += `\n${details}`;
          }
        } else {
          // Show raw result if format is unexpected
          toolTrace += `\n  **Result:** \`${JSON.stringify(resultOutput)}\``;
        }
      }

      return toolTrace;
    });

    output += traces.join("\n\n");

    // Add token usage information if available
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

  ngOnInit(): void {
    // Component initialization - copilot connection is triggered by menu button
  }

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
   * Get the copilot coeditor object (for displaying in UI)
   */
  public getCopilot() {
    return this.copilotCoeditorService.getCopilot();
  }

  private updateConnectionStatus(): void {
    this.isConnected = this.copilotService.isConnected();
  }
}
