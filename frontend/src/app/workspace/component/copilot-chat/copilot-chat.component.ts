// copilot-chat.component.ts
import { Component, OnDestroy, AfterViewInit, ViewChild, ElementRef } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { TexeraCopilot, AgentResponse, CopilotState } from "../../service/copilot/texera-copilot";
import { CopilotCoeditorService } from "../../service/copilot/copilot-coeditor.service";

@UntilDestroy()
@Component({
  selector: "texera-copilot-chat",
  templateUrl: "copilot-chat.component.html",
  styleUrls: ["copilot-chat.component.scss"],
})
export class CopilotChatComponent implements OnDestroy, AfterViewInit {
  @ViewChild("deepChat", { static: false }) deepChatElement?: ElementRef;

  public isChatVisible = false; // Whether chat panel is shown at all
  public isExpanded = true; // Whether chat content is expanded or minimized
  public showToolResults = false; // Whether to show tool call results
  public isConnected = false;
  private isInitialized = false;
  public selectedMessageIndex: number | null = null; // Track which message is selected

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
                // Format tool traces
                const displayText = this.formatToolTrace(response);

                // Add trace message via addMessage API
                if (displayText && this.deepChatElement?.nativeElement?.addMessage) {
                  this.deepChatElement.nativeElement.addMessage({ role: "ai", text: displayText });
                }

                // Keep processing state true - loading indicator stays visible
              } else if (response.type === "response") {
                // For final response, signal completion with the content
                // This will let deep-chat handle adding the message and clearing loading
                if (response.isDone) {
                  signals.onResponse({ text: response.content });
                }
              }
            },
            error: (e: unknown) => {
              signals.onResponse({ error: e ?? "Unknown error" });
            },
            complete: () => {
              // Handle completion without final response (happens when generation is stopped)
              const currentState = this.copilotService.getState();
              if (currentState === CopilotState.STOPPING) {
                // Generation was stopped by user - show completion message
                signals.onResponse({ text: "_Generation stopped._" });
              } else if (currentState === CopilotState.GENERATING) {
                // Generation completed unexpectedly
                signals.onResponse({ text: "_Generation completed._" });
              }
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

  ngAfterViewInit(): void {
    // Click listeners will be set up in connect() method when chat becomes visible
  }

  /**
   * Set up click listeners for agent messages
   */
  private setupMessageClickListeners(): void {
    const deepChatElement = this.deepChatElement?.nativeElement;

    console.log("Setting up message click listeners...");
    console.log("deepChatElement:", deepChatElement);

    if (!deepChatElement) {
      console.error("Deep chat element not found!");
      return;
    }

    // Deep-chat is a web component that uses Shadow DOM
    // We need to access its shadow root to interact with internal elements
    const shadowRoot = deepChatElement.shadowRoot;

    console.log("Shadow root:", shadowRoot);

    if (shadowRoot) {
      // Attach click listener to the shadow root
      shadowRoot.addEventListener("click", (event: MouseEvent) => {
        this.handleDeepChatClick(event);
      });
      console.log("Click listener attached to shadow root");
    } else {
      // Fallback: try attaching to the element itself
      deepChatElement.addEventListener("click", (event: MouseEvent) => {
        this.handleDeepChatClick(event);
      });
      console.log("Click listener attached to deep-chat element (no shadow root)");
    }

    // Also set up a MutationObserver to handle dynamically added messages
    const targetNode = shadowRoot || deepChatElement;
    const observer = new MutationObserver(() => {
      console.log("DOM mutation detected in deep-chat");
    });

    observer.observe(targetNode, {
      childList: true,
      subtree: true,
    });
  }

  /**
   * Handle clicks within the deep-chat component
   */
  private handleDeepChatClick(event: MouseEvent): void {
    const target = event.target as HTMLElement;

    // Log the clicked element for debugging
    console.log("Clicked element:", target);
    console.log("Element classes:", target.className);
    console.log("Element tag:", target.tagName);

    // Try to find the message bubble by traversing up the DOM tree
    let messageElement = this.findMessageBubble(target);

    if (messageElement) {
      // Check if it's an AI message (not a user message)
      const isAiMessage = this.isAiMessage(messageElement);

      console.log("Found message element, is AI:", isAiMessage);

      if (isAiMessage) {
        this.onMessageClick(messageElement);
      }
    }
  }

  /**
   * Find the message bubble element by traversing up the DOM
   */
  private findMessageBubble(element: HTMLElement): HTMLElement | null {
    let current: HTMLElement | null = element;
    let depth = 0;
    const maxDepth = 10; // Prevent infinite loops

    while (current && depth < maxDepth) {
      // Log each level for debugging
      console.log(`Level ${depth}:`, current.className, current.tagName);

      // Check if this element looks like a message bubble
      // Common patterns: message-bubble, message, bubble, etc.
      const className = current.className || "";
      if (
        className.includes("message") ||
        className.includes("bubble") ||
        current.hasAttribute("data-message") ||
        current.classList?.contains("message")
      ) {
        console.log("Found message bubble:", current);
        return current;
      }

      current = current.parentElement;
      depth++;
    }

    return null;
  }

  /**
   * Check if a message element is from the AI
   */
  private isAiMessage(element: HTMLElement): boolean {
    const className = element.className || "";
    const parent = element.parentElement;
    const parentClass = parent?.className || "";

    // Check various patterns that might indicate an AI message
    return (
      className.includes("ai") ||
      className.includes("assistant") ||
      className.includes("bot") ||
      parentClass.includes("ai") ||
      parentClass.includes("assistant") ||
      parentClass.includes("bot") ||
      (element.hasAttribute("role") && element.getAttribute("role") === "ai")
    );
  }

  /**
   * Handle message click event
   */
  private onMessageClick(messageElement: HTMLElement): void {
    // Remove 'selected' class from all previously selected messages
    const deepChat = this.deepChatElement?.nativeElement;
    if (deepChat) {
      const allElements = deepChat.querySelectorAll(".selected");
      allElements.forEach((el: Element) => el.classList.remove("selected"));
    }

    // Add 'selected' class to clicked message
    messageElement.classList.add("selected");

    // Update selected message index (use a timestamp or unique ID)
    this.selectedMessageIndex = Date.now();

    console.log("Agent message selected!");
  }

  /**
   * Close message action buttons
   */
  public closeMessageActions(): void {
    this.selectedMessageIndex = null;

    // Remove 'selected' class from all messages
    const allMessages = this.deepChatElement?.nativeElement.querySelectorAll(".ai-message, [class*=\"ai\"], [role=\"ai\"]");
    allMessages?.forEach((msg: Element) => msg.classList.remove("selected"));
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

      // Set up click listeners after the chat panel is rendered
      setTimeout(() => {
        this.setupMessageClickListeners();
      }, 1000);
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
