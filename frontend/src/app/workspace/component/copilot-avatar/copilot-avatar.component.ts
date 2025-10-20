// copilot-avatar.component.ts
import { Component, OnInit, ViewChild, ElementRef, AfterViewInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { TexeraCopilot } from "../../service/copilot/texera-copilot";
import type { ModelMessage } from "ai";

@UntilDestroy()
@Component({
  selector: "texera-copilot-avatar",
  templateUrl: "copilot-avatar.component.html",
  styleUrls: ["copilot-avatar.component.scss"],
})
export class CopilotAvatarComponent implements OnInit, AfterViewInit {
  @ViewChild("deepChat", { static: false }) deepChatElement?: ElementRef;

  public isVisible = false;
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

        this.copilotService
          .sendMessage(userText)
          .pipe(untilDestroyed(this))
          .subscribe({
            next: (reply: string) => {
              signals.onResponse({ text: reply });
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

  constructor(public copilotService: TexeraCopilot) {}

  ngOnInit(): void {
    // Subscribe to message stream for real-time updates (including tool traces)
    this.copilotService.messages$.pipe(untilDestroyed(this)).subscribe(message => {
      if (this.deepChatElement?.nativeElement) {
        const newMessage = {
          role: message.role === "assistant" ? "ai" : "user",
          text: typeof message.content === "string" ? message.content : JSON.stringify(message.content),
        };

        // Add message to deep-chat
        const currentMessages = this.deepChatElement.nativeElement.messages || [];
        this.deepChatElement.nativeElement.messages = [...currentMessages, newMessage];
      }
    });

    // Update connection status
    this.updateConnectionStatus();
  }

  ngAfterViewInit(): void {
    // Load existing messages if any
    if (this.deepChatElement?.nativeElement) {
      const existing = this.copilotService.getMessages();
      if (existing.length > 0) {
        this.deepChatElement.nativeElement.messages = existing.map((m: ModelMessage) => ({
          role: m.role === "assistant" ? "ai" : "user",
          text: typeof m.content === "string" ? m.content : JSON.stringify(m.content),
        }));
      }
    }
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
