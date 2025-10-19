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

  // Deep-chat configuration
  public deepChatConfig = {
    connect: {
      handler: async (body: any, signals: any) => {
        try {
          const last = body?.messages?.[body.messages.length - 1];
          const userText: string = typeof last?.text === "string" ? last.text : "";
          const reply = await this.copilotService.sendMessage(userText);
          signals.onResponse({ text: reply });
        } catch (e: any) {
          signals.onResponse({ error: e?.message ?? "Unknown error" });
        }
      },
    },
    demo: false,
    introMessage: { text: "Hi! I'm Texera Copilot. I can help you build and modify workflows." },
    textInput: { placeholder: { text: "Ask me anything about workflows..." } },
  };

  constructor(public copilotService: TexeraCopilot) {}

  ngOnInit(): void {
    this.copilotService.state$.pipe(untilDestroyed(this)).subscribe(state => {
      this.isVisible = state.isEnabled;
      this.isConnected = state.isConnected;
      this.isProcessing = state.isProcessing;
    });
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

  public toggleChat(): void {
    this.isChatVisible = !this.isChatVisible;
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
