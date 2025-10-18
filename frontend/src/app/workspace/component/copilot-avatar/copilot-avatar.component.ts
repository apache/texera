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

import { Component, OnDestroy, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { TexeraCopilot, CopilotMessage } from "../../service/copilot/texera-copilot";

/**
 * CopilotAvatarComponent displays an AI assistant avatar in the menu bar
 * When clicked, it shows a floating chat interface for interacting with the copilot
 */
@UntilDestroy()
@Component({
  selector: "texera-copilot-avatar",
  templateUrl: "copilot-avatar.component.html",
  styleUrls: ["copilot-avatar.component.scss"],
})
export class CopilotAvatarComponent implements OnInit, OnDestroy {
  public isVisible: boolean = false;
  public isChatVisible: boolean = false;
  public isConnected: boolean = false;
  public isProcessing: boolean = false;
  public messages: CopilotMessage[] = [];
  public userInput: string = "";

  constructor(public copilotService: TexeraCopilot) {}

  ngOnInit(): void {
    // Subscribe to copilot state changes
    this.copilotService.state$.pipe(untilDestroyed(this)).subscribe(state => {
      this.isVisible = state.isEnabled;
      this.isConnected = state.isConnected;
      this.isProcessing = state.isProcessing;
      this.messages = state.messages;
    });
  }

  ngOnDestroy(): void {
    // Cleanup handled by @UntilDestroy()
  }

  /**
   * Toggle the chat box visibility
   */
  public toggleChat(): void {
    this.isChatVisible = !this.isChatVisible;
  }

  /**
   * Send a message to the copilot
   */
  public async sendMessage(): Promise<void> {
    if (!this.userInput.trim() || this.isProcessing) {
      return;
    }

    const message = this.userInput.trim();
    this.userInput = "";

    try {
      await this.copilotService.sendMessage(message);
    } catch (error) {
      console.error("Error sending message:", error);
    }
  }

  /**
   * Handle Enter key press in input field
   */
  public onKeyPress(event: KeyboardEvent): void {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      this.sendMessage();
    }
  }

  /**
   * Get the status indicator color
   */
  public getStatusColor(): string {
    if (!this.isConnected) {
      return "red";
    }
    if (this.isProcessing) {
      return "orange";
    }
    return "green";
  }

  /**
   * Get the status tooltip text
   */
  public getStatusTooltip(): string {
    if (!this.isConnected) {
      return "Disconnected";
    }
    if (this.isProcessing) {
      return "Processing...";
    }
    return "Connected";
  }
}
