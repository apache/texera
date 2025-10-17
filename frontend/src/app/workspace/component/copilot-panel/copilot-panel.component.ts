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

import { Component, OnInit, OnDestroy } from "@angular/core";
import { FormControl } from "@angular/forms";
import { Subject } from "rxjs";
import { takeUntil } from "rxjs/operators";
import { TexeraCopilot, CopilotState } from "../../service/copilot/texera-copilot";

@Component({
  selector: "texera-copilot-panel",
  templateUrl: "./copilot-panel.component.html",
  styleUrls: ["./copilot-panel.component.scss"],
})
export class CopilotPanelComponent implements OnInit, OnDestroy {
  public copilotState: CopilotState;
  public inputControl = new FormControl("");
  public showThinkingLog = false;
  public showQuickActions = false;

  private destroy$ = new Subject<void>();

  constructor(private copilot: TexeraCopilot) {
    this.copilotState = this.copilot.getState();
  }

  ngOnInit(): void {
    // Subscribe to copilot state changes
    this.copilot.state$.pipe(takeUntil(this.destroy$)).subscribe(state => {
      this.copilotState = state;
    });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  /**
   * Send message to copilot
   */
  async sendMessage(): Promise<void> {
    const message = this.inputControl.value?.trim();
    if (!message) return;

    // Clear input
    this.inputControl.setValue("");

    try {
      await this.copilot.sendMessage(message);
    } catch (error) {
      console.error("Error sending message:", error);
    }
  }

  /**
   * Toggle copilot
   */
  async toggleCopilot(): Promise<void> {
    try {
      await this.copilot.toggle();
    } catch (error) {
      console.error("Error toggling copilot:", error);
    }
  }

  /**
   * Clear conversation
   */
  clearConversation(): void {}

  /**
   * Toggle thinking log visibility
   */
  toggleThinkingLog(): void {
    this.showThinkingLog = !this.showThinkingLog;
  }

  /**
   * Toggle quick actions menu
   */
  toggleQuickActions(): void {
    this.showQuickActions = !this.showQuickActions;
  }

  /**
   * Quick action: Suggest workflow
   */
  async suggestWorkflow(): Promise<void> {
    const description = prompt("Describe the workflow you want to create:");
    if (description) {
      await this.copilot.suggestWorkflow(description);
    }
  }

  /**
   * Quick action: Analyze workflow
   */
  async analyzeWorkflow(): Promise<void> {
    await this.copilot.analyzeWorkflow();
  }

  /**
   * Quick action: List available operators
   */
  async listOperators(): Promise<void> {
    await this.copilot.sendMessage("List all available operator types grouped by category");
  }

  /**
   * Quick action: Auto-layout
   */
  async autoLayout(): Promise<void> {
    await this.copilot.sendMessage("Apply automatic layout to the workflow");
  }

  /**
   * Format timestamp for display
   */
  formatTimestamp(date: Date): string {
    return new Date(date).toLocaleTimeString();
  }

  /**
   * Format tool calls for display
   */
  formatToolCalls(toolCalls?: any[]): string {
    if (!toolCalls || toolCalls.length === 0) return "";

    return toolCalls.map(tc => `🔧 ${tc.function?.name || tc.name}`).join(", ");
  }
}
