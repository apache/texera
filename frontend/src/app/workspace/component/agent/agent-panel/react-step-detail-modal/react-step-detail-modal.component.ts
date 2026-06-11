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

import { Component, Input, Output, EventEmitter } from "@angular/core";
import { ReActStep } from "../../../../service/agent/agent-types";
import { NzModalComponent, NzModalContentDirective } from "ng-zorro-antd/modal";
import { NgIf, NgFor, SlicePipe, DatePipe } from "@angular/common";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzDescriptionsComponent, NzDescriptionsItemComponent } from "ng-zorro-antd/descriptions";
import { NzTagComponent } from "ng-zorro-antd/tag";
import { NzCollapseComponent, NzCollapsePanelComponent } from "ng-zorro-antd/collapse";

/**
 * Reusable modal component for displaying ReActStep details.
 * Shows step identification, token usage, and tool calls.
 */
@Component({
  selector: "texera-react-step-detail-modal",
  templateUrl: "./react-step-detail-modal.component.html",
  styleUrls: ["./react-step-detail-modal.component.scss"],
  imports: [
    NzModalComponent,
    NzModalContentDirective,
    NgIf,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzDescriptionsComponent,
    NzDescriptionsItemComponent,
    NzTagComponent,
    NzCollapseComponent,
    NgFor,
    NzCollapsePanelComponent,
    SlicePipe,
    DatePipe,
  ],
})
export class ReActStepDetailModalComponent {
  @Input() visible: boolean = false;
  @Input() step: ReActStep | null = null;
  @Input() agentId: string | null = null;
  @Output() visibleChange = new EventEmitter<boolean>();

  public closeModal(): void {
    this.visible = false;
    this.visibleChange.emit(false);
  }

  /**
   * Format data for display.
   * If the data is a string, return it as-is (with newlines preserved).
   * If it's an object, JSON.stringify it with formatting.
   */
  public formatResult(data: any): string {
    if (typeof data === "string") {
      return data;
    }
    return JSON.stringify(data, null, 2);
  }

  public formatJson(data: any): string {
    return JSON.stringify(data, null, 2);
  }

  public getToolResult(step: ReActStep, toolCallIndex: number): any {
    if (!step.toolResults || toolCallIndex >= step.toolResults.length) {
      return null;
    }
    const toolResult = step.toolResults[toolCallIndex];
    return toolResult.output || toolResult.result || toolResult;
  }

  public getToolOperatorAccess(
    step: ReActStep,
    toolCallIndex: number
  ): { viewedOperatorIds: string[]; addedOperatorIds: string[]; modifiedOperatorIds: string[] } | null {
    if (!step.operatorAccess) {
      return null;
    }
    return step.operatorAccess.get(toolCallIndex) || null;
  }

  public hasOperatorAccess(step: ReActStep): boolean {
    return !!step.operatorAccess && step.operatorAccess.size > 0;
  }

  /**
   * Get tag color for a message role.
   */
  public getMessageRoleColor(role: string): string {
    switch (role) {
      case "user":
        return "blue";
      case "assistant":
        return "orange";
      case "tool":
        return "green";
      default:
        return "default";
    }
  }

  // ============================================================================
  // Input Messages helpers
  // ============================================================================

  /**
   * Get text content from a message (user or assistant text parts).
   */
  public getTextFromMessage(msg: any): string {
    if (!msg?.content) return "";
    if (typeof msg.content === "string") return msg.content;
    if (Array.isArray(msg.content)) {
      return msg.content
        .filter((p: any) => p.type === "text")
        .map((p: any) => p.text || "")
        .join("\n");
    }
    return "";
  }

  /**
   * Get tool call summaries from an assistant message.
   * Returns array of { toolName, operatorId, fullArgs } for display.
   */
  public getToolCallSummaries(msg: any): { toolName: string; operatorId: string; fullArgs: any }[] {
    if (!msg?.content || !Array.isArray(msg.content)) return [];
    return msg.content
      .filter((p: any) => p.type === "tool-call")
      .map((p: any) => {
        const args = p.args || p.input || {};
        return {
          toolName: p.toolName,
          operatorId: args.operatorId || "",
          fullArgs: args,
        };
      });
  }

  /**
   * Get tool calls from an assistant message, formatted as function-call strings.
   */
  public getToolCallStrings(msg: any): string[] {
    if (!msg?.content || !Array.isArray(msg.content)) return [];
    return msg.content.filter((p: any) => p.type === "tool-call").map((p: any) => this.formatAsFunction(p));
  }

  /**
   * Format a tool-call part as function-call notation: toolName(key=val, key=val)
   */
  private formatAsFunction(part: any): string {
    const args = part.args || part.input || {};
    const params = Object.entries(args)
      .map(([k, v]) => {
        let val: string;
        if (typeof v === "string") {
          val = v.length > 60 ? `"${v.substring(0, 60)}..."` : `"${v}"`;
        } else {
          const s = JSON.stringify(v);
          val = s.length > 60 ? s.substring(0, 60) + "..." : s;
        }
        return `${k}=${val}`;
      })
      .join(", ");
    return `${part.toolName}(${params})`;
  }

  /**
   * Build a toolCallId → toolName map from all input messages.
   */
  private buildToolCallNameMap(messages: any[]): Map<string, string> {
    const map = new Map<string, string>();
    for (const msg of messages) {
      if (msg.role === "assistant" && Array.isArray(msg.content)) {
        for (const part of msg.content) {
          if (part.type === "tool-call") {
            map.set(part.toolCallId, part.toolName);
          }
        }
      }
    }
    return map;
  }

  /**
   * Get full tool result content items for expanded view.
   * Each item includes: toolName, resultContent string, approximate token count, and whether it was trimmed.
   */
  public getToolResultFullItems(
    msg: any
  ): { toolName: string; resultContent: string; tokenCount: number; isTrimmed: boolean }[] {
    if (!msg?.content || !Array.isArray(msg.content)) return [];
    const nameMap = this.step?.inputMessages ? this.buildToolCallNameMap(this.step.inputMessages) : new Map();
    return msg.content
      .filter((p: any) => p.type === "tool-result")
      .map((p: any) => {
        const raw = p.result ?? p.output ?? p.content ?? "";
        const resultStr = typeof raw === "string" ? raw : JSON.stringify(raw, null, 2);
        return {
          toolName: nameMap.get(p.toolCallId) || p.toolCallId,
          resultContent: resultStr,
          tokenCount: Math.ceil(resultStr.length / 4),
          isTrimmed: resultStr.includes("context compaction"),
        };
      });
  }

  /**
   * Get structured tool results from a tool message.
   * Each result includes: toolName, approximate token count, and whether it was trimmed.
   */
  public getToolResultItems(msg: any): { toolName: string; tokenCount: number; isTrimmed: boolean }[] {
    if (!msg?.content || !Array.isArray(msg.content)) return [];
    const nameMap = this.step?.inputMessages ? this.buildToolCallNameMap(this.step.inputMessages) : new Map();
    return msg.content
      .filter((p: any) => p.type === "tool-result")
      .map((p: any) => {
        const raw = p.result ?? p.output ?? p.content ?? "";
        const resultStr = typeof raw === "string" ? raw : JSON.stringify(raw);
        return {
          toolName: nameMap.get(p.toolCallId) || p.toolCallId,
          tokenCount: Math.ceil(resultStr.length / 4),
          isTrimmed: resultStr.includes("context compaction"),
        };
      });
  }
}
