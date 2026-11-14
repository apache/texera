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
 * A content part of an input message. The agent backend tags each part with a `type` and
 * fills only the fields for that kind, so every field but the tag is optional here and the
 * modal reads them by name. Tool-call arguments arrive under `args` or `input`; a tool
 * result arrives under `result`, `output`, or `content`.
 */
interface MessagePart {
  type: string;
  text?: string;
  toolName?: string;
  toolCallId?: string;
  args?: { operatorId?: string } & Record<string, unknown>;
  input?: { operatorId?: string } & Record<string, unknown>;
  result?: unknown;
  output?: unknown;
  content?: unknown;
}

/** An input message as this modal reads it: a role and either plain text or typed parts. */
interface InputMessage {
  role?: string;
  content?: string | MessagePart[];
}

/**
 * Reusable modal component for displaying ReActStep details.
 * Shows step identification, token usage, and tool calls.
 */
@Component({
  selector: "texera-react-step-detail-modal",
  templateUrl: "./react-step-detail-modal.component.html",
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
  public formatResult(data: unknown): string {
    if (typeof data === "string") {
      return data;
    }
    return JSON.stringify(data, null, 2);
  }

  public formatJson(data: unknown): string {
    return JSON.stringify(data, null, 2);
  }

  public getToolResult(step: ReActStep, toolCallIndex: number): unknown {
    if (!step.toolResults || toolCallIndex >= step.toolResults.length) {
      return null;
    }
    const toolResult = step.toolResults[toolCallIndex] as { output?: unknown; result?: unknown };
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

  // ============================================================================
  // Input Messages helpers
  // ============================================================================

  /**
   * Get text content from a message (user or assistant text parts).
   */
  public getTextFromMessage(msg: InputMessage | null): string {
    if (!msg?.content) return "";
    if (typeof msg.content === "string") return msg.content;
    if (Array.isArray(msg.content)) {
      return msg.content
        .filter(p => p.type === "text")
        .map(p => p.text || "")
        .join("\n");
    }
    return "";
  }

  /**
   * Get tool call summaries from an assistant message.
   * Returns array of { toolName, operatorId, fullArgs } for display.
   */
  public getToolCallSummaries(
    msg: InputMessage | null
  ): { toolName: string; operatorId: string; fullArgs: Record<string, unknown> }[] {
    if (!msg?.content || !Array.isArray(msg.content)) return [];
    return msg.content
      .filter(p => p.type === "tool-call")
      .map(p => {
        const args: { operatorId?: string } & Record<string, unknown> = p.args || p.input || {};
        return {
          toolName: p.toolName ?? "",
          operatorId: args.operatorId || "",
          fullArgs: args,
        };
      });
  }

  /**
   * Build a toolCallId → toolName map from all input messages.
   */
  private buildToolCallNameMap(messages: InputMessage[]): Map<string, string> {
    const map = new Map<string, string>();
    for (const msg of messages) {
      if (msg.role === "assistant" && Array.isArray(msg.content)) {
        for (const part of msg.content) {
          if (part.type === "tool-call" && part.toolCallId && part.toolName) {
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
    msg: InputMessage | null
  ): { toolName: string; resultContent: string; tokenCount: number; isTrimmed: boolean }[] {
    if (!msg?.content || !Array.isArray(msg.content)) return [];
    const nameMap = this.step?.inputMessages ? this.buildToolCallNameMap(this.step.inputMessages) : new Map();
    return msg.content
      .filter(p => p.type === "tool-result")
      .map(p => {
        const raw = p.result ?? p.output ?? p.content ?? "";
        const resultStr = typeof raw === "string" ? raw : JSON.stringify(raw, null, 2);
        return {
          toolName: nameMap.get(p.toolCallId ?? "") || p.toolCallId || "",
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
  public getToolResultItems(msg: InputMessage | null): { toolName: string; tokenCount: number; isTrimmed: boolean }[] {
    if (!msg?.content || !Array.isArray(msg.content)) return [];
    const nameMap = this.step?.inputMessages ? this.buildToolCallNameMap(this.step.inputMessages) : new Map();
    return msg.content
      .filter(p => p.type === "tool-result")
      .map(p => {
        const raw = p.result ?? p.output ?? p.content ?? "";
        const resultStr = typeof raw === "string" ? raw : JSON.stringify(raw);
        return {
          toolName: nameMap.get(p.toolCallId ?? "") || p.toolCallId || "",
          tokenCount: Math.ceil(resultStr.length / 4),
          isTrimmed: resultStr.includes("context compaction"),
        };
      });
  }
}
