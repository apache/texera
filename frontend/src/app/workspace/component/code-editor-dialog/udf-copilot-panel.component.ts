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

import { Component, EventEmitter, Input, Output } from "@angular/core";
import { FormsModule } from "@angular/forms";
import { NgFor, NgIf } from "@angular/common";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { ChatMessage, UdfContext, UdfCopilotService } from "../../service/udf-copilot/udf-copilot.service";

interface ChatRow {
  role: "user" | "assistant";
  content: string;
  suggestedCode?: string;
}

@Component({
  selector: "texera-udf-copilot-panel",
  templateUrl: "./udf-copilot-panel.component.html",
  styleUrls: ["./udf-copilot-panel.component.scss"],
  imports: [FormsModule, NgFor, NgIf, NzButtonComponent, NzIconDirective],
})
export class UdfCopilotPanelComponent {
  // Function-reference inputs (stable across change detection cycles) — the
  // parent passes thunks instead of values so the panel doesn't churn whenever
  // the parent renders (e.g. on every resize tick).
  @Input() getCode!: () => string;
  @Input() getContext!: () => UdfContext;
  @Output() apply = new EventEmitter<string>();
  @Output() closed = new EventEmitter<void>();

  rows: ChatRow[] = [];
  input: string = "";
  loading = false;

  private _contextHint?: UdfContext;

  /** Snapshot used only for the empty-state hint. Cached on first read. */
  get contextHint(): UdfContext {
    if (!this._contextHint) {
      try {
        this._contextHint = this.getContext?.() ?? {};
      } catch {
        this._contextHint = {};
      }
    }
    return this._contextHint;
  }

  constructor(private udfCopilot: UdfCopilotService) {}

  send() {
    const text = this.input.trim();
    if (!text || this.loading) return;
    this.rows = [...this.rows, { role: "user", content: text }];
    this.input = "";
    this.loading = true;

    const apiMessages: ChatMessage[] = this.rows
      .filter(r => r.role === "user" || r.role === "assistant")
      .map(r => ({ role: r.role, content: r.content }));

    let code = "";
    let context: UdfContext = {};
    try {
      code = this.getCode?.() ?? "";
      context = this.getContext?.() ?? {};
    } catch {}

    this.udfCopilot
      .chat({ messages: apiMessages, code, context })
      .subscribe({
        next: res => {
          this.rows = [
            ...this.rows,
            { role: "assistant", content: res.reply ?? "", suggestedCode: res.suggestedCode },
          ];
          this.loading = false;
        },
        error: err => {
          this.rows = [
            ...this.rows,
            { role: "assistant", content: `Error: ${err?.message ?? "request failed"}` },
          ];
          this.loading = false;
        },
      });
  }

  onApply(suggested?: string) {
    if (suggested) this.apply.emit(suggested);
  }

  onKeyDown(ev: KeyboardEvent) {
    if (ev.key === "Enter" && (ev.metaKey || ev.ctrlKey)) {
      ev.preventDefault();
      this.send();
    }
  }

  clear() {
    this.rows = [];
  }

  close() {
    this.closed.emit();
  }
}
