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

import { CommonModule } from "@angular/common";
import {
  Component,
  ChangeDetectorRef,
  ElementRef,
  EventEmitter,
  Input,
  OnChanges,
  OnDestroy,
  OnInit,
  Output,
  SimpleChanges,
} from "@angular/core";
import { FormsModule } from "@angular/forms";
import { AnalyzeResponse, UploadResponse } from "../../../service/user/genesis/genesis.service";
import { GenesisStepItem, GenesisStepsPanelComponent } from "./genesis-steps-panel.component";

export type GenesisCardChoice =
  | { kind: "suggestion"; suggestionId: string }
  | { kind: "custom"; text: string }
  | { kind: "skip" }
  | { kind: "cancel" };

@Component({
  selector: "texera-genesis-card",
  standalone: true,
  imports: [CommonModule, FormsModule, GenesisStepsPanelComponent],
  templateUrl: "./genesis-card.component.html",
  styleUrls: ["./genesis-card.component.scss"],
})
export class GenesisCardComponent implements OnInit, OnDestroy, OnChanges {
  /**
   * When null, only the optional thinking banner is shown (upload/analyze in flight).
   * When set, the full pick UI is rendered.
   */
  @Input() data: { upload: UploadResponse; analyze: AnalyzeResponse } | null = null;
  /** Upload/analyze phased steps (Linear-style checklist). Empty when idle. */
  @Input() analysisSteps: GenesisStepItem[] = [];

  @Output() choice = new EventEmitter<GenesisCardChoice>();

  /** Fade-out before emit. */
  visible = true;

  customGoalText = "";

  /** Staggered visibility for suggestion cards ( indices true = visible ). */
  cardVisible: boolean[] = [];

  private staggerTimers: number[] = [];

  get headerTitle(): string {
    if (!this.data) {
      return "";
    }
    const raw = (
      this.data.analyze?.dataset_summary ||
      this.data.analyze?.detected_scenario ||
      "Dataset"
    ).trim();
    return raw || "Dataset";
  }

  constructor(
    private host: ElementRef<HTMLElement>,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.host.nativeElement.focus();
    window.addEventListener("keydown", this.onKeyDown);
  }

  ngOnDestroy(): void {
    window.removeEventListener("keydown", this.onKeyDown);
    this.clearStaggerTimers();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (!changes["data"]) {
      return;
    }
    this.clearStaggerTimers();
    if (!this.data) {
      this.cardVisible = [];
      return;
    }
    const n = this.suggestions.length;
    this.cardVisible = new Array(n).fill(false);
    for (let i = 0; i < n; i++) {
      const tid = window.setTimeout(() => {
        this.cardVisible[i] = true;
        this.cdr.detectChanges();
      }, 100 * i);
      this.staggerTimers.push(tid);
    }
  }

  private clearStaggerTimers(): void {
    for (const t of this.staggerTimers) {
      window.clearTimeout(t);
    }
    this.staggerTimers = [];
  }

  private readonly onKeyDown = (ev: KeyboardEvent): void => {
    if (!this.data) {
      return;
    }
    const target = ev.target as HTMLElement | null;
    if (
      target &&
      (target.tagName === "TEXTAREA" ||
        target.tagName === "INPUT" ||
        target.tagName === "SELECT" ||
        target.isContentEditable)
    ) {
      return;
    }
    const key = ev.key.toLowerCase();
    if (key === "a") {
      this.pickIndex(0);
    } else if (key === "b") {
      this.pickIndex(1);
    } else if (key === "c") {
      this.pickIndex(2);
    } else if (key === "d") {
      this.pickIndex(3);
    }
  };

  get displayRowCount(): number {
    if (!this.data) {
      return 0;
    }
    const a = this.data.analyze;
    const u = this.data.upload;
    return a?.row_count ?? u?.row_count ?? u?.sample_rows?.length ?? 0;
  }

  get suggestions() {
    if (!this.data) {
      return [];
    }
    return (this.data.analyze.suggestions ?? []).slice(0, 4);
  }

  get analyzeError(): boolean {
    return !!this.data?.analyze?.llm_error;
  }

  isChoiceDisabled(s: { error?: boolean }): boolean {
    return !!s?.error;
  }

  cardStaggerClass(i: number): string {
    if (i < 0 || i >= this.cardVisible.length) {
      return "";
    }
    return this.cardVisible[i] ? "genesis-card__choice--stagger-visible" : "";
  }

  pickIndex(index: number): void {
    if (!this.data) {
      return;
    }
    const s = this.suggestions[index];
    if (!s || this.isChoiceDisabled(s)) {
      return;
    }
    this.finish({ kind: "suggestion", suggestionId: s.id });
  }

  onCustomBuild(): void {
    if (!this.data) {
      return;
    }
    const text = this.customGoalText.trim();
    if (!text) {
      return;
    }
    this.finish({ kind: "custom", text });
  }

  /** Cmd+Enter / Ctrl+Enter submits; plain Enter stays a newline in the textarea. */
  onCustomGoalKeydown(ev: KeyboardEvent): void {
    if (ev.key !== "Enter") {
      return;
    }
    if (!ev.ctrlKey && !ev.metaKey) {
      return;
    }
    ev.preventDefault();
    ev.stopPropagation();
    this.onCustomBuild();
  }

  onSkip(): void {
    if (!this.data) {
      return;
    }
    this.finish({ kind: "skip" });
  }

  onCancel(): void {
    this.finish({ kind: "cancel" });
  }

  private finish(choice: GenesisCardChoice): void {
    this.visible = false;
    window.setTimeout(() => this.choice.emit(choice), 200);
  }
}
