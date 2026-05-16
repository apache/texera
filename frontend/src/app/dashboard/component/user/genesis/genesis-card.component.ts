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
import { Component, ElementRef, EventEmitter, Input, OnDestroy, OnInit, Output } from "@angular/core";
import { AnalyzeResponse, UploadResponse } from "../../../service/user/genesis/genesis.service";

export type GenesisCardChoice =
  | { kind: "suggestion"; suggestionId: string }
  | { kind: "skip" }
  | { kind: "cancel" };

@Component({
  selector: "texera-genesis-card",
  standalone: true,
  imports: [CommonModule],
  templateUrl: "./genesis-card.component.html",
  styleUrls: ["./genesis-card.component.scss"],
})
export class GenesisCardComponent implements OnInit, OnDestroy {
  @Input({ required: true }) data!: { upload: UploadResponse; analyze: AnalyzeResponse };
  @Output() choice = new EventEmitter<GenesisCardChoice>();

  /** Fade-out before emit. */
  visible = true;

  constructor(private host: ElementRef<HTMLElement>) {}

  ngOnInit(): void {
    this.host.nativeElement.focus();
    window.addEventListener("keydown", this.onKeyDown);
  }

  ngOnDestroy(): void {
    window.removeEventListener("keydown", this.onKeyDown);
  }

  private readonly onKeyDown = (ev: KeyboardEvent) => {
    const key = ev.key.toLowerCase();
    if (key === "a") {
      this.pickIndex(0);
    } else if (key === "b") {
      this.pickIndex(1);
    } else if (key === "c") {
      this.pickIndex(2);
    }
  };

  get displayRowCount(): number {
    const a = this.data?.analyze;
    const u = this.data?.upload;
    return (
      a?.row_count ??
      u?.row_count ??
      u?.sample_rows?.length ??
      0
    );
  }

  get suggestions() {
    return (this.data?.analyze.suggestions ?? []).slice(0, 3);
  }

  pickIndex(index: number): void {
    const s = this.suggestions[index];
    if (!s) {
      return;
    }
    this.finish({ kind: "suggestion", suggestionId: s.id });
  }

  onSkip(): void {
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
