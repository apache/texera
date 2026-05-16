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

import { Component } from "@angular/core";
import { CommonModule } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { TEXERA_WORKFLOW_IDS, texeraWorkflowUrl } from "../bet-pilot.service";
import { BpWfPreviewComponent } from "./bp-wf-preview.component";

interface ParsedLine {
  raw: string;
  player: string;
  line: number | null;
  direction: "HIGHER" | "LOWER" | "?";
  valid: boolean;
  note: string;
}

@Component({
  selector: "texera-bp-lines-input",
  standalone: true,
  imports: [CommonModule, FormsModule, BpWfPreviewComponent],
  templateUrl: "./bp-lines-input.component.html",
  styleUrls: ["./bp-screen.shared.scss"],
})
export class BpLinesInputComponent {
  rawInput = "";
  uploaded: string[] = ["champions-picks-3.49x.png", "champions-picks-2.97x.png"]; // image filenames the user listed
  status: "idle" | "sending" | "sent" = "idle";
  workflowUrl = texeraWorkflowUrl("wf2_daily");
  wfId = TEXERA_WORKFLOW_IDS.wf2_daily;

  // Stubbed OCR output for the demo — one fake line per filename
  stubLines: ParsedLine[] = [
    {
      raw: "PatMen Higher 31.5 Kills on Maps 1+2",
      player: "PatMen",
      line: 31.5,
      direction: "HIGHER",
      valid: true,
      note: "OCR captured from 3.49x slip",
    },
    {
      raw: "invy Higher 27.5 Kills on Maps 1+2",
      player: "invy",
      line: 27.5,
      direction: "HIGHER",
      valid: true,
      note: "OCR captured from 3.49x slip",
    },
    {
      raw: "Primmie Higher 38.5 Kills on Maps 1+2",
      player: "Primmie",
      line: 38.5,
      direction: "HIGHER",
      valid: true,
      note: "OCR captured from 2.97x slip",
    },
  ];

  get parsedLines(): ParsedLine[] {
    return this.rawInput
      .split("\n")
      .map(s => s.trim())
      .filter(s => s.length > 0)
      .map(line => this.parseLine(line));
  }

  get validCount(): number {
    return this.combined.filter(p => p.valid).length;
  }
  get errorCount(): number {
    return this.combined.filter(p => !p.valid).length;
  }

  /**
   * Drag-and-drop / file-picker handler. We never upload the bytes — we just
   * collect filenames + sizes so the screen can show a list of "uploaded"
   * images. Real OCR happens in the workflow's UDF; today that's stubbed.
   */
  onFilesChosen(event: Event): void {
    const input = event.target as HTMLInputElement;
    if (!input.files) return;
    const names = Array.from(input.files).map(f => f.name);
    this.uploaded = [...this.uploaded, ...names];
    // Add a fake OCR row per image so the user sees a complete pipeline shape
    for (const n of names) {
      this.stubLines.push({
        raw: `(from ${n})`,
        player: this.fakePlayer(n),
        line: 22.5 + Math.floor(Math.random() * 12),
        direction: "HIGHER",
        valid: true,
        note: "stub OCR — real model will parse this image",
      });
    }
    input.value = "";
  }

  removeUploaded(i: number): void {
    this.uploaded.splice(i, 1);
    this.stubLines.splice(i, 1);
  }

  /**
   * Manual line entry. Expects one of:
   *   "PatMen HIGHER 31.5"
   *   "invy 27.5"   (direction defaulted to HIGHER)
   *   "Primmie, 38.5, LOWER"
   * Anything that doesn't match a name + number is flagged.
   */
  private parseLine(raw: string): ParsedLine {
    // strip commas, normalize spaces
    const tokens = raw.replace(/,/g, " ").replace(/\s+/g, " ").trim().split(" ");
    let player = "",
      line: number | null = null,
      direction: "HIGHER" | "LOWER" | "?" = "?";
    for (const t of tokens) {
      const normalized = t.toUpperCase();
      if (normalized === "HIGHER" || normalized === "OVER") {
        direction = "HIGHER";
      } else if (normalized === "LOWER" || normalized === "UNDER") {
        direction = "LOWER";
      } else if (/^\d+(\.\d+)?$/.test(t)) {
        line = parseFloat(t);
      } else {
        player = player ? `${player} ${t}` : t;
      }
    }
    if (!player || line === null) {
      return { raw, player, line, direction, valid: false, note: "couldn't parse: need a name and a number" };
    }
    if (direction === "?") direction = "HIGHER";
    return { raw, player, line, direction, valid: true, note: "ready" };
  }

  private fakePlayer(filename: string): string {
    // strip extension and use the basename as a fake player name
    return filename.replace(/\.[^.]+$/, "").replace(/[^A-Za-z0-9]/g, "");
  }

  /**
   * Stand-in send. Real version posts a JSON payload of parsed lines to a
   * workflow execution endpoint and polls for results.
   */
  sendToWorkflow(): void {
    this.status = "sending";
    setTimeout(() => {
      this.status = "sent";
    }, 700);
  }

  reset(): void {
    this.rawInput = "";
    this.uploaded = [];
    this.stubLines = [];
    this.status = "idle";
  }

  get combined(): ParsedLine[] {
    return [...this.stubLines, ...this.parsedLines];
  }
}
