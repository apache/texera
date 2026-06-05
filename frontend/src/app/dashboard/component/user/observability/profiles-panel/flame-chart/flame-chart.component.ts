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

import { Component, Input, OnChanges, SimpleChanges } from "@angular/core";
import { CommonModule } from "@angular/common";
import { FlameFrame } from "../../../../../service/user/observability/observability.types";

/**
 * Lightweight SVG flame-graph renderer.
 *
 * Reasons we built this rather than pulling a flame-chart library:
 *   - The gateway already returns a typed FlameFrame tree, so the
 *     layout work is trivial — no need to vet a third-party
 *     renderer's HTML/template paths.
 *   - Smaller bundle: this whole component is < 200 lines.
 *   - Full control over the trust surface: bars are <rect> nodes,
 *     labels are <text> nodes, both bound with Angular property /
 *     interpolation only. Nothing is rendered via innerHTML; no
 *     formatter callbacks built from server output.
 *
 * Layout: classic top-down flame chart. The root frame spans the
 * full width; each level below subdivides its parent's range in
 * proportion to children's `value`. Frames whose computed width is
 * below `MIN_VISIBLE_PX` are skipped to keep the DOM bounded for
 * very wide trees.
 */
@Component({
  selector: "texera-flame-chart",
  templateUrl: "./flame-chart.component.html",
  styleUrls: ["./flame-chart.component.scss"],
  imports: [CommonModule],
})
export class FlameChartComponent implements OnChanges {
  /** Root frame. ``null`` renders an empty state in the parent. */
  @Input() root: FlameFrame | null = null;
  /** Outer width in px. Bars scale inside this box. */
  @Input() width = 720;
  /** Height per stack level in px. Total chart height grows with
   *  tree depth. */
  @Input() levelHeight = 18;

  /** Pre-flattened rows ready for the template *ngFor. */
  rows: ReadonlyArray<FlameRow> = [];
  totalDepth = 0;

  ngOnChanges(changes: SimpleChanges): void {
    if ("root" in changes || "width" in changes) {
      this.recompute();
    }
  }

  private recompute(): void {
    if (!this.root || this.root.value <= 0) {
      this.rows = [];
      this.totalDepth = 0;
      return;
    }
    this.rows = flattenFlame(this.root, this.width);
    this.totalDepth = this.rows.reduce((m, r) => Math.max(m, r.depth + 1), 0);
  }

  /** Stable hue derived from the frame name so repeated frames keep
   *  the same colour across re-renders without coupling to any
   *  library palette. */
  hueFor(name: string): number {
    let h = 0;
    for (let i = 0; i < name.length; i++) {
      h = (h * 31 + name.charCodeAt(i)) % 360;
    }
    return h;
  }

  trackByRow(_index: number, row: FlameRow): string {
    return `${row.depth}:${row.x}:${row.name}`;
  }
}

/** One bar in the rendered flame graph. */
export interface FlameRow {
  readonly name: string;
  /** Stack depth, 0 = root. */
  readonly depth: number;
  /** Pixel offset from the left edge. */
  readonly x: number;
  /** Bar width in px. */
  readonly width: number;
  /** Sample value (raw, not pixels). Surfaced as a tooltip via the title attribute. */
  readonly value: number;
}

/** Visual cutoff — bars narrower than this are not emitted. */
export const MIN_VISIBLE_PX = 2;

/**
 * Pure function: walk the flame tree and produce one [[FlameRow]]
 * per visible frame. Exported so it can be unit-tested without a DOM
 * fixture. Defensive against zero / negative values (treated as 0)
 * and missing children arrays (treated as empty).
 */
export function flattenFlame(root: FlameFrame, totalWidth: number): ReadonlyArray<FlameRow> {
  const rows: FlameRow[] = [];
  const totalValue = root.value > 0 ? root.value : 0;
  if (totalValue === 0 || totalWidth <= 0) return rows;

  function walk(frame: FlameFrame, depth: number, x: number, width: number): void {
    if (width < MIN_VISIBLE_PX) return;
    rows.push({
      name: frame.name,
      depth,
      x,
      width,
      value: frame.value,
    });
    const children = frame.children ?? [];
    if (children.length === 0) return;
    // Children widths are proportional to their value share. We
    // do NOT renormalize against the parent — children's values may
    // sum to less than the parent (self-time), which produces a
    // trailing gap, the standard flame-graph convention.
    const parentValue = frame.value > 0 ? frame.value : 0;
    if (parentValue === 0) return;
    let cursor = x;
    for (const child of children) {
      const cv = child.value > 0 ? child.value : 0;
      const cw = (cv / parentValue) * width;
      walk(child, depth + 1, cursor, cw);
      cursor += cw;
    }
  }

  walk(root, 0, 0, totalWidth);
  return rows;
}
