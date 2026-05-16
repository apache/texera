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

import { Component, Input, OnChanges, OnDestroy } from "@angular/core";
import { CommonModule } from "@angular/common";
import { HttpClient } from "@angular/common/http";
import { Subject, takeUntil } from "rxjs";

interface WfOp {
  operatorID: string;
  customDisplayName?: string;
  operatorType: string;
}
interface WfLink {
  source: { operatorID: string };
  target: { operatorID: string };
}
interface WfContent {
  operators: WfOp[];
  links: WfLink[];
  operatorPositions: Record<string, { x: number; y: number }>;
}

interface Node {
  id: string;
  label: string;
  x: number;
  y: number;
  w: number;
  h: number;
  kind: "source" | "udf" | "sink";
}
interface Edge {
  fromId: string;
  toId: string;
}

const CACHE = new Map<number, WfContent>();
const NODE_W = 96;
const NODE_H = 38;
const VIEW_W = 540;
const VIEW_H = 260;

@Component({
  selector: "texera-bp-wf-preview",
  standalone: true,
  imports: [CommonModule],
  template: `
    <ng-container *ngIf="error; else maybeLoaded">
      <div class="wf-preview-msg">Couldn't load workflow preview · {{ error }}</div>
    </ng-container>
    <ng-template #maybeLoaded>
      <ng-container *ngIf="loaded; else loadingTpl">
        <div class="wf-preview-header">
          <span>{{ opCount }} operators · {{ linkCount }} links</span>
        </div>
        <svg
          [attr.viewBox]="'0 0 ' + VIEW_W + ' ' + VIEW_H"
          class="wf-svg">
          <defs>
            <marker
              id="bp-wf-arrow"
              viewBox="0 0 10 10"
              refX="9"
              refY="5"
              markerWidth="7"
              markerHeight="7"
              orient="auto">
              <path
                d="M 0 0 L 10 5 L 0 10 z"
                fill="var(--bp-accent)" />
            </marker>
          </defs>
          <g>
            <path
              *ngFor="let e of edgePaths"
              [attr.d]="e"
              stroke="var(--bp-accent)"
              stroke-width="1.5"
              fill="none"
              marker-end="url(#bp-wf-arrow)"
              opacity="0.7" />
          </g>
          <g>
            <g
              *ngFor="let n of nodes"
              [attr.transform]="'translate(' + n.x + ',' + n.y + ')'">
              <rect
                [attr.width]="n.w"
                [attr.height]="n.h"
                rx="6"
                ry="6"
                [attr.fill]="nodeFill(n.kind)"
                [attr.stroke]="nodeStroke(n.kind)"
                stroke-width="1" />
              <text
                [attr.x]="n.w / 2"
                [attr.y]="n.h / 2 + 3"
                text-anchor="middle"
                font-size="10"
                fill="var(--bp-text)">
                {{ truncate(n.label) }}
              </text>
            </g>
          </g>
        </svg>
      </ng-container>
      <ng-template #loadingTpl>
        <div class="wf-preview-msg">Loading workflow…</div>
      </ng-template>
    </ng-template>
  `,
  styles: [
    `
      :host {
        display: block;
        width: 560px;
        background: var(--bp-popover);
        border: 1px solid var(--bp-line);
        border-radius: 10px;
        padding: 12px 14px;
        box-shadow: var(--bp-shadow);
      }
      .wf-preview-header {
        color: var(--bp-muted);
        font-size: 11px;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        font-weight: 600;
        margin-bottom: 8px;
      }
      .wf-preview-msg {
        color: var(--bp-muted);
        font-size: 12.5px;
        padding: 6px 2px;
      }
      .wf-svg {
        width: 100%;
        height: 260px;
        display: block;
      }
    `,
  ],
})
export class BpWfPreviewComponent implements OnChanges, OnDestroy {
  private readonly destroy$ = new Subject<void>();
  @Input() wid: number | null = null;
  loaded = false;
  error = "";
  nodes: Node[] = [];
  edgePaths: string[] = [];
  opCount = 0;
  linkCount = 0;
  readonly VIEW_W = VIEW_W;
  readonly VIEW_H = VIEW_H;

  constructor(private http: HttpClient) {}

  ngOnChanges(): void {
    if (!this.wid) return;
    if (CACHE.has(this.wid)) {
      this.render(CACHE.get(this.wid)!);
      return;
    }
    this.loaded = false;
    this.error = "";
    this.http
      .get<any>(`/api/workflow/${this.wid}`)
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: resp => {
          try {
            const raw = resp?.content ?? resp?.workflow?.content;
            const parsed: WfContent = typeof raw === "string" ? JSON.parse(raw) : raw;
            CACHE.set(this.wid!, parsed);
            this.render(parsed);
          } catch (e) {
            this.error = "parse error";
          }
        },
        error: () => {
          this.error = "fetch failed";
        },
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private render(c: WfContent): void {
    const ops = c.operators || [];
    const links = c.links || [];
    const positions = c.operatorPositions || {};

    this.opCount = ops.length;
    this.linkCount = links.length;

    // Original coordinate bounds
    const xs = ops.map(o => positions[o.operatorID]?.x ?? 0);
    const ys = ops.map(o => positions[o.operatorID]?.y ?? 0);
    const minX = Math.min(...xs, 0);
    const minY = Math.min(...ys, 0);
    const maxX = Math.max(...xs, 1) + NODE_W;
    const maxY = Math.max(...ys, 1) + NODE_H;

    const pad = 12;
    const sx = (VIEW_W - 2 * pad) / Math.max(1, maxX - minX);
    const sy = (VIEW_H - 2 * pad) / Math.max(1, maxY - minY);
    const s = Math.min(sx, sy, 1);

    const indexById = new Map<string, Node>();
    this.nodes = ops.map(o => {
      const pos = positions[o.operatorID] ?? { x: 0, y: 0 };
      const n: Node = {
        id: o.operatorID,
        label: o.customDisplayName || o.operatorID,
        x: pad + (pos.x - minX) * s,
        y: pad + (pos.y - minY) * s,
        w: NODE_W * s,
        h: NODE_H * s,
        kind: this.classify(o, links),
      };
      indexById.set(o.operatorID, n);
      return n;
    });

    this.edgePaths = links
      .map(l => {
        const a = indexById.get(l.source.operatorID);
        const b = indexById.get(l.target.operatorID);
        if (!a || !b) return "";
        const x1 = a.x + a.w;
        const y1 = a.y + a.h / 2;
        const x2 = b.x;
        const y2 = b.y + b.h / 2;
        const dx = Math.max(20, (x2 - x1) / 2);
        return `M ${x1} ${y1} C ${x1 + dx} ${y1}, ${x2 - dx} ${y2}, ${x2} ${y2}`;
      })
      .filter(p => p !== "");

    this.loaded = true;
  }

  private classify(op: WfOp, links: WfLink[]): "source" | "udf" | "sink" {
    const id = op.operatorID;
    const hasIn = links.some(l => l.target.operatorID === id);
    const hasOut = links.some(l => l.source.operatorID === id);
    if (!hasIn) return "source";
    if (!hasOut) return "sink";
    return "udf";
  }

  nodeFill(kind: Node["kind"]): string {
    if (kind === "source") return "var(--bp-panel-2)";
    if (kind === "sink") return "rgba(74, 222, 128, 0.10)";
    return "var(--bp-panel)";
  }
  nodeStroke(kind: Node["kind"]): string {
    if (kind === "source") return "var(--bp-line)";
    if (kind === "sink") return "var(--bp-good)";
    return "var(--bp-accent)";
  }

  truncate(s: string): string {
    if (s.length <= 14) return s;
    return s.slice(0, 13) + "…";
  }
}
