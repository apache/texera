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

import {
  AfterViewInit,
  Component,
  ElementRef,
  EventEmitter,
  Input,
  NgZone,
  OnChanges,
  OnDestroy,
  Output,
  SimpleChanges,
  ViewChild,
} from "@angular/core";
import { CommonModule } from "@angular/common";
import { fromEvent, Subscription } from "rxjs";
import { takeUntil } from "rxjs/operators";
import * as joint from "jointjs";
import { JointUIService } from "../../../service/joint-ui/joint-ui.service";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { OperatorPredicate, OperatorLink } from "../../../types/workflow-common.interface";

/**
 * Per-operator diff status used to drive both stroke color and the small label rendered
 * under each operator. Computed once in compare-workspace from a combination of:
 *   - operator presence (only in one side's content)
 *   - operator properties (operatorProperties JSON)
 *   - output (row counts / schema from the compare summary)
 */
export type OperatorDiffStatus =
  | "identical"
  | "propsDiffer"
  | "outputDiffer"
  | "onlyInA"
  | "onlyInB";

/** Which side of the compare this DAG renders — drives onlyIn* interpretation per side. */
export type CompareSide = "A" | "B";

interface SavedWorkflowContent {
  operators?: OperatorPredicate[];
  links?: OperatorLink[];
  operatorPositions?: Record<string, { x: number; y: number }>;
}

// Color + short user-facing label per diff status. Reading order from least → most
// disruptive: identical (green) → outputDiffer (amber) → propsDiffer (orange) →
// onlyHere (red). Kept short so it fits under a 60-px operator at any zoom.
const STATUS_VISUAL: Record<OperatorDiffStatus, { color: string; label: string }> = {
  identical: { color: "#2ecc71", label: "matches" },
  outputDiffer: { color: "#f1c40f", label: "output differs" },
  propsDiffer: { color: "#e67e22", label: "props differ" },
  onlyInA: { color: "#e74c3c", label: "only here" },
  onlyInB: { color: "#e74c3c", label: "only here" },
};
const STATUS_STROKE_WIDTH = 4;
const UNKNOWN_STROKE = "#bbbbbb";
const SELECTED_STROKE = "#1a73e8";
const SELECTED_STROKE_WIDTH = 5;

const MIN_ZOOM = 0.25;
const MAX_ZOOM = 3;
const ZOOM_STEP = 0.1;
const FIT_PADDING = 40;

@Component({
  selector: "texera-compare-dag",
  standalone: true,
  imports: [CommonModule],
  template: `
    <div class="compare-dag">
      <div class="compare-dag-header">
        <span>{{ label }}</span>
        <span class="compare-dag-tools">
          <button type="button" title="Zoom out" (click)="zoomBy(-1)">−</button>
          <button type="button" title="Reset view" (click)="fitToContent()">Fit</button>
          <button type="button" title="Zoom in" (click)="zoomBy(1)">+</button>
        </span>
      </div>
      <div #paperHost class="compare-dag-paper"></div>
    </div>
  `,
  styles: [
    `
      :host {
        display: block;
        height: 100%;
      }
      .compare-dag {
        display: flex;
        flex-direction: column;
        height: 100%;
        background: #fff;
        border: 1px solid #e0e0e0;
        border-radius: 4px;
        overflow: hidden;
      }
      .compare-dag-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 6px 10px;
        background: #f5f5f5;
        font-weight: 600;
        font-size: 12px;
        border-bottom: 1px solid #e0e0e0;
      }
      .compare-dag-tools {
        display: inline-flex;
        gap: 4px;
        font-weight: 400;
      }
      .compare-dag-tools button {
        width: 24px;
        height: 22px;
        line-height: 20px;
        border: 1px solid #d0d0d0;
        background: #fff;
        border-radius: 3px;
        cursor: pointer;
        font-size: 12px;
        padding: 0;
      }
      .compare-dag-tools button:hover {
        background: #f0f0f0;
      }
      .compare-dag-paper {
        flex: 1;
        min-height: 0;
        background: #fafafa;
        overflow: hidden;
        cursor: grab;
        position: relative;
      }
      .compare-dag-paper.is-panning {
        cursor: grabbing;
      }
    `,
  ],
})
export class CompareDagComponent implements AfterViewInit, OnChanges, OnDestroy {
  @Input() label = "";
  /** Parsed workflow content (operators / links / operatorPositions) — or a JSON string. */
  @Input() content: SavedWorkflowContent | string | null = null;
  /** Combined diff status per operator (presence + props + output). */
  @Input() diffStatusMap: ReadonlyMap<string, OperatorDiffStatus> = new Map();
  @Input() selectedOperatorId: string | null = null;
  /**
   * Which side this DAG renders. Lets us interpret `onlyInA` / `onlyInB` correctly:
   * an operator with status `onlyInA` is "only here" on side A, but on side B it
   * simply isn't in the content (so this case never reaches that DAG).
   */
  @Input() side: CompareSide = "A";

  @Output() operatorSelected = new EventEmitter<string>();

  @ViewChild("paperHost", { static: true }) paperHost!: ElementRef<HTMLDivElement>;

  private graph: joint.dia.Graph | null = null;
  private paper: joint.dia.Paper | null = null;
  private operatorCells = new Map<string, joint.dia.Element>();
  private metadataReady = false;
  private resizeObserver: ResizeObserver | null = null;
  private wheelListener: ((e: WheelEvent) => void) | null = null;
  private panSubscription: Subscription | null = null;

  constructor(
    private jointUIService: JointUIService,
    private operatorMetadataService: OperatorMetadataService,
    private zone: NgZone
  ) {}

  ngAfterViewInit(): void {
    this.graph = new joint.dia.Graph();
    const host = this.paperHost.nativeElement;
    // Mirror WorkflowEditorComponent's paper config so operators render with the same look.
    this.paper = new joint.dia.Paper({
      el: host,
      model: this.graph,
      background: { color: "#F6F6F6" },
      drawGrid: { name: "fixedDot", args: { color: "black", scaleFactor: 8, thickness: 1.2 } },
      gridSize: 1,
      sorting: joint.dia.Paper.sorting.APPROX,
      defaultLink: JointUIService.getDefaultLinkCell(),
      width: Math.max(host.clientWidth, 100),
      height: Math.max(host.clientHeight, 100),
      // Read-only viewer — clicks for selection only, no dragging operators or links around.
      interactive: {
        elementMove: false,
        labelMove: false,
        arrowheadMove: false,
        vertexMove: false,
        vertexAdd: false,
        vertexRemove: false,
        addLinkFromMagnet: false,
        useLinkTools: false,
        linkMove: false,
      },
    });

    // Use pointerdown (not pointerclick) so the selection fires even if the mouse moves
    // a pixel between down and up — JointJS would otherwise reclassify the click as a drag
    // and pointerclick wouldn't fire. Mirrors how the main editor wires selection.
    this.paper.on("element:pointerdown", (cellView: joint.dia.ElementView) => {
      const id = cellView.model.id as string;
      if (id) this.operatorSelected.emit(id);
    });

    this.installPanHandler();
    this.installZoomHandler();
    this.installResizeObserver();

    // JointUIService.getJointOperatorElement requires operator metadata so it can look up
    // additionalMetadata. Wait for it before the first render.
    this.operatorMetadataService.getOperatorMetadata().subscribe(() => {
      this.metadataReady = true;
      this.render();
    });
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (!this.graph || !this.paper) return;
    if (changes["content"]) {
      this.render();
    } else if (changes["diffStatusMap"] || changes["selectedOperatorId"]) {
      this.applyStyling();
    }
    if (changes["selectedOperatorId"]) {
      // When the selection changes from the other DAG, the operator we just got asked
      // to highlight may be outside this DAG's current viewport. Pan it into view so the
      // user can actually see the cross-side highlight.
      this.panToSelectedOperator();
    }
  }

  ngOnDestroy(): void {
    this.resizeObserver?.disconnect();
    this.resizeObserver = null;
    if (this.wheelListener) {
      this.paperHost.nativeElement.removeEventListener("wheel", this.wheelListener);
      this.wheelListener = null;
    }
    this.panSubscription?.unsubscribe();
    this.panSubscription = null;
    this.paper?.remove();
    this.paper = null;
    this.graph = null;
    this.operatorCells.clear();
  }

  zoomBy(direction: number): void {
    if (!this.paper) return;
    const current = this.paper.scale().sx || 1;
    const next = Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, current + direction * ZOOM_STEP));
    // Zoom around the paper center so the visible content stays roughly in place.
    const rect = this.paperHost.nativeElement.getBoundingClientRect();
    this.scaleAround(next, rect.width / 2, rect.height / 2);
  }

  fitToContent(): void {
    if (!this.paper || !this.graph) return;
    const cells = this.graph.getCells();
    if (cells.length === 0) {
      this.paper.scale(1, 1);
      this.paper.translate(0, 0);
      return;
    }
    const bbox = this.graph.getBBox();
    if (!bbox || bbox.width === 0 || bbox.height === 0) return;
    const host = this.paperHost.nativeElement;
    const viewportWidth = Math.max(host.clientWidth - FIT_PADDING * 2, 100);
    const viewportHeight = Math.max(host.clientHeight - FIT_PADDING * 2, 100);
    // Choose the largest scale that fits both axes, clamped to our zoom range.
    const scale = Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, Math.min(viewportWidth / bbox.width, viewportHeight / bbox.height)));
    this.paper.scale(scale, scale);
    // Translate so the bbox is centered in the viewport.
    const tx = (host.clientWidth - bbox.width * scale) / 2 - bbox.x * scale;
    const ty = (host.clientHeight - bbox.height * scale) / 2 - bbox.y * scale;
    this.paper.translate(tx, ty);
  }

  private installPanHandler(): void {
    if (!this.paper) return;
    const paper = this.paper;
    const host = this.paperHost.nativeElement;
    // Drag on blank area to pan. Mirrors WorkflowEditorComponent.handlePaperPan.
    paper.on("blank:pointerdown", () => {
      host.classList.add("is-panning");
      this.zone.runOutsideAngular(() => {
        const up$ = fromEvent(document, "mouseup");
        const move$ = fromEvent<MouseEvent>(document, "mousemove").pipe(takeUntil(up$));
        const moveSub = move$.subscribe(event => {
          paper.translate(paper.translate().tx + event.movementX, paper.translate().ty + event.movementY);
        });
        up$.subscribe(() => {
          moveSub.unsubscribe();
          host.classList.remove("is-panning");
        });
      });
    });
  }

  private installZoomHandler(): void {
    if (!this.paper) return;
    const host = this.paperHost.nativeElement;
    // Mouse wheel zooms around the cursor. preventDefault so the page doesn't scroll too.
    this.wheelListener = (event: WheelEvent) => {
      event.preventDefault();
      if (!this.paper) return;
      const current = this.paper.scale().sx || 1;
      const direction = event.deltaY < 0 ? 1 : -1;
      const next = Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, current + direction * ZOOM_STEP));
      if (next === current) return;
      const rect = host.getBoundingClientRect();
      this.scaleAround(next, event.clientX - rect.left, event.clientY - rect.top);
    };
    host.addEventListener("wheel", this.wheelListener, { passive: false });
  }

  /**
   * Set scale to `nextScale` while keeping the point (`localX`, `localY`) — measured
   * relative to the paper host — anchored in place. Adjusting the translation after the
   * scale is what makes "zoom toward the cursor" feel natural.
   */
  private scaleAround(nextScale: number, localX: number, localY: number): void {
    if (!this.paper) return;
    const currentScale = this.paper.scale().sx || 1;
    const { tx, ty } = this.paper.translate();
    // Model coords of the cursor before scaling.
    const modelX = (localX - tx) / currentScale;
    const modelY = (localY - ty) / currentScale;
    this.paper.scale(nextScale, nextScale);
    // Translate so that the same model point lands at (localX, localY) again.
    this.paper.translate(localX - modelX * nextScale, localY - modelY * nextScale);
  }

  private installResizeObserver(): void {
    if (typeof ResizeObserver === "undefined") return;
    const host = this.paperHost.nativeElement;
    this.resizeObserver = new ResizeObserver(() => {
      if (!this.paper) return;
      this.paper.setDimensions(Math.max(host.clientWidth, 100), Math.max(host.clientHeight, 100));
    });
    this.resizeObserver.observe(host);
  }

  private render(): void {
    if (!this.graph || !this.metadataReady) return;
    this.graph.clear();
    this.operatorCells.clear();
    if (!this.content) return;

    let parsed: SavedWorkflowContent;
    if (typeof this.content === "string") {
      try {
        parsed = JSON.parse(this.content) as SavedWorkflowContent;
      } catch {
        return;
      }
    } else {
      parsed = this.content;
    }
    const operators = parsed.operators ?? [];
    const links = parsed.links ?? [];
    const positions = parsed.operatorPositions ?? {};

    // Operators — reuse the main editor's JointUIService so each operator looks like a
    // real Texera operator (proper ports, type-specific icon, friendly name, etc.).
    operators.forEach(op => {
      const point = positions[op.operatorID] ?? { x: 0, y: 0 };
      try {
        const cell = this.jointUIService.getJointOperatorElement(op, point);
        this.graph!.addCell(cell);
        this.operatorCells.set(op.operatorID, cell);
      } catch (e) {
        // Operator metadata missing for this type — skip rendering rather than crash.
        // eslint-disable-next-line no-console
        console.warn(`compare-dag: skipping operator ${op.operatorID}:`, e);
      }
    });

    links.forEach(link => {
      if (!this.operatorCells.has(link.source.operatorID) || !this.operatorCells.has(link.target.operatorID)) {
        return;
      }
      try {
        const linkCell = JointUIService.getJointLinkCell(link);
        this.graph!.addCell(linkCell);
      } catch (e) {
        // eslint-disable-next-line no-console
        console.warn(`compare-dag: skipping link ${link.linkID}:`, e);
      }
    });

    this.applyStyling();
    // Fit on next frame so the host has its final size after Angular's view pass.
    requestAnimationFrame(() => this.fitToContent());
  }

  private applyStyling(): void {
    this.operatorCells.forEach((cell, opId) => {
      const status = this.diffStatusMap.get(opId);
      const visual = status ? STATUS_VISUAL[status] : undefined;
      const isSelected = this.selectedOperatorId === opId;
      const strokeColor = isSelected ? SELECTED_STROKE : visual?.color ?? UNKNOWN_STROKE;
      const strokeWidth = isSelected ? SELECTED_STROKE_WIDTH : STATUS_STROKE_WIDTH;
      cell.attr("rect.body/stroke", strokeColor);
      cell.attr("rect.body/stroke-width", strokeWidth);
      cell.attr("rect.body/fill", isSelected ? "#E8F0FE" : "#FFFFFF");
      // Drop a soft shadow on the selected operator so the cross-side highlight pops.
      cell.attr("rect.body/filter", isSelected ? "drop-shadow(0 0 6px rgba(26, 115, 232, 0.6))" : "none");

      // The operator markup already has a `.texera-operator-state` text element used by
      // the main editor for running/completed labels. We're not in an execution view, so
      // it's free real estate — repurpose it for the diff label. Centered under the
      // operator and color-matched to the border so the two signals reinforce each other.
      const labelText = visual?.label ?? "";
      cell.attr("text.texera-operator-state/text", labelText);
      cell.attr("text.texera-operator-state/fill", visual?.color ?? "#888");
      cell.attr("text.texera-operator-state/visibility", labelText ? "visible" : "hidden");
      cell.attr("text.texera-operator-state/font-weight", "600");
    });
  }

  /**
   * Center the selected operator in the viewport — but only if it currently sits outside
   * the viewport. Don't disturb the user's scroll/zoom for operators they already see.
   */
  private panToSelectedOperator(): void {
    if (!this.paper || !this.graph) return;
    const opId = this.selectedOperatorId;
    if (!opId) return;
    const cell = this.operatorCells.get(opId);
    if (!cell) return;
    const host = this.paperHost.nativeElement;
    const scale = this.paper.scale().sx || 1;
    const { tx, ty } = this.paper.translate();
    const bbox = cell.getBBox();
    // Screen-space bbox of the operator.
    const screenX = bbox.x * scale + tx;
    const screenY = bbox.y * scale + ty;
    const screenW = bbox.width * scale;
    const screenH = bbox.height * scale;
    const visible =
      screenX >= 0 && screenY >= 0 && screenX + screenW <= host.clientWidth && screenY + screenH <= host.clientHeight;
    if (visible) return;
    // Center the operator's bbox in the host.
    const targetTx = host.clientWidth / 2 - (bbox.x + bbox.width / 2) * scale;
    const targetTy = host.clientHeight / 2 - (bbox.y + bbox.height / 2) * scale;
    this.paper.translate(targetTx, targetTy);
  }
}
