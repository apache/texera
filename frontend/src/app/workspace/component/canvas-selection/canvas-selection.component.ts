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
  Input,
  Output,
  EventEmitter,
  OnDestroy,
  ChangeDetectorRef,
  inject,
} from "@angular/core";
import { NgIf } from "@angular/common";
import * as joint from "jointjs";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { OperatorMenuService } from "../../service/operator-menu/operator-menu.service";
import { OverboxButtonComponent } from "../overbox/overbox-button.component";
import { selectionBounds, intersectsSelection, isSelectionGesture, SelectionBounds } from "./selection-geometry";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzIconDirective } from "ng-zorro-antd/icon";

/** Captures selection gestures before JointJS pan/drag handlers. Selection uses the shared highlight API. */
@Component({
  selector: "texera-canvas-selection",
  imports: [NgIf, OverboxButtonComponent, NzButtonComponent, NzIconDirective],
  template: `
    <div
      *ngIf="showActions"
      class="selection-actions"
      [style.left.px]="actionX"
      [style.top.px]="actionY">
      <span class="selection-count">{{ selectedCount }} selected</span>
      <button
        nz-button
        nzSize="small"
        type="button"
        title="Copy selected elements"
        (click)="copy()">
        <span
          nz-icon
          nzType="copy"></span>
      </button>
      <button
        nz-button
        nzSize="small"
        nzDanger
        type="button"
        title="Delete selected elements"
        [disabled]="!canModify"
        (click)="remove()">
        <span
          nz-icon
          nzType="delete"></span>
      </button>
      <texera-overbox-button
        size="small"
        [disabled]="!canModify"></texera-overbox-button>
      <button
        nz-button
        nzSize="small"
        type="button"
        title="Close selection actions"
        (click)="dismiss()">
        <span
          nz-icon
          nzType="close"></span>
      </button>
    </div>
  `,
  styles: [
    `
      :host {
        position: absolute;
        inset: 0;
        pointer-events: none;
        z-index: 12;
      }
      .selection-actions {
        position: absolute;
        display: flex;
        align-items: center;
        box-sizing: border-box;
        flex-wrap: nowrap;
        gap: 6px;
        min-height: 36px;
        padding: 4px 6px;
        background: white;
        border: 1px solid #d9d9d9;
        border-radius: 4px;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
        pointer-events: auto;
        white-space: nowrap;
        width: max-content;
      }
      .selection-count {
        color: #595959;
        font-size: 12px;
        padding: 0 4px;
      }
      button[nz-button] {
        align-items: center;
        display: inline-flex;
        justify-content: center;
      }
    `,
  ],
})
export class CanvasSelectionComponent implements AfterViewInit, OnDestroy {
  @Input({ required: true }) paper!: joint.dia.Paper;
  @Input() disabled = false;
  @Output() contextRequested = new EventEmitter<MouseEvent>();
  private clickedID: string | undefined;
  private readonly actions = inject(WorkflowActionService);
  private readonly menu = inject(OperatorMenuService);
  private readonly cdr = inject(ChangeDetectorRef);
  showActions = false;
  actionX = 0;
  actionY = 0;
  private start: { x: number; y: number } | null = null;
  private additive = false;
  private moved = false;
  private suppressContextUntil = 0;
  private selectionElement: SVGRectElement | null = null;
  get canModify(): boolean {
    return !this.disabled && this.actions.checkWorkflowModificationEnabled();
  }
  get selectedCount(): number {
    const selected = this.actions.getJointGraphWrapper().getCurrentHighlights();
    return selected.operators.length + selected.commentBoxes.length + selected.links.length;
  }

  ngAfterViewInit(): void {
    this.paper.el.addEventListener("mousedown", this.down, true);
    this.paper.el.addEventListener("contextmenu", this.contextMenu, true);
    document.addEventListener("mousemove", this.move);
    document.addEventListener("mouseup", this.up);
    document.addEventListener("keydown", this.keydown);
    window.addEventListener("blur", this.cancel);
  }
  ngOnDestroy(): void {
    this.paper.el.removeEventListener("mousedown", this.down, true);
    this.paper.el.removeEventListener("contextmenu", this.contextMenu, true);
    document.removeEventListener("mousemove", this.move);
    document.removeEventListener("mouseup", this.up);
    document.removeEventListener("keydown", this.keydown);
    window.removeEventListener("blur", this.cancel);
    this.removeSelectionElement();
  }
  private down = (event: MouseEvent): void => {
    this.showActions = false;
    if (this.disabled || !isSelectionGesture(event)) {
      this.removeSelectionElement();
      return;
    }
    event.preventDefault();
    event.stopImmediatePropagation();
    this.start = { x: event.clientX, y: event.clientY };
    this.clickedID = this.paper.findView(event.target as SVGElement)?.model.id.toString();
    this.additive = event.shiftKey;
    this.moved = false;
    this.removeSelectionElement();
  };
  private move = (event: MouseEvent): void => {
    if (!this.start) return;
    if (Math.hypot(event.clientX - this.start.x, event.clientY - this.start.y) < 4 && !this.moved) return;
    this.moved = true;
    const bounds = selectionBounds(
      this.paper.clientToLocalPoint(this.start),
      this.paper.clientToLocalPoint({ x: event.clientX, y: event.clientY })
    );
    this.renderSelectionElement(bounds);
    this.cdr.detectChanges();
  };
  private up = (event: MouseEvent): void => {
    if (!this.start) return;
    const start = this.start;
    this.start = null;
    if (!this.moved) {
      if (event.button === 2) {
        const wrapper = this.actions.getJointGraphWrapper();
        const graph = this.actions.getTexeraGraph();
        const id = this.clickedID;
        if (id && graph.hasOperator(id) && !wrapper.getCurrentHighlightedOperatorIDs().includes(id))
          this.actions.highlightOperators(false, id);
        else if (id && graph.hasCommentBox(id) && !wrapper.getCurrentHighlightedCommentBoxIDs().includes(id))
          this.actions.highlightCommentBoxes(false, id);
        else if (id && graph.hasLinkWithID(id) && !wrapper.getCurrentHighlightedLinkIDs().includes(id))
          this.actions.highlightLinks(false, id);
        else if (!id) wrapper.unhighlightElements(wrapper.getCurrentHighlights());
        this.suppressContextUntil = Date.now() + 500;
        this.contextRequested.emit(event);
      }
      return;
    }
    this.suppressContextUntil = Date.now() + 500;
    const bounds = selectionBounds(
      this.paper.clientToLocalPoint(start),
      this.paper.clientToLocalPoint({ x: event.clientX, y: event.clientY })
    );
    const graph = this.actions.getTexeraGraph();
    const wrapper = this.actions.getJointGraphWrapper();
    if (!this.additive) wrapper.unhighlightElements(wrapper.getCurrentHighlights());
    const elements = this.paper.model.getElements().filter(element => {
      const id = element.id.toString();
      if (!graph.hasOperator(id) && !graph.hasCommentBox(id)) return false;
      const b = element.getBBox();
      // A frame enclosing the gesture must not swallow selection of operators inside it.
      if (graph.hasCommentBox(id) && graph.getCommentBox(id).overbox)
        return (
          b.x >= bounds.x &&
          b.y >= bounds.y &&
          b.x + b.width <= bounds.x + bounds.width &&
          b.y + b.height <= bounds.y + bounds.height
        );
      return intersectsSelection(bounds, b);
    });
    const ids = elements.map(e => e.id.toString());
    this.actions.highlightOperators(true, ...ids.filter(id => graph.hasOperator(id)));
    this.actions.highlightCommentBoxes(true, ...ids.filter(id => graph.hasCommentBox(id)));
    const operators = new Set(wrapper.getCurrentHighlightedOperatorIDs());
    this.actions.highlightLinks(
      true,
      ...graph
        .getAllLinks()
        .filter(link => operators.has(link.source.operatorID) && operators.has(link.target.operatorID))
        .map(link => link.linkID)
    );
    const rect = this.paper.el.getBoundingClientRect();
    const screenBounds = selectionBounds(start, { x: event.clientX, y: event.clientY });
    this.actionX = Math.max(8, Math.min(screenBounds.x - rect.left, rect.width - 260));
    this.actionY = Math.max(8, screenBounds.y - rect.top - 44);
    this.showActions = this.selectedCount > 0;
    if (!this.showActions) this.removeSelectionElement();
    this.cdr.detectChanges();
  };
  private contextMenu = (event: MouseEvent): void => {
    if ((this.start && this.moved) || Date.now() < this.suppressContextUntil) {
      event.preventDefault();
      event.stopImmediatePropagation();
    } else if (this.start) {
      // Some browsers fire contextmenu on press, before a secondary drag can begin.
      event.preventDefault();
      event.stopImmediatePropagation();
    }
  };
  private keydown = (event: KeyboardEvent): void => {
    if (event.key === "Escape") this.cancel();
  };
  private cancel = (): void => {
    this.start = null;
    this.removeSelectionElement();
    this.showActions = false;
    this.cdr.detectChanges();
  };
  dismiss(): void {
    this.removeSelectionElement();
    this.showActions = false;
  }

  private renderSelectionElement(bounds: SelectionBounds): void {
    if (!this.selectionElement) {
      this.selectionElement = document.createElementNS("http://www.w3.org/2000/svg", "rect");
      this.selectionElement.setAttribute("class", "canvas-selection-preview");
      this.selectionElement.setAttribute("fill", "#1677ff");
      this.selectionElement.setAttribute("fill-opacity", "0.1");
      this.selectionElement.setAttribute("stroke", "#1677ff");
      this.selectionElement.setAttribute("stroke-width", "1.25");
      this.selectionElement.setAttribute("stroke-dasharray", "5 4");
      this.selectionElement.setAttribute("pointer-events", "none");
      const animation = document.createElementNS("http://www.w3.org/2000/svg", "animate");
      animation.setAttribute("attributeName", "stroke-dashoffset");
      animation.setAttribute("from", "0");
      animation.setAttribute("to", "-18");
      animation.setAttribute("dur", "1s");
      animation.setAttribute("repeatCount", "indefinite");
      this.selectionElement.appendChild(animation);
      // The first viewport child is painted below JointJS cells and their tools.
      this.paper.viewport.insertBefore(this.selectionElement, this.paper.viewport.firstChild);
    }
    this.selectionElement.setAttribute("x", bounds.x.toString());
    this.selectionElement.setAttribute("y", bounds.y.toString());
    this.selectionElement.setAttribute("width", bounds.width.toString());
    this.selectionElement.setAttribute("height", bounds.height.toString());
  }

  private removeSelectionElement(): void {
    this.selectionElement?.remove();
    this.selectionElement = null;
  }
  copy(): void {
    this.menu.saveHighlightedElements();
    this.dismiss();
  }
  remove(): void {
    if (!this.canModify) return;
    const highlights = this.actions.getJointGraphWrapper().getCurrentHighlights();
    this.actions.getTexeraGraph().bundleActions(() => {
      this.actions.deleteOperatorsAndLinks([...highlights.operators]);
      [...highlights.links].forEach(id => {
        if (this.actions.getTexeraGraph().hasLinkWithID(id)) this.actions.deleteLinkWithID(id);
      });
      [...highlights.commentBoxes].forEach(id => this.actions.deleteCommentBox(id));
    });
    this.dismiss();
  }
}
