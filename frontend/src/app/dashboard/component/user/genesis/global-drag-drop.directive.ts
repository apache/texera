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

import { DOCUMENT } from "@angular/common";
import { Directive, Inject, OnDestroy, Renderer2 } from "@angular/core";
import { Router } from "@angular/router";
import { NzMessageService } from "ng-zorro-antd/message";
import { GenesisOrchestratorService } from "../../../service/user/genesis/genesis-orchestrator.service";

@Directive({
  selector: "[texeraGlobalGenesisDragDrop]",
  standalone: true,
})
export class GlobalDragDropDirective implements OnDestroy {
  private overlayEl: HTMLElement | null = null;
  /** Element receiving `.genesis-active-dropzone` (workflow list container). */
  private dropHostEl: HTMLElement | null = null;

  private readonly captureOpts: AddEventListenerOptions = { capture: true };

  /** DOM `DataTransfer.types` may be a DOMStringList without `.includes`; use Array.from. */
  private static dataTransferHasFiles(dt: DataTransfer | null | undefined): boolean {
    if (!dt?.types) {
      return false;
    }
    const list = dt.types as unknown as string[] | DOMStringList;
    return Array.from(list as Iterable<string>).includes("Files");
  }

  private readonly onDragEnter = (ev: Event): void => {
    const e = ev as DragEvent;
    if (!this.isGenesisWorkflowListRoute()) {
      return;
    }
    const dt = e.dataTransfer;
    if (!dt || !GlobalDragDropDirective.dataTransferHasFiles(dt)) {
      return;
    }
    e.preventDefault();
    this.highlightWorkflowList();
  };

  private readonly onDragOver = (ev: Event): void => {
    const e = ev as DragEvent;
    if (!this.isGenesisWorkflowListRoute()) {
      return;
    }
    const dt = e.dataTransfer;
    if (!dt || !GlobalDragDropDirective.dataTransferHasFiles(dt)) {
      return;
    }
    e.preventDefault();
    dt.dropEffect = "copy";
    this.showDropOverlay();
  };

  private readonly onDrop = (ev: Event): void => {
    const e = ev as DragEvent;
    if (!this.isGenesisWorkflowListRoute()) {
      return;
    }
    this.hideDropOverlay();
    e.preventDefault();
    const file = e.dataTransfer?.files?.[0];
    if (!file) {
      return;
    }
    const name = file.name.toLowerCase();
    if (!name.endsWith(".csv") && !name.endsWith(".tsv")) {
      this.message.error("Only CSV or TSV files are supported.");
      return;
    }
    void this.genesisOrchestrator.run(file);
  };

  private readonly onWindowDragEnd = (): void => {
    this.hideDropOverlay();
  };

  constructor(
    private router: Router,
    private genesisOrchestrator: GenesisOrchestratorService,
    private message: NzMessageService,
    private renderer: Renderer2,
    @Inject(DOCUMENT) private document: Document
  ) {
    this.document.addEventListener("dragenter", this.onDragEnter, this.captureOpts);
    this.document.addEventListener("dragover", this.onDragOver, this.captureOpts);
    this.document.addEventListener("drop", this.onDrop, this.captureOpts);
    window.addEventListener("dragend", this.onWindowDragEnd, this.captureOpts);
  }

  ngOnDestroy(): void {
    this.document.removeEventListener("dragenter", this.onDragEnter, this.captureOpts);
    this.document.removeEventListener("dragover", this.onDragOver, this.captureOpts);
    this.document.removeEventListener("drop", this.onDrop, this.captureOpts);
    window.removeEventListener("dragend", this.onWindowDragEnd, this.captureOpts);
    this.hideDropOverlay();
  }

  /**
   * CSV Genesis drop target: workflow **list** page only (`/dashboard/user/workflow`),
   * not the editor (`/dashboard/user/workflow/:id`).
   */
  private isGenesisWorkflowListRoute(): boolean {
    const raw = this.router.url.split("?")[0];
    let path = raw === "" ? "/" : raw;
    if (path.length > 1) {
      path = path.replace(/\/+$/, "");
    }
    return path === "/dashboard/user/workflow";
  }

  private highlightWorkflowList(): void {
    const el = this.document.querySelector(".genesis-workflow-drop-host") as HTMLElement | null;
    if (!el || this.dropHostEl === el) {
      if (el && !this.dropHostEl) {
        this.dropHostEl = el;
        this.renderer.addClass(this.dropHostEl, "genesis-active-dropzone");
      }
      return;
    }
    this.clearListHighlight();
    this.dropHostEl = el;
    this.renderer.addClass(this.dropHostEl, "genesis-active-dropzone");
  }

  private clearListHighlight(): void {
    if (this.dropHostEl) {
      this.renderer.removeClass(this.dropHostEl, "genesis-active-dropzone");
      this.dropHostEl = null;
    }
  }

  private showDropOverlay(): void {
    this.highlightWorkflowList();
    if (this.overlayEl) {
      return;
    }
    const el = this.renderer.createElement("div");
    this.renderer.addClass(el, "texera-genesis-drop-overlay");
    const inner = this.renderer.createElement("div");
    this.renderer.addClass(inner, "texera-genesis-drop-overlay__inner");
    const text = this.renderer.createText("Drop your CSV here to start AI analysis");
    this.renderer.appendChild(inner, text);
    this.renderer.appendChild(el, inner);
    this.renderer.appendChild(this.document.body, el);
    this.overlayEl = el;
  }

  private hideDropOverlay(): void {
    this.clearListHighlight();
    if (this.overlayEl) {
      this.renderer.removeChild(this.document.body, this.overlayEl);
      this.overlayEl = null;
    }
  }
}
