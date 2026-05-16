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

  private readonly captureOpts: AddEventListenerOptions = { capture: true };

  /** DOM `DataTransfer.types` may be a DOMStringList without `.includes`; use Array.from. */
  private static dataTransferHasFiles(dt: DataTransfer | null | undefined): boolean {
    if (!dt?.types) {
      return false;
    }
    const list = dt.types as unknown as string[] | DOMStringList;
    return Array.from(list as Iterable<string>).includes("Files");
  }

  private readonly onDragOver = (ev: Event): void => {
    const e = ev as DragEvent;
    console.log("[Genesis] dragover fired, url=", this.router.url);
    if (!this.isGenesisRoute()) {
      return;
    }
    const dt = e.dataTransfer;
    if (!dt || !GlobalDragDropDirective.dataTransferHasFiles(dt)) {
      console.log("[Genesis] dragover ignored (no file payload in dataTransfer.types)");
      return;
    }
    e.preventDefault();
    dt.dropEffect = "copy";
    this.showDropOverlay();
  };

  private readonly onDrop = (ev: Event): void => {
    const e = ev as DragEvent;
    console.log("[Genesis] drop fired");
    if (!this.isGenesisRoute()) {
      return;
    }
    this.hideDropOverlay();
    e.preventDefault();
    const file = e.dataTransfer?.files?.[0];
    if (!file) {
      console.log("[Genesis] drop: no file in dataTransfer.files");
      return;
    }
    const name = file.name.toLowerCase();
    if (!name.endsWith(".csv") && !name.endsWith(".tsv")) {
      this.message.error("Only CSV or TSV files are supported.");
      return;
    }
    void this.genesisOrchestrator.run(file);
  };

  private readonly onWindowDragLeave = (ev: Event): void => {
    const e = ev as DragEvent;
    if (!this.isGenesisRoute() || !this.overlayEl) {
      return;
    }
    if (e.clientX === 0 && e.clientY === 0) {
      this.hideDropOverlay();
    }
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
    console.log("[Genesis] Directive instantiated, url=", this.router.url);

    this.document.addEventListener("dragover", this.onDragOver, this.captureOpts);
    this.document.addEventListener("drop", this.onDrop, this.captureOpts);
    window.addEventListener("dragleave", this.onWindowDragLeave, this.captureOpts);
    window.addEventListener("dragend", this.onWindowDragEnd, this.captureOpts);
  }

  ngOnDestroy(): void {
    this.document.removeEventListener("dragover", this.onDragOver, this.captureOpts);
    this.document.removeEventListener("drop", this.onDrop, this.captureOpts);
    window.removeEventListener("dragleave", this.onWindowDragLeave, this.captureOpts);
    window.removeEventListener("dragend", this.onWindowDragEnd, this.captureOpts);
    this.hideDropOverlay();
  }

  private isGenesisRoute(): boolean {
    const raw = this.router.url.split("?")[0];
    let path = raw === "" ? "/" : raw;
    if (path.length > 1) {
      path = path.replace(/\/+$/, "");
    }
    if (path === "") {
      path = "/";
    }

    let result: boolean;
    if (path === "/") {
      result = true;
    } else if (path === "/dashboard" || path.startsWith("/dashboard/home")) {
      result = true;
    } else {
      result = /^\/dashboard\/user\/workflow(\/\d+)?$/.test(path);
    }
    console.log("[Genesis] isGenesisRoute check:", path, "→", result);
    return result;
  }

  private showDropOverlay(): void {
    if (this.overlayEl) {
      return;
    }
    const el = this.renderer.createElement("div");
    this.renderer.addClass(el, "texera-genesis-drop-overlay");
    const inner = this.renderer.createElement("div");
    this.renderer.addClass(inner, "texera-genesis-drop-overlay__inner");
    const text = this.renderer.createText("🧬 Drop CSV to analyze");
    this.renderer.appendChild(inner, text);
    this.renderer.appendChild(el, inner);
    this.renderer.appendChild(this.document.body, el);
    this.overlayEl = el;
  }

  private hideDropOverlay(): void {
    if (this.overlayEl) {
      this.renderer.removeChild(this.document.body, this.overlayEl);
      this.overlayEl = null;
    }
  }
}
