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

import { Component, EventEmitter, Input, NgZone, OnDestroy, OnInit, Output } from "@angular/core";
import { NgFor, NgIf } from "@angular/common";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { UdfContext } from "../../service/udf-copilot/udf-copilot.service";

@Component({
  selector: "texera-udf-context-panel",
  templateUrl: "./udf-context-panel.component.html",
  styleUrls: ["./udf-context-panel.component.scss"],
  imports: [NgFor, NgIf, NzIconDirective, NzButtonComponent],
})
export class UdfContextPanelComponent implements OnInit, OnDestroy {
  /** Pull-style context provider — parent owns the source of truth. */
  @Input() getContext!: () => UdfContext;
  @Output() closed = new EventEmitter<void>();

  context: UdfContext = {};
  private pollTimer?: number;

  constructor(private ngZone: NgZone) {}

  ngOnInit() {
    this.refresh();
    // Poll for context updates (sample row may arrive async after WS roundtrip).
    // Run outside Angular's zone so we don't churn change detection 4x per
    // second; we manually trigger CD via the assignment below.
    this.ngZone.runOutsideAngular(() => {
      this.pollTimer = window.setInterval(() => {
        this.ngZone.run(() => this.refresh());
      }, 1500);
    });
  }

  ngOnDestroy() {
    if (this.pollTimer !== undefined) {
      window.clearInterval(this.pollTimer);
    }
  }

  refresh() {
    try {
      this.context = this.getContext?.() ?? {};
    } catch {
      this.context = {};
    }
  }

  get hasSchema(): boolean {
    return (this.context.upstreamSchema?.length ?? 0) > 0;
  }

  get hasSample(): boolean {
    return !!this.context.sampleRow && Object.keys(this.context.sampleRow).length > 0;
  }

  sampleValueFor(col: string): string {
    const v = this.context.sampleRow?.[col];
    if (v === undefined || v === null) return "—";
    if (typeof v === "string") return v;
    try {
      return JSON.stringify(v);
    } catch {
      return String(v);
    }
  }

  close() {
    this.closed.emit();
  }
}
