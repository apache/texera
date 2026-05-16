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

import { ChangeDetectorRef, Component, HostListener, OnDestroy, OnInit } from "@angular/core";
import { CdkDrag, CdkDragHandle } from "@angular/cdk/drag-drop";
import { DatePipe, NgClass, NgIf } from "@angular/common";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { MarkdownComponent } from "ngx-markdown";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzEmptyModule } from "ng-zorro-antd/empty";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzMenuDirective, NzMenuItemComponent } from "ng-zorro-antd/menu";
import { NzResizableDirective, NzResizeEvent, NzResizeHandlesComponent } from "ng-zorro-antd/resizable";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { AgentReport, AgentReportService } from "../../service/agent/agent-report.service";

const MIN_WIDTH = 400;
const MIN_HEIGHT = 400;
const DEFAULT_WIDTH = 520;

/**
 * Floating right-side panel that displays the most recent agent-generated
 * analysis report (model comparison tables, key metrics, recommendations).
 *
 * The panel collapses to a docked button when `width === 0`. Reports are
 * pushed in by the agent chat component via {@link AgentReportService}; the
 * panel auto-opens when a new report arrives and when the user clicks
 * "View Report" from the chat card.
 */
@UntilDestroy()
@Component({
  selector: "texera-results-dashboard-panel",
  templateUrl: "results-dashboard-panel.component.html",
  styleUrls: ["results-dashboard-panel.component.scss"],
  imports: [
    NgIf,
    NgClass,
    DatePipe,
    CdkDrag,
    CdkDragHandle,
    MarkdownComponent,
    NzButtonComponent,
    NzEmptyModule,
    NzIconDirective,
    NzMenuDirective,
    NzMenuItemComponent,
    NzResizableDirective,
    NzResizeHandlesComponent,
    NzTooltipDirective,
  ],
})
export class ResultsDashboardPanelComponent implements OnInit, OnDestroy {
  protected readonly window = window;

  width = 0;
  height = Math.max(MIN_HEIGHT, Math.floor(window.innerHeight * 0.6));
  dragPosition = { x: 0, y: 0 };
  resizeRaf = -1;

  report: AgentReport | null = null;

  constructor(
    private agentReportService: AgentReportService,
    private notificationService: NotificationService,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.report = this.agentReportService.snapshot;

    this.agentReportService.currentReport$.pipe(untilDestroyed(this)).subscribe(report => {
      const isNew = !!report && (!this.report || report.sourceId !== this.report.sourceId);
      this.report = report;
      // Auto-open when a fresh report arrives and the panel is collapsed.
      if (isNew && this.width === 0) {
        this.openPanel();
      }
      this.cdr.markForCheck();
    });

    this.agentReportService.openRequests$.pipe(untilDestroyed(this)).subscribe(() => {
      if (this.width === 0) this.openPanel();
    });
  }

  @HostListener("window:beforeunload")
  ngOnDestroy(): void {
    cancelAnimationFrame(this.resizeRaf);
  }

  openPanel(): void {
    this.width = this.width === 0 ? DEFAULT_WIDTH : 0;
  }

  onResize({ width, height }: NzResizeEvent): void {
    cancelAnimationFrame(this.resizeRaf);
    this.resizeRaf = requestAnimationFrame(() => {
      if (width) this.width = width;
      if (height) this.height = height;
    });
  }

  async copyMarkdown(): Promise<void> {
    if (!this.report) return;
    try {
      await navigator.clipboard.writeText(this.report.markdown);
      this.notificationService.success("Report copied to clipboard");
    } catch {
      this.notificationService.error("Failed to copy report");
    }
  }

  exportMarkdown(): void {
    if (!this.report) return;
    const blob = new Blob([this.report.markdown], { type: "text/markdown;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    const ts = new Date(this.report.timestamp).toISOString().replace(/[:.]/g, "-");
    a.href = url;
    a.download = `agent-report-${ts}.md`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }
}
