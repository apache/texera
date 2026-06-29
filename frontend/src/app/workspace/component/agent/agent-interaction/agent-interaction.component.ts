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

import { ChangeDetectorRef, Component, Input, OnChanges, OnInit, SimpleChanges } from "@angular/core";
import { DomSanitizer, SafeHtml } from "@angular/platform-browser";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { AgentService } from "../../../service/agent/agent.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { NgIf, NgFor } from "@angular/common";
import {
  NzTheadComponent,
  NzTrDirective,
  NzTableCellDirective,
  NzThMeasureDirective,
  NzTbodyComponent,
} from "ng-zorro-antd/table";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzSelectComponent, NzOptionComponent } from "ng-zorro-antd/select";
import { FormsModule } from "@angular/forms";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzInputDirective, NzAutosizeDirective } from "ng-zorro-antd/input";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { SampleRow } from "../../../service/agent/agent.service";

/**
 * AgentInteractionComponent provides a compact interface for users to send feedback
 * or messages to agents regarding a specific operator.
 * It consists of an agent dropdown and a text input area.
 */
@UntilDestroy()
@Component({
  selector: "texera-agent-interaction",
  templateUrl: "./agent-interaction.component.html",
  styleUrls: ["./agent-interaction.component.scss"],
  imports: [
    NgIf,
    NzTheadComponent,
    NzTrDirective,
    NgFor,
    NzTableCellDirective,
    NzThMeasureDirective,
    NzTbodyComponent,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzSpaceCompactItemDirective,
    NzSelectComponent,
    FormsModule,
    NzOptionComponent,
    NzTooltipDirective,
    NzInputDirective,
    NzAutosizeDirective,
    NzButtonComponent,
    NzWaveDirective,
  ],
})
export class AgentInteractionComponent implements OnInit, OnChanges {
  @Input() operatorId!: string;
  @Input() operatorDisplayName?: string;
  @Input() sampleTuples?: SampleRow[];

  public availableAgents: Array<{ id: string; name: string; isConnected: boolean }> = [];
  public selectedAgentId: string | null = null;
  public feedbackMessage: string = "";

  // Cached visualization HTML to avoid iframe re-render on every WS update
  private cachedVisualizationHtml: SafeHtml | null = null;
  private cachedVisualizationRawHtml: string | null = null;

  constructor(
    private agentService: AgentService,
    private workflowActionService: WorkflowActionService,
    private notificationService: NotificationService,
    private changeDetectorRef: ChangeDetectorRef,
    private sanitizer: DomSanitizer
  ) {}

  ngOnInit(): void {
    this.loadAvailableAgents();
    this.agentService.agentChange$.pipe(untilDestroyed(this)).subscribe(() => {
      this.loadAvailableAgents();
    });
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["sampleTuples"]) {
      // Only update cached visualization HTML when the actual content changes
      const newRows = changes["sampleTuples"].currentValue as SampleRow[] | undefined;
      const htmlContent = newRows?.[0]?.tuple["html-content"];
      const newHtml = typeof htmlContent === "string" ? htmlContent : null;
      if (newHtml !== this.cachedVisualizationRawHtml) {
        this.cachedVisualizationRawHtml = newHtml;
        this.cachedVisualizationHtml = newHtml ? this.sanitizer.bypassSecurityTrustHtml(newHtml) : null;
      }
    }
  }

  private loadAvailableAgents(): void {
    this.agentService
      .getAllAgents()
      .pipe(untilDestroyed(this))
      .subscribe(agents => {
        const connectedAgentIds = new Set(this.agentService.getActivelyConnectedAgentIds());

        this.availableAgents = agents.map(agent => ({
          id: agent.id,
          name: agent.name,
          isConnected: connectedAgentIds.has(agent.id),
        }));

        // Auto-select: prefer connected agent, then first agent if only one
        const connectedAgent = this.availableAgents.find(a => a.isConnected);
        if (connectedAgent) {
          this.selectedAgentId = connectedAgent.id;
        } else if (this.availableAgents.length === 1) {
          this.selectedAgentId = this.availableAgents[0].id;
        }

        this.changeDetectorRef.detectChanges();
      });
  }

  public isSelectedAgentConnected(): boolean {
    if (!this.selectedAgentId) return false;
    return this.agentService.isAgentActivelyConnected(this.selectedAgentId);
  }

  public sendFeedbackToAgent(): void {
    if (!this.selectedAgentId || !this.feedbackMessage.trim() || !this.operatorId) {
      return;
    }

    if (!this.isSelectedAgentConnected()) {
      this.notificationService.error("Agent is not connected. Please open the agent chat panel first.");
      return;
    }

    const agentId = this.selectedAgentId;
    const operatorName = this.operatorDisplayName || this.getOperatorName() || "this operator";
    const contextMessage = `Regarding operator "${operatorName}" (ID: ${this.operatorId}): ${this.feedbackMessage.trim()}`;

    this.agentService.sendMessage(agentId, contextMessage, "feedback");
    this.notificationService.success("Message sent to agent successfully");
    this.feedbackMessage = "";
    this.changeDetectorRef.detectChanges();
  }

  private getOperatorName(): string | undefined {
    try {
      const operator = this.workflowActionService.getTexeraGraph().getOperator(this.operatorId);
      return operator?.customDisplayName || undefined;
    } catch {
      return undefined;
    }
  }

  public canSend(): boolean {
    return !!this.selectedAgentId && !!this.feedbackMessage.trim();
  }

  /**
   * Check if sample rows represent a visualization (has __is_visualization__ flag).
   */
  public isVisualization(): boolean {
    if (!this.sampleTuples || this.sampleTuples.length === 0) return false;
    return this.sampleTuples[0].tuple["__is_visualization__"] === true;
  }

  /**
   * Get the cached sanitized HTML content from a visualization record for iframe srcdoc.
   */
  public getVisualizationHtml(): SafeHtml {
    return this.cachedVisualizationHtml || this.sanitizer.bypassSecurityTrustHtml("");
  }

  /**
   * Get visible column names from sample rows.
   */
  public getSampleColumns(): string[] {
    if (!this.sampleTuples || this.sampleTuples.length === 0) return [];
    return Object.keys(this.sampleTuples[0].tuple).filter(k => k !== "__is_visualization__");
  }

  /**
   * Get display name for a column header.
   */
  public getColumnDisplayName(col: string): string {
    return col;
  }

  public getDisplayRows(): Array<{ row?: SampleRow; isEllipsis: boolean }> {
    if (!this.sampleTuples || this.sampleTuples.length === 0) return [];

    const rows: Array<{ row?: SampleRow; isEllipsis: boolean }> = [];
    for (let i = 0; i < this.sampleTuples.length; i++) {
      if (i > 0) {
        const prevIdx = this.sampleTuples[i - 1].rowIndex;
        const currIdx = this.sampleTuples[i].rowIndex;
        if (currIdx - prevIdx > 1) {
          rows.push({ isEllipsis: true });
        }
      }
      rows.push({ row: this.sampleTuples[i], isEllipsis: false });
    }
    return rows;
  }
}
