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
  standalone: false,
})
export class AgentInteractionComponent implements OnInit, OnChanges {
  @Input() operatorId!: string;
  @Input() operatorDisplayName?: string;
  @Input() sampleRecords?: Record<string, any>[];
  @Input() resultStatistics?: Record<string, string>;

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
    if (changes["sampleRecords"]) {
      // Only update cached visualization HTML when the actual content changes
      const newRecords = changes["sampleRecords"].currentValue as Record<string, any>[] | undefined;
      const newHtml = newRecords?.[0]?.["html-content"] || null;
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
   * Check if sample records represent a visualization (has __is_visualization__ flag).
   */
  public isVisualization(): boolean {
    if (!this.sampleRecords || this.sampleRecords.length === 0) return false;
    return this.sampleRecords[0]["__is_visualization__"] === true;
  }

  /**
   * Get the cached sanitized HTML content from a visualization record for iframe srcdoc.
   */
  public getVisualizationHtml(): SafeHtml {
    return this.cachedVisualizationHtml || this.sanitizer.bypassSecurityTrustHtml("");
  }

  /**
   * Get column names from sample records, placing __row_index__ first (displayed as "Row").
   */
  public getSampleColumns(): string[] {
    if (!this.sampleRecords || this.sampleRecords.length === 0) return [];
    const allKeys = Object.keys(this.sampleRecords[0]);
    const rowIndexKey = allKeys.find(k => k.startsWith("_") && k.includes("row_index"));
    const otherKeys = allKeys.filter(k => k !== rowIndexKey);
    return rowIndexKey ? [rowIndexKey, ...otherKeys] : otherKeys;
  }

  /**
   * Get display name for a column header.
   */
  public getColumnDisplayName(col: string): string {
    if (col.startsWith("_") && col.includes("row_index")) return "Row";
    return col;
  }

  /**
   * Parse resultStatistics into displayable column stats.
   * Each entry in resultStatistics is a JSON string with { data_type, statistics: { ... } }.
   */
  public getParsedColumnStats(): Array<{
    column: string;
    dataType: string;
    stats: Array<{ key: string; value: string }>;
  }> {
    if (!this.resultStatistics) return [];
    const sampleCols = this.getSampleColumns().filter(c => !c.startsWith("_") || !c.includes("row_index"));
    const columns = sampleCols.length > 0 ? sampleCols : Object.keys(this.resultStatistics);
    const result: Array<{ column: string; dataType: string; stats: Array<{ key: string; value: string }> }> = [];
    const excludedKeys = new Set(["count", "std", "p25", "median", "p75"]);

    for (const colName of columns) {
      const statsJson = this.resultStatistics[colName];
      if (!statsJson) continue;
      try {
        const parsed = JSON.parse(statsJson);
        const dataType: string = parsed.data_type ?? "unknown";
        const statistics: Record<string, any> = parsed.statistics ?? {};
        const statEntries: Array<{ key: string; value: string }> = [];

        for (const [key, value] of Object.entries(statistics)) {
          if (value === undefined || excludedKeys.has(key)) continue;
          if (key === "top_10" && typeof value === "object") {
            const topEntries = Object.entries(value as Record<string, any>)
              .slice(0, 5)
              .map(([k, v]) => `${k}: ${v}`)
              .join(", ");
            statEntries.push({ key: "top values", value: topEntries });
          } else if (value === null || String(value) === "null") {
            statEntries.push({ key, value: "NaN" });
          } else if (typeof value !== "object") {
            const formatted =
              typeof value === "number" && !Number.isInteger(value)
                ? Number(value.toPrecision(4)).toString()
                : String(value);
            statEntries.push({ key, value: formatted });
          }
        }
        result.push({ column: colName, dataType, stats: statEntries });
      } catch {
        // skip unparseable
      }
    }
    return result;
  }

  public hasColumnStats(): boolean {
    return this.getParsedColumnStats().length > 0;
  }

  public getDisplayRows(): Array<{ record?: Record<string, any>; isEllipsis: boolean }> {
    if (!this.sampleRecords || this.sampleRecords.length === 0) return [];
    const rowIndexKey = Object.keys(this.sampleRecords[0]).find(k => k.startsWith("_") && k.includes("row_index"));
    if (!rowIndexKey) {
      return this.sampleRecords.map(r => ({ record: r, isEllipsis: false }));
    }

    const rows: Array<{ record?: Record<string, any>; isEllipsis: boolean }> = [];
    for (let i = 0; i < this.sampleRecords.length; i++) {
      if (i > 0) {
        const prevIdx = this.sampleRecords[i - 1][rowIndexKey];
        const currIdx = this.sampleRecords[i][rowIndexKey];
        if (typeof prevIdx === "number" && typeof currIdx === "number" && currIdx - prevIdx > 1) {
          rows.push({ isEllipsis: true });
        }
      }
      rows.push({ record: this.sampleRecords[i], isEllipsis: false });
    }
    return rows;
  }
}
