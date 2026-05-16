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

import { Component, Input, OnChanges, OnInit, SimpleChanges } from "@angular/core";
import { NgFor, NgIf } from "@angular/common";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzTabComponent, NzTabsComponent } from "ng-zorro-antd/tabs";
import { NzTooltipModule } from "ng-zorro-antd/tooltip";
import { NzMessageService } from "ng-zorro-antd/message";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DataProfilingService } from "./data-profiling.service";
import {
  CleaningSuggestion,
  ColumnProfile,
  ColumnRole,
  ColumnRoleKind,
  DatasetProfile,
  QualityScoreBreakdown,
} from "./data-profiling.types";
import {
  computeQualityScore,
  detectColumnRoles,
  generateSuggestions,
  qualityScoreColor,
  qualityScoreLabel,
  roleBadge,
  suggestionToOperatorHint,
} from "./data-profiling.utils";

interface RoleSummaryEntry {
  kind: ColumnRoleKind;
  icon: string;
  label: string;
  color: string;
  columns: string[];
}

@UntilDestroy()
@Component({
  selector: "texera-data-profiling-panel",
  standalone: true,
  templateUrl: "./data-profiling-panel.component.html",
  styleUrls: ["./data-profiling-panel.component.scss"],
  imports: [
    NgFor,
    NgIf,
    NzIconDirective,
    NzButtonComponent,
    NzTabsComponent,
    NzTabComponent,
    NzTooltipModule,
  ],
})
export class DataProfilingPanelComponent implements OnInit, OnChanges {
  @Input() source: string = "diabetes.csv";

  profile?: DatasetProfile;
  loading = true;
  score?: QualityScoreBreakdown;
  suggestions: CleaningSuggestion[] = [];
  roles: ColumnRole[] = [];
  rolesByColumn: Record<string, ColumnRole> = {};
  roleSummary: RoleSummaryEntry[] = [];

  constructor(
    private profilingService: DataProfilingService,
    private message: NzMessageService
  ) {}

  ngOnInit(): void {
    this.loadProfile();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["source"] && !changes["source"].firstChange) {
      this.loadProfile();
    }
  }

  private loadProfile(): void {
    this.loading = true;
    this.profilingService
      .getProfile(this.source)
      .pipe(untilDestroyed(this))
      .subscribe(profile => {
        this.profile = profile;
        this.score = computeQualityScore(profile);
        this.suggestions = generateSuggestions(profile);
        this.roles = detectColumnRoles(profile);
        this.rolesByColumn = Object.fromEntries(this.roles.map(r => [r.column, r]));
        this.roleSummary = this.summarizeRoles(this.roles);
        this.loading = false;
      });
  }

  private summarizeRoles(roles: ColumnRole[]): RoleSummaryEntry[] {
    const order: ColumnRoleKind[] = [
      "target",
      "possible_target",
      "id",
      "datetime",
      "feature",
      "constant",
    ];
    const grouped = new Map<ColumnRoleKind, string[]>();
    for (const r of roles) {
      if (!grouped.has(r.role)) grouped.set(r.role, []);
      grouped.get(r.role)!.push(r.column);
    }
    return order
      .filter(k => grouped.has(k))
      .map(k => {
        const badge = roleBadge(k);
        return {
          kind: k,
          icon: badge.icon,
          label: badge.label,
          color: badge.color,
          columns: grouped.get(k)!,
        };
      });
  }

  // --- Quality Score helpers ---
  scoreColor(): string {
    return this.score ? qualityScoreColor(this.score.band) : "#bfbfbf";
  }
  scoreLabel(): string {
    return this.score ? qualityScoreLabel(this.score.band) : "";
  }

  // --- Suggestions ---
  severityBg(severity: CleaningSuggestion["severity"]): string {
    switch (severity) {
      case "critical":
        return "#fff1f0";
      case "warning":
        return "#fffbe6";
      case "info":
        return "#e6f4ff";
    }
  }
  severityBorder(severity: CleaningSuggestion["severity"]): string {
    switch (severity) {
      case "critical":
        return "#ffa39e";
      case "warning":
        return "#ffe58f";
      case "info":
        return "#91caff";
    }
  }
  severityDot(severity: CleaningSuggestion["severity"]): string {
    switch (severity) {
      case "critical":
        return "🔴";
      case "warning":
        return "🟡";
      case "info":
        return "🔵";
    }
  }

  addToWorkflow(s: CleaningSuggestion): void {
    const hint = suggestionToOperatorHint(s);
    // Agent integration is a follow-up — copy the hint so the user can paste into the agent.
    if (typeof navigator !== "undefined" && navigator.clipboard) {
      navigator.clipboard.writeText(hint).catch(() => {});
    }
    const verb = s.action === "review_outliers" ? "Copied" : "✅ Copied";
    this.message.success(`${verb}: ${hint}`);
  }

  // --- Column helpers ---
  roleFor(col: ColumnProfile): ColumnRole | undefined {
    return this.rolesByColumn[col.name];
  }
  roleBadgeOf(col: ColumnProfile) {
    const r = this.roleFor(col);
    return r ? roleBadge(r.role) : roleBadge("feature");
  }
  isDropRole(role: ColumnRoleKind): boolean {
    return role === "id" || role === "constant";
  }

  histogramBars(col: ColumnProfile): { height: number }[] {
    if (!col.histogram || col.histogram.length === 0) return [];
    const max = Math.max(...col.histogram, 1);
    return col.histogram.map(v => ({ height: Math.round((v / max) * 100) }));
  }

  topValueShare(col: ColumnProfile): number {
    if (!col.topValues || col.topValues.length === 0 || !col.count) return 0;
    return Math.round((col.topValues[0].count / col.count) * 100);
  }

  isImbalanced(col: ColumnProfile): boolean {
    return col.dtype === "categorical" && this.topValueShare(col) > 90 && col.unique > 1;
  }

  formatNumber(value: number | undefined, digits: number = 2): string {
    if (value === undefined || value === null || Number.isNaN(value)) return "—";
    if (Math.abs(value) >= 1000) return value.toLocaleString();
    return Number(value.toFixed(digits)).toString();
  }
}
