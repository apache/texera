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

import { ChangeDetectorRef, Component, OnInit } from "@angular/core";
import { NgFor, NgIf } from "@angular/common";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { firstValueFrom, merge } from "rxjs";
import { debounceTime, filter, take } from "rxjs/operators";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import {
  PYTHON_UDF_V2_OP_TYPE,
  WorkflowGraphReadonly,
} from "../../service/workflow-graph/model/workflow-graph";
import { OperatorPredicate } from "../../types/workflow-common.interface";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { IndexableObject } from "../../types/result-table.interface";

const INSIGHT_NAME_SUBSTRINGS = ["insight", "summary"];

const MID_KEY_ORDER = ["top_predictors", "top_associations", "interpretation", "next_steps"] as const;

type FetchInsightOutcome =
  | { kind: "row"; row: IndexableObject }
  | { kind: "empty" }
  | { kind: "none" };

export interface AiInsightSectionView {
  label: string;
  emoji: string;
  text?: string;
  bullets?: string[];
}

@UntilDestroy()
@Component({
  selector: "texera-ai-insight-panel",
  templateUrl: "./ai-insight-panel.component.html",
  styleUrls: ["./ai-insight-panel.component.scss"],
  imports: [NgIf, NgFor],
})
export class AiInsightPanelComponent implements OnInit {
  hasInsightOperatorInWorkflow = false;
  /** When true, show “Run the workflow…” (no finished run / no rows yet). */
  waitingForRun = true;
  sections: AiInsightSectionView[] = [];
  collapsed = false;

  constructor(
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowActionService: WorkflowActionService,
    private workflowResultService: WorkflowResultService,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    merge(
      this.executeWorkflowService.getExecutionStateStream(),
      this.workflowResultService.getResultUpdateStream(),
      this.workflowActionService.workflowChanged()
    )
      .pipe(debounceTime(150), untilDestroyed(this))
      .subscribe(() => {
        void this.refresh();
      });

    void this.refresh();
  }

  toggleCollapsed(): void {
    this.collapsed = !this.collapsed;
  }

  private async refresh(): Promise<void> {
    const graph = this.workflowActionService.getTexeraGraph();
    const insightOpId = this.resolveInsightOperatorId(graph);
    this.hasInsightOperatorInWorkflow = insightOpId !== undefined;

    if (!insightOpId) {
      this.waitingForRun = false;
      this.sections = [];
      this.cdr.markForCheck();
      return;
    }

    const exec = this.executeWorkflowService.getExecutionState();
    const finishedSuccess =
      exec.state === ExecutionState.Completed || exec.state === ExecutionState.Terminated;

    if (!finishedSuccess) {
      this.waitingForRun = true;
      this.sections = [];
      this.cdr.markForCheck();
      return;
    }

    const outcome = await this.fetchInsightOutcome(insightOpId);
    if (outcome.kind === "none") {
      this.waitingForRun = true;
      this.sections = [];
      this.cdr.markForCheck();
      return;
    }
    if (outcome.kind === "empty") {
      this.waitingForRun = false;
      this.sections = [];
      this.cdr.markForCheck();
      return;
    }

    this.waitingForRun = false;
    this.sections = this.buildSections(outcome.row);
    this.cdr.markForCheck();
  }

  private resolveInsightOperatorId(graph: WorkflowGraphReadonly): string | undefined {
    const candidates = graph
      .getAllOperators()
      .filter(op => !op.isDisabled)
      .filter(op => op.operatorType === PYTHON_UDF_V2_OP_TYPE)
      .filter(op => this.isInsightNamedOperator(op));

    if (candidates.length === 0) {
      return undefined;
    }

    candidates.sort((a, b) => {
      const la = this.isLeafOperator(graph, a.operatorID) ? 0 : 1;
      const lb = this.isLeafOperator(graph, b.operatorID) ? 0 : 1;
      if (la !== lb) {
        return la - lb;
      }
      return a.operatorID.localeCompare(b.operatorID);
    });

    return candidates[0]?.operatorID;
  }

  private isInsightNamedOperator(op: OperatorPredicate): boolean {
    const name = (op.customDisplayName ?? "").toLowerCase();
    return INSIGHT_NAME_SUBSTRINGS.some(s => name.includes(s));
  }

  private isLeafOperator(graph: WorkflowGraphReadonly, operatorID: string): boolean {
    return (
      graph
        .getOutputLinksByOperatorId(operatorID)
        .filter(link => graph.isLinkEnabled(link.linkID)).length === 0
    );
  }

  private async fetchInsightOutcome(operatorID: string): Promise<FetchInsightOutcome> {
    const paginated = this.workflowResultService.getPaginatedResultService(operatorID);
    if (paginated) {
      try {
        const page = await firstValueFrom(
          paginated.selectPage(1, 40).pipe(
            filter(ev => Array.isArray(ev.table)),
            take(1)
          )
        );
        if (page.table.length === 0) {
          return { kind: "empty" };
        }
        return { kind: "row", row: page.table[0] as IndexableObject };
      } catch {
        return { kind: "none" };
      }
    }

    const snap = this.workflowResultService.getResultService(operatorID)?.getCurrentResultSnapshot();
    if (snap === undefined) {
      return { kind: "none" };
    }
    if (snap.length === 0) {
      return { kind: "empty" };
    }
    return { kind: "row", row: snap[0] as IndexableObject };
  }

  private buildSections(row: IndexableObject): AiInsightSectionView[] {
    const keys = this.sortInsightKeys(Object.keys(row));
    const sections: AiInsightSectionView[] = [];
    for (const key of keys) {
      const raw = row[key];
      const text = this.cellToDisplayString(raw);
      if (text === null) {
        continue;
      }
      const emoji = this.emojiForKey(key);
      const label = this.formatKeyLabel(key);
      const bullets = this.maybeBulletsForKey(key, text);
      sections.push(
        bullets
          ? { label, emoji, bullets }
          : {
              label,
              emoji,
              text,
            }
      );
    }
    return sections;
  }

  private sortInsightKeys(keys: string[]): string[] {
    const ordered: string[] = [];
    const use = (k: string) => {
      if (keys.includes(k) && !ordered.includes(k)) {
        ordered.push(k);
      }
    };
    use("summary");
    for (const k of MID_KEY_ORDER) {
      use(k);
    }
    const rest = keys
      .filter(k => k !== "summary" && k !== "caveat" && !ordered.includes(k))
      .sort((a, b) => a.localeCompare(b));
    ordered.push(...rest);
    use("caveat");
    return ordered;
  }

  private emojiForKey(key: string): string {
    if (key === "summary") {
      return "📊";
    }
    if (key === "top_predictors" || key === "top_associations") {
      return "🎯";
    }
    if (key === "interpretation") {
      return "💡";
    }
    if (key === "next_steps") {
      return "✅";
    }
    if (key === "caveat") {
      return "⚠️";
    }
    return "📝";
  }

  private formatKeyLabel(key: string): string {
    return key.replace(/_/g, " ").toUpperCase();
  }

  private cellToDisplayString(value: unknown): string | null {
    if (value === null || value === undefined) {
      return null;
    }
    if (typeof value === "string") {
      const t = value.trim();
      return t === "" ? null : t;
    }
    if (typeof value === "number" || typeof value === "boolean") {
      return String(value);
    }
    if (typeof value === "object") {
      try {
        return JSON.stringify(value);
      } catch {
        return String(value);
      }
    }
    return String(value);
  }

  /** Comma-separated predictor lists → bullet lines; otherwise single paragraph. */
  private maybeBulletsForKey(key: string, text: string): string[] | undefined {
    if (key !== "top_predictors" && key !== "top_associations") {
      return undefined;
    }
    if (!text.includes(",")) {
      return undefined;
    }
    const parts = text
      .split(",")
      .map(p => p.trim())
      .filter(Boolean);
    return parts.length > 1 ? parts : undefined;
  }
}
