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

import { CommonModule } from "@angular/common";
import { Component, OnInit } from "@angular/core";
import { ActivatedRoute } from "@angular/router";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { forkJoin } from "rxjs";
import {
  ExecutionOperatorResultPage,
  OperatorPortCompareResult,
  WorkflowExecutionCompareSummary,
  WorkflowExecutionsService,
} from "../../../dashboard/service/user/workflow-executions/workflow-executions.service";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { CompareDagComponent, OperatorDiffStatus } from "./compare-dag/compare-dag.component";

/**
 * One row's worth of side-by-side data. `kind` controls the visual treatment:
 *   - `same`    both sides identical → no tint
 *   - `changed` both rows exist but differ on at least one cell → yellow tint,
 *               cells that differ get the per-cell red highlight
 *   - `onlyA`   row exists in A, B is padding → red tint on A, "—" on B
 *   - `onlyB`   row exists in B, A is padding → green tint on B, "—" on A
 */
type RowKind = "same" | "changed" | "onlyA" | "onlyB";

interface DiffCell {
  readonly value: string;
  readonly differs: boolean;
  readonly missing: boolean;
}

interface DiffRow {
  readonly kind: RowKind;
  readonly cellsA: ReadonlyArray<DiffCell>;
  readonly cellsB: ReadonlyArray<DiffCell>;
}

@UntilDestroy()
@Component({
  selector: "texera-compare-workspace",
  standalone: true,
  imports: [CommonModule, CompareDagComponent],
  templateUrl: "./compare-workspace.component.html",
  styleUrls: ["./compare-workspace.component.scss"],
})
export class CompareWorkspaceComponent implements OnInit {
  wid = 0;
  eidA = 0;
  eidB = 0;

  loading = true;
  loadError: string | null = null;
  summary: WorkflowExecutionCompareSummary | null = null;

  // Workflow content for each side's version, fed into the DAG canvases.
  // The WorkflowVersionService parses content from string into a structured object, so we
  // accept `any` here and let the DAG component handle either shape.
  contentA: any = null;
  contentB: any = null;
  // operatorId → combined diff status (presence + properties + output). Single map
  // shared by both DAGs so the labels and colors agree on each side. Onlyin-A status
  // visibly renders only on side A's DAG (and vice versa) since that operator isn't
  // present in the other side's content.
  diffStatusMap: ReadonlyMap<string, OperatorDiffStatus> = new Map();
  // Counts for the header summary chip.
  diffCounts = { identical: 0, propsDiffer: 0, outputDiffer: 0, onlyInA: 0, onlyInB: 0 };

  selected: OperatorPortCompareResult | null = null;
  // When set, the user clicked an operator that isn't in summary.operators — meaning
  // neither execution captured per-operator results for it. Used to show a hint in the
  // result panel instead of failing silently.
  clickedOperatorMissing: string | null = null;
  pageIndex = 0;
  pageSize = 25;
  pageA: ExecutionOperatorResultPage | null = null;
  pageB: ExecutionOperatorResultPage | null = null;
  pageLoadError: string | null = null;
  diffRows: ReadonlyArray<DiffRow> = [];
  unionColumnNames: ReadonlyArray<string> = [];
  // Row count summaries shown above each side's table.
  rowsOnA = 0;
  rowsOnB = 0;

  constructor(
    private route: ActivatedRoute,
    private executionsService: WorkflowExecutionsService,
    private workflowVersionService: WorkflowVersionService
  ) {}

  ngOnInit(): void {
    this.wid = Number(this.route.snapshot.paramMap.get("wid"));
    this.eidA = Number(this.route.snapshot.paramMap.get("eidA"));
    this.eidB = Number(this.route.snapshot.paramMap.get("eidB"));
    this.executionsService
      .compareTwoExecutions(this.wid, this.eidA, this.eidB)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: summary => {
          this.summary = summary;
          this.loadWorkflowContents(summary);
          this.loading = false;
          const firstShared = summary.operators.find(o => o.status === "shared") ?? summary.operators[0];
          if (firstShared) {
            this.selectOperator(firstShared);
          }
        },
        error: err => {
          this.loadError = err?.error?.message ?? err?.message ?? "Failed to load comparison";
          this.loading = false;
        },
      });
  }

  private loadWorkflowContents(summary: WorkflowExecutionCompareSummary): void {
    this.contentA = null;
    this.contentB = null;
    if (summary.vidA > 0) {
      this.workflowVersionService
        .retrieveWorkflowByVersion(summary.wid, summary.vidA)
        .pipe(untilDestroyed(this))
        .subscribe(wf => {
          this.contentA = wf?.content ?? null;
          this.recomputeDiffStatus();
        });
    }
    if (summary.vidB > 0) {
      this.workflowVersionService
        .retrieveWorkflowByVersion(summary.wid, summary.vidB)
        .pipe(untilDestroyed(this))
        .subscribe(wf => {
          this.contentB = wf?.content ?? null;
          this.recomputeDiffStatus();
        });
    }
  }

  /**
   * Walk both side's operator sets and compute a per-operator diff status that combines
   *   - presence: operator missing on the other side → `onlyInA` / `onlyInB`
   *   - properties: same id on both sides but `operatorProperties` JSON differs → `propsDiffer`
   *   - output: ports' row counts or schemas differ → `outputDiffer`
   *   - otherwise → `identical`
   * Pure presence/property checks come from the workflow content; output checks fall
   * back to the per-port summary that the compare endpoint returned.
   */
  private recomputeDiffStatus(): void {
    if (!this.summary || !this.contentA || !this.contentB) return;
    const opsA = new Map<string, any>();
    const opsB = new Map<string, any>();
    for (const op of this.parseOperators(this.contentA)) opsA.set(op.operatorID, op);
    for (const op of this.parseOperators(this.contentB)) opsB.set(op.operatorID, op);

    // Build a quick lookup: operatorId → true when at least one of its ports has a
    // row-count or schema mismatch. Cheaper than re-scanning summary inside the loop.
    const opIdsWithOutputDiff = new Set<string>();
    for (const entry of this.summary.operators) {
      const portDiffers =
        entry.status === "onlyInA" ||
        entry.status === "onlyInB" ||
        !entry.schemaMatches ||
        entry.rowCountA !== entry.rowCountB;
      if (portDiffers) opIdsWithOutputDiff.add(entry.operatorId);
    }

    const result = new Map<string, OperatorDiffStatus>();
    const counts = { identical: 0, propsDiffer: 0, outputDiffer: 0, onlyInA: 0, onlyInB: 0 };
    const allIds = new Set<string>([...opsA.keys(), ...opsB.keys()]);
    for (const id of allIds) {
      const a = opsA.get(id);
      const b = opsB.get(id);
      let status: OperatorDiffStatus;
      if (a && !b) {
        status = "onlyInA";
      } else if (!a && b) {
        status = "onlyInB";
      } else if (this.opPropertiesEqual(a, b)) {
        status = opIdsWithOutputDiff.has(id) ? "outputDiffer" : "identical";
      } else {
        status = "propsDiffer";
      }
      result.set(id, status);
      counts[status]++;
    }
    this.diffStatusMap = result;
    this.diffCounts = counts;
  }

  /**
   * Pull the operators array out of a parsed-or-string workflow content. WorkflowVersion
   * service usually parses it, but some legacy paths hand back a string; tolerate both.
   */
  private parseOperators(content: any): ReadonlyArray<{ operatorID: string; operatorProperties?: any }> {
    let parsed: any = content;
    if (typeof content === "string") {
      try {
        parsed = JSON.parse(content);
      } catch {
        return [];
      }
    }
    return Array.isArray(parsed?.operators) ? parsed.operators : [];
  }

  private opPropertiesEqual(a: any, b: any): boolean {
    // Deep-equal operatorProperties via JSON-string canonicalization. Field order matters
    // here; if that becomes a real issue we'd switch to a stable stringify, but Texera's
    // current persistence keeps key order consistent.
    return JSON.stringify(a?.operatorProperties ?? {}) === JSON.stringify(b?.operatorProperties ?? {});
  }

  /** Click handler from either DAG canvas. Resolves to the first port of the operator. */
  onDagOperatorClicked(operatorId: string): void {
    if (!this.summary) return;
    const entry = this.summary.operators.find(o => o.operatorId === operatorId);
    if (entry) {
      this.clickedOperatorMissing = null;
      this.selectOperator(entry);
    } else {
      // No per-operator results were persisted for this operator in either execution.
      // Surface this in the result panel so the click isn't silently ignored.
      this.clickedOperatorMissing = operatorId;
      this.selected = null;
    }
  }

  selectOperator(entry: OperatorPortCompareResult): void {
    this.selected = entry;
    this.pageIndex = 0;
    this.loadPage();
  }

  changePage(delta: number): void {
    const next = Math.max(0, this.pageIndex + delta);
    if (next === this.pageIndex) return;
    this.pageIndex = next;
    this.loadPage();
  }

  private loadPage(): void {
    if (!this.selected) return;
    this.pageLoadError = null;
    this.pageA = null;
    this.pageB = null;
    this.diffRows = [];
    this.unionColumnNames = [];
    this.rowsOnA = 0;
    this.rowsOnB = 0;

    const entry = this.selected;
    const fetchA =
      entry.status === "onlyInB"
        ? null
        : this.executionsService.retrieveExecutionResultPage(
            this.wid,
            this.eidA,
            entry.operatorId,
            entry.portId,
            this.pageIndex,
            this.pageSize
          );
    const fetchB =
      entry.status === "onlyInA"
        ? null
        : this.executionsService.retrieveExecutionResultPage(
            this.wid,
            this.eidB,
            entry.operatorId,
            entry.portId,
            this.pageIndex,
            this.pageSize
          );

    forkJoin({
      a: fetchA ?? Promise.resolve(null),
      b: fetchB ?? Promise.resolve(null),
    })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: ({ a, b }) => {
          this.pageA = a as ExecutionOperatorResultPage | null;
          this.pageB = b as ExecutionOperatorResultPage | null;
          this.computeRowDiff();
        },
        error: err => {
          this.pageLoadError = err?.error?.message ?? err?.message ?? "Failed to load result page";
        },
      });
  }

  /**
   * Build the side-by-side diff row list from the two fetched pages. Rows are paired
   * positionally (A[i] vs B[i]) — the same positional convention the existing
   * "deterministic sort" banner warns about. Each pair is classified into one of four
   * kinds so the template can apply git-diff–style coloring.
   */
  private computeRowDiff(): void {
    const rowsA = this.pageA?.rows ?? [];
    const rowsB = this.pageB?.rows ?? [];
    const colsA = this.pageA?.schema.map(s => s.name) ?? [];
    const colsB = this.pageB?.schema.map(s => s.name) ?? [];
    this.rowsOnA = this.pageA?.totalRowCount ?? rowsA.length;
    this.rowsOnB = this.pageB?.totalRowCount ?? rowsB.length;

    // Display the union of columns so per-cell diffs line up when schemas overlap.
    // Order: columns from A first (preserving A's column order), then any
    // B-only columns appended at the end.
    const seen = new Set<string>();
    const union: string[] = [];
    [...colsA, ...colsB].forEach(name => {
      if (!seen.has(name)) {
        seen.add(name);
        union.push(name);
      }
    });
    this.unionColumnNames = union;

    const maxRows = Math.max(rowsA.length, rowsB.length);
    const rows: DiffRow[] = [];
    for (let i = 0; i < maxRows; i++) {
      const rowA = rowsA[i];
      const rowB = rowsB[i];
      const cellsA: DiffCell[] = [];
      const cellsB: DiffCell[] = [];
      let anyCellDiffers = false;

      union.forEach(col => {
        const aPresent = !!rowA && col in rowA;
        const bPresent = !!rowB && col in rowB;
        const aVal = aPresent ? this.stringifyCell(rowA[col]) : "";
        const bVal = bPresent ? this.stringifyCell(rowB[col]) : "";
        const differs = aPresent && bPresent && aVal !== bVal;
        if (differs) anyCellDiffers = true;
        cellsA.push({ value: aVal, differs, missing: !aPresent });
        cellsB.push({ value: bVal, differs, missing: !bPresent });
      });

      const kind: RowKind = !rowA ? "onlyB" : !rowB ? "onlyA" : anyCellDiffers ? "changed" : "same";
      rows.push({ kind, cellsA, cellsB });
    }
    this.diffRows = rows;
  }

  private stringifyCell(value: unknown): string {
    if (value === null || value === undefined) return "NULL";
    if (typeof value === "string") return value;
    if (typeof value === "number" || typeof value === "boolean") return String(value);
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }

  schemaDiffSummary(entry: OperatorPortCompareResult): string {
    const namesA = new Set(entry.schemaA.map(a => a.name));
    const namesB = new Set(entry.schemaB.map(a => a.name));
    const added = entry.schemaB.filter(a => !namesA.has(a.name)).map(a => a.name);
    const removed = entry.schemaA.filter(a => !namesB.has(a.name)).map(a => a.name);
    const parts: string[] = [];
    if (added.length) parts.push(`+ ${added.join(", ")}`);
    if (removed.length) parts.push(`− ${removed.join(", ")}`);
    return parts.length ? parts.join("  ") : "schemas match";
  }

  /**
   * Operator IDs are of the form `{OperatorType}-operator-{uuid}`. Strip the suffix so the
   * left rail just shows the operator type (e.g. "TextInput" instead of
   * "TextInput-operator-bd06d395-…").
   */
  displayOperatorName(operatorId: string): string {
    const idx = operatorId.indexOf("-operator-");
    return idx > 0 ? operatorId.substring(0, idx) : operatorId;
  }

  /**
   * True when the current page is the last page of results on both sides. Used to disable
   * the Next button so the user can't page past the end into an empty view.
   */
  isLastPage(): boolean {
    if (!this.pageA && !this.pageB) return true;
    const totalA = this.pageA?.totalRowCount ?? 0;
    const totalB = this.pageB?.totalRowCount ?? 0;
    const maxTotal = Math.max(totalA, totalB);
    return (this.pageIndex + 1) * this.pageSize >= maxTotal;
  }
}
