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

interface CellDiff {
  readonly value: string;
  readonly differs: boolean;
  readonly missing: boolean;
}

interface RowPair {
  readonly index: number;
  readonly cellsA: ReadonlyArray<CellDiff>;
  readonly cellsB: ReadonlyArray<CellDiff>;
  readonly anyDiff: boolean;
}

@UntilDestroy()
@Component({
  selector: "texera-compare-workspace",
  standalone: true,
  imports: [CommonModule],
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

  selected: OperatorPortCompareResult | null = null;
  pageIndex = 0;
  pageSize = 25;
  pageA: ExecutionOperatorResultPage | null = null;
  pageB: ExecutionOperatorResultPage | null = null;
  pageLoadError: string | null = null;
  rowPairs: ReadonlyArray<RowPair> = [];
  unionColumnNames: ReadonlyArray<string> = [];

  constructor(
    private route: ActivatedRoute,
    private executionsService: WorkflowExecutionsService
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
    this.rowPairs = [];
    this.unionColumnNames = [];

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

  private computeRowDiff(): void {
    const rowsA = this.pageA?.rows ?? [];
    const rowsB = this.pageB?.rows ?? [];
    const colsA = this.pageA?.schema.map(s => s.name) ?? [];
    const colsB = this.pageB?.schema.map(s => s.name) ?? [];
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
    const pairs: RowPair[] = [];
    for (let i = 0; i < maxRows; i++) {
      const rowA = rowsA[i];
      const rowB = rowsB[i];
      const cellsA: CellDiff[] = [];
      const cellsB: CellDiff[] = [];
      let anyDiff = false;

      union.forEach(col => {
        const aPresent = rowA && col in rowA;
        const bPresent = rowB && col in rowB;
        const aVal = aPresent ? this.stringifyCell(rowA[col]) : "";
        const bVal = bPresent ? this.stringifyCell(rowB[col]) : "";
        const differs = aPresent && bPresent && aVal !== bVal;
        if (differs || aPresent !== bPresent || !rowA || !rowB) {
          anyDiff = true;
        }
        cellsA.push({ value: aVal, differs, missing: !rowA || !aPresent });
        cellsB.push({ value: bVal, differs, missing: !rowB || !bPresent });
      });

      pairs.push({ index: i, cellsA, cellsB, anyDiff });
    }
    this.rowPairs = pairs;
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

  badgeClass(entry: OperatorPortCompareResult): string {
    if (entry.status === "onlyInA" || entry.status === "onlyInB") return "badge-red";
    if (!entry.schemaMatches) return "badge-red";
    if (entry.rowCountA !== entry.rowCountB) return "badge-yellow";
    return "badge-green";
  }

  rowCountDelta(entry: OperatorPortCompareResult): string {
    const a = entry.rowCountA ?? 0;
    const b = entry.rowCountB ?? 0;
    return `A: ${entry.rowCountA ?? "—"} · B: ${entry.rowCountB ?? "—"} · Δ ${b - a}`;
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
}
