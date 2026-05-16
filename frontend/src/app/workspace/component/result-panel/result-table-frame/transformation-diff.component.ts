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
import { NgIf, NgFor } from "@angular/common";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzTagComponent } from "ng-zorro-antd/tag";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowResultService } from "../../../service/workflow-result/workflow-result.service";
import { SchemaAttribute } from "../../../types/workflow-compiling.interface";

interface ColumnDiffEntry {
  name: string;
  type?: string;
}

interface UpstreamInfo {
  operatorId: string;
  operatorName: string;
  rowCount: number;
  schema: ReadonlyArray<SchemaAttribute>;
}

/**
 * Shows a compact "what changed" strip above the result grid: input operator(s)
 * on the left, current operator on the right, with row-count and column-count
 * deltas in the middle. Click the chevron to expand into a schema diff drawer
 * that lists added / removed / kept columns as colored tags.
 *
 * Built from data already in the front end: workflow graph (to find the
 * upstream operator), and the per-operator pagination services (for schema +
 * row count). No new backend round trips.
 */
@UntilDestroy()
@Component({
  selector: "texera-transformation-diff",
  templateUrl: "./transformation-diff.component.html",
  styleUrls: ["./transformation-diff.component.scss"],
  imports: [NgIf, NgFor, NzIconDirective, NzTagComponent],
})
export class TransformationDiffComponent implements OnInit, OnChanges {
  @Input() operatorId?: string;

  /** True for source operators that have no inputs (start of the pipeline). */
  isSource = false;
  /** True when we have more than one input link (join, union, etc.). */
  multiInput = false;
  inputCount = 0;

  currentOpName = "";
  currentRowCount: number | null = null;
  currentSchema: ReadonlyArray<SchemaAttribute> = [];

  upstream: UpstreamInfo | null = null;

  rowDelta: number | null = null;
  rowDeltaPercent: number | null = null;

  addedColumns: ColumnDiffEntry[] = [];
  removedColumns: ColumnDiffEntry[] = [];
  /** Columns present in both inputs and output. Type changes within are flagged. */
  keptColumns: ColumnDiffEntry[] = [];
  /** Columns present in both with a type change between input and output. */
  typeChangedColumns: { name: string; from: string; to: string }[] = [];

  /** Drawer collapsed by default — most users glance, only expand for detail. */
  isExpanded = false;

  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowResultService: WorkflowResultService
  ) {}

  ngOnInit(): void {
    // Refresh when any result update arrives — totals or schemas may have moved.
    this.workflowResultService
      .getResultUpdateStream()
      .pipe(untilDestroyed(this))
      .subscribe(() => this.recompute());
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.operatorId) {
      this.isExpanded = false;
      this.recompute();
    }
  }

  toggleExpanded(): void {
    if (this.isSource || !this.upstream) return;
    this.isExpanded = !this.isExpanded;
  }

  private recompute(): void {
    this.resetState();
    if (!this.operatorId) return;

    const graph = this.workflowActionService.getTexeraGraph();
    const currentOp = graph.getOperator(this.operatorId);
    this.currentOpName = currentOp?.customDisplayName ?? this.operatorId;

    const currentService = this.workflowResultService.getPaginatedResultService(this.operatorId);
    if (currentService) {
      this.currentRowCount = currentService.getCurrentTotalNumTuples();
      this.currentSchema = currentService.getSchema();
    }

    const inputLinks = graph.getInputLinksByOperatorId(this.operatorId);
    this.inputCount = inputLinks.length;
    if (inputLinks.length === 0) {
      this.isSource = true;
      return;
    }
    if (inputLinks.length > 1) {
      // Join / union with multiple sources — collapse to a "multi-input" hint.
      this.multiInput = true;
      return;
    }

    const upstreamOpId = inputLinks[0].source.operatorID;
    const upstreamOp = graph.getOperator(upstreamOpId);
    const upstreamService = this.workflowResultService.getPaginatedResultService(upstreamOpId);
    if (!upstreamService || !upstreamOp) return;

    this.upstream = {
      operatorId: upstreamOpId,
      operatorName: upstreamOp.customDisplayName ?? upstreamOpId,
      rowCount: upstreamService.getCurrentTotalNumTuples(),
      schema: upstreamService.getSchema(),
    };

    this.computeRowDelta();
    this.computeColumnDiff();
  }

  private resetState(): void {
    this.isSource = false;
    this.multiInput = false;
    this.inputCount = 0;
    this.currentOpName = "";
    this.currentRowCount = null;
    this.currentSchema = [];
    this.upstream = null;
    this.rowDelta = null;
    this.rowDeltaPercent = null;
    this.addedColumns = [];
    this.removedColumns = [];
    this.keptColumns = [];
    this.typeChangedColumns = [];
  }

  private computeRowDelta(): void {
    if (!this.upstream || this.currentRowCount === null) return;
    const before = this.upstream.rowCount;
    const after = this.currentRowCount;
    this.rowDelta = after - before;
    if (before > 0) this.rowDeltaPercent = (this.rowDelta / before) * 100;
  }

  private computeColumnDiff(): void {
    if (!this.upstream) return;
    const upstreamByName = new Map(this.upstream.schema.map(a => [a.attributeName, a]));
    const currentByName = new Map(this.currentSchema.map(a => [a.attributeName, a]));

    for (const [name, attr] of currentByName) {
      if (!upstreamByName.has(name)) {
        this.addedColumns.push({ name, type: attr.attributeType });
      } else {
        const upAttr = upstreamByName.get(name)!;
        this.keptColumns.push({ name, type: attr.attributeType });
        if (upAttr.attributeType !== attr.attributeType) {
          this.typeChangedColumns.push({
            name,
            from: upAttr.attributeType ?? "?",
            to: attr.attributeType ?? "?",
          });
        }
      }
    }
    for (const [name, attr] of upstreamByName) {
      if (!currentByName.has(name)) {
        this.removedColumns.push({ name, type: attr.attributeType });
      }
    }
  }

  /** Render-helper: row delta sign / magnitude classes for the badge. */
  get rowDeltaClass(): "positive" | "negative" | "zero" | "unknown" {
    if (this.rowDelta === null) return "unknown";
    if (this.rowDelta > 0) return "positive";
    if (this.rowDelta < 0) return "negative";
    return "zero";
  }

  get hasChanges(): boolean {
    return (
      this.addedColumns.length > 0 ||
      this.removedColumns.length > 0 ||
      this.typeChangedColumns.length > 0 ||
      (this.rowDelta !== null && this.rowDelta !== 0)
    );
  }

  formatNumber(n: number | null): string {
    if (n === null || n === undefined) return "—";
    return n.toLocaleString();
  }

  formatSigned(n: number | null): string {
    if (n === null) return "—";
    if (n > 0) return `+${n.toLocaleString()}`;
    return n.toLocaleString();
  }

  formatPercent(p: number | null): string {
    if (p === null) return "";
    const sign = p > 0 ? "+" : "";
    return `${sign}${p.toFixed(1)}%`;
  }

  /**
   * Width (as a percentage string) of the "after" bar in the row-count
   * comparison: scale `currentRowCount` against whichever side is larger so
   * the longer bar always pins to 100%.
   */
  afterBarWidth(): string {
    if (!this.upstream || this.currentRowCount === null) return "0%";
    const denom = Math.max(this.upstream.rowCount, this.currentRowCount);
    if (denom <= 0) return "0%";
    return `${(this.currentRowCount / denom) * 100}%`;
  }

  /**
   * Width of the "before" bar — pinned to 100% unless the after side is
   * larger (e.g. an explode/cross-join), in which case scale down.
   */
  beforeBarWidth(): string {
    if (!this.upstream || this.upstream.rowCount <= 0) return "0%";
    if (this.currentRowCount === null || this.currentRowCount <= this.upstream.rowCount) return "100%";
    return `${(this.upstream.rowCount / this.currentRowCount) * 100}%`;
  }
}
