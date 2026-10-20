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

import { ChangeDetectorRef, Component, EventEmitter, Input, OnChanges, Output, SimpleChanges } from "@angular/core";
import { NgFor, NgIf } from "@angular/common";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { Subscription } from "rxjs";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowResultService } from "../../../service/workflow-result/workflow-result.service";
import { WorkflowCompilingService } from "../../../service/compile-workflow/workflow-compiling.service";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { WorkflowConsoleService } from "../../../service/workflow-console/workflow-console.service";
import { ExecutionState } from "../../../types/execute-workflow.interface";

/** How many columns the preview shows before it says how many more there are. */
export const PREVIEW_COLUMN_LIMIT = 6;
/** How many rows the preview shows. */
export const PREVIEW_ROW_LIMIT = 5;
/** How many characters of a cell the preview shows; the rest is in the cell's tooltip. */
export const PREVIEW_CELL_LIMIT = 24;

/** A cell as the preview prints it: short, and never "[object Object]". */
export function formatPreviewCell(value: unknown): string {
  if (value === null || value === undefined) return "";
  const text = typeof value === "object" ? JSON.stringify(value) : String(value);
  return text.length > PREVIEW_CELL_LIMIT ? text.slice(0, PREVIEW_CELL_LIMIT - 1) + "…" : text;
}

export interface OperatorResultPreview {
  title: string;
  kind: "table" | "chart" | "error" | "none";
  summary: string;
  columns: readonly string[];
  moreColumns: number;
  detail?: string;
}

/**
 * What hovering an operator on the canvas shows about its result: the first rows of a table,
 * that it drew a chart, its error, or why there is nothing. The rows are fetched on their own
 * request, which leaves the page the result table is on alone.
 */
@UntilDestroy()
@Component({
  selector: "texera-operator-result-preview",
  templateUrl: "./operator-result-preview.component.html",
  styleUrls: ["./operator-result-preview.component.scss"],
  imports: [NgIf, NgFor, NzIconDirective],
})
export class OperatorResultPreviewComponent implements OnChanges {
  @Input() operatorId = "";
  // whether the card offers to pin the result next to the operator
  @Input() pinnable = false;
  @Output() pin = new EventEmitter<void>();

  // The first rows of a table result, one string per shown column; undefined while they load.
  rows: readonly (readonly string[])[] | undefined = undefined;
  private rowsSubscription?: Subscription;

  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowResultService: WorkflowResultService,
    private workflowCompilingService: WorkflowCompilingService,
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowConsoleService: WorkflowConsoleService,
    private changeDetectorRef: ChangeDetectorRef
  ) {}

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["operatorId"]) {
      this.loadRows();
    }
  }

  private loadRows(): void {
    this.rowsSubscription?.unsubscribe();
    const paginated = this.workflowResultService.getPaginatedResultService(this.operatorId);
    if (!paginated || paginated.getCurrentTotalNumTuples() === 0) {
      this.rows = [];
      return;
    }
    this.rows = undefined;
    const columns = this.outputColumns(this.operatorId).slice(0, PREVIEW_COLUMN_LIMIT);
    // Replaced when another operator is hovered, and dropped with the preview when the pointer leaves.
    this.rowsSubscription = paginated
      .peekFirstRows(PREVIEW_ROW_LIMIT)
      .pipe(untilDestroyed(this))
      .subscribe(page => {
        this.rows = page.table.map(row =>
          columns.map(column => formatPreviewCell((row as Record<string, unknown>)[column]))
        );
        this.changeDetectorRef.detectChanges();
      });
  }

  get preview(): OperatorResultPreview {
    const id = this.operatorId;
    const graph = this.workflowActionService.getTexeraGraph();
    const operator = graph.hasOperator(id) ? graph.getOperator(id) : undefined;
    const title = operator?.customDisplayName ?? operator?.operatorType ?? id;
    const base = { title, columns: [], moreColumns: 0 };

    const error = this.executeWorkflowService
      .getErrorMessages()
      .concat(Object.values(this.workflowCompilingService.getWorkflowCompilationErrors()))
      .find(e => e.operatorId === id);
    if (error) {
      return { ...base, kind: "error", summary: "failed", detail: error.message };
    }
    // A bad tuple pauses the worker and writes an ERROR to the console rather than failing the run.
    const consoleError = (this.workflowConsoleService.getConsoleMessages(id) ?? []).find(
      message => message.msgType.name === "ERROR"
    );
    if (consoleError) {
      return { ...base, kind: "error", summary: "failed", detail: consoleError.title };
    }

    const paginated = this.workflowResultService.getPaginatedResultService(id);
    if (paginated) {
      const columns = this.outputColumns(id);
      const rows = paginated.getCurrentTotalNumTuples();
      // The preview is of the first output port; the pinned window has a tab for each.
      const outputCount = operator?.outputPorts.length ?? 1;
      return {
        ...base,
        kind: "table",
        summary:
          `${rows.toLocaleString()} ${rows === 1 ? "row" : "rows"} · ${columns.length} ${
            columns.length === 1 ? "column" : "columns"
          }` + (outputCount > 1 ? ` · first of ${outputCount} outputs` : ""),
        columns: columns.slice(0, PREVIEW_COLUMN_LIMIT),
        moreColumns: Math.max(0, columns.length - PREVIEW_COLUMN_LIMIT),
      };
    }

    if (this.workflowResultService.hasResult(id)) {
      return { ...base, kind: "chart", summary: "chart" };
    }

    const hasRun = this.executeWorkflowService.getExecutionState().state !== ExecutionState.Uninitialized;
    return {
      ...base,
      kind: "none",
      summary: "no result",
      detail: hasRun
        ? "This run did not keep this operator's result. Turn on its eye icon to keep it next time."
        : "Run the workflow to see its result.",
    };
  }

  /** The columns the operator's first output port was compiled to produce. */
  private outputColumns(operatorId: string): string[] {
    const ports = this.workflowCompilingService.getOperatorOutputSchemaMap(operatorId);
    const firstPort = ports ? Object.values(ports)[0] : undefined;
    return (firstPort ?? []).map(attribute => attribute.attributeName);
  }
}
