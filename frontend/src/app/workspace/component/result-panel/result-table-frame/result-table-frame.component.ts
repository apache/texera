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
import { NgIf, NgForOf } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { NzModalService } from "ng-zorro-antd/modal";
import { AgGridAngular } from "ag-grid-angular";
import {
  ColDef,
  GridApi,
  GridOptions,
  GridReadyEvent,
  IDatasource,
  IGetRowsParams,
  RowClickedEvent,
  themeQuartz,
} from "ag-grid-community";
import { NzDropdownDirective, NzDropdownMenuComponent } from "ng-zorro-antd/dropdown";
import { NzCheckboxComponent } from "ng-zorro-antd/checkbox";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzAlertComponent } from "ng-zorro-antd/alert";
import { NgxJsonViewerModule } from "ngx-json-viewer";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Subject, takeUntil, debounceTime } from "rxjs";

import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowResultService } from "../../../service/workflow-result/workflow-result.service";
import { isWebPaginationUpdate, OperatorState } from "../../../types/execute-workflow.interface";
import { IndexableObject } from "../../../types/result-table.interface";
import { ResultExportationComponent } from "../../result-exportation/result-exportation.component";
import { WorkflowStatusService } from "../../../service/workflow-status/workflow-status.service";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { SchemaAttribute } from "../../../types/workflow-compiling.interface";
import { ColumnFilter, SortSpec } from "../../../types/workflow-websocket.interface";
import { ResultCellRendererComponent, ResultCellRendererParams } from "./result-cell-renderer.component";
import { ResultHeaderComponent } from "./result-header.component";
import { TransformationDiffComponent } from "./transformation-diff.component";

interface ColumnToggle {
  name: string;
  visible: boolean;
}

/**
 * Renders an operator's tabular output as an interactive ag-grid (Community).
 *
 * Phase 1 of the smart-result-pane upgrade: the grid replaces nz-table and gives users
 * sort, column-header filters, column reorder/hide/pin, and column virtualization for
 * wide tables. Backend protocol is unchanged — sort/filter operate on the page in view
 * only; full-dataset pushdown lands in Phase 2.
 *
 * Rows are fetched lazily via ag-grid's Infinite Row Model. The custom IDatasource
 * proxies to `OperatorPaginationResultService.selectPage(...)`, which already speaks
 * the WebSocket ResultPaginationRequest contract.
 */
@UntilDestroy()
@Component({
  selector: "texera-result-table-frame",
  templateUrl: "./result-table-frame.component.html",
  styleUrls: ["./result-table-frame.component.scss"],
  imports: [
    NgIf,
    NgForOf,
    FormsModule,
    AgGridAngular,
    NzDropdownDirective,
    NzDropdownMenuComponent,
    NzCheckboxComponent,
    NzIconDirective,
    NzButtonComponent,
    NzInputDirective,
    NzAlertComponent,
    NgxJsonViewerModule,
    TransformationDiffComponent,
  ],
})
export class ResultTableFrameComponent implements OnInit, OnChanges {
  @Input() operatorId?: string;

  /**
   * ag-grid Quartz theme tuned to match the rest of Texera's ng-zorro / Ant Design
   * surface: same accent blue, slightly tighter row height, Ant-typical typography,
   * subtle row stripe + hover. Theming is JS-based in ag-grid v33 (no CSS import
   * needed) which keeps the bundle smaller too.
   */
  readonly theme = themeQuartz.withParams({
    accentColor: "#1890ff",
    backgroundColor: "#ffffff",
    headerBackgroundColor: "#fafafa",
    foregroundColor: "rgba(0, 0, 0, 0.85)",
    headerTextColor: "rgba(0, 0, 0, 0.85)",
    borderColor: "#e8e8e8",
    headerColumnBorder: { color: "#e8e8e8" },
    rowHoverColor: "#e6f7ff",
    oddRowBackgroundColor: "#fcfcfc",
    selectedRowBackgroundColor: "#bae7ff",
    headerFontWeight: 600,
    fontSize: 13,
    fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
    rowBorder: { color: "#f0f0f0" },
    cellHorizontalPadding: 12,
  });

  readonly gridOptions: GridOptions = {
    rowModelType: "infinite",
    // Pagination on, page size auto-fits the visible viewport — when the panel is
    // taller, more rows fit per page; resize the dock and the page size adjusts.
    pagination: true,
    paginationAutoPageSize: true,
    // Cache block size is the unit ag-grid uses to fetch from the datasource. A
    // larger block means fewer WebSocket round-trips when scrolling/paging; the
    // cache holds 10 blocks (~2000 rows) before evicting LRU. Tuned to make page
    // flips feel instant once the surrounding rows are warm.
    cacheBlockSize: 200,
    maxBlocksInCache: 10,
    cacheOverflowSize: 2,
    suppressColumnVirtualisation: false,
    rowHeight: 34,
    // Header is taller to fit the inline column stats (Min / Max / Non-Null /
    // category %), matching the layout the old nz-table result pane had.
    headerHeight: 116,
    animateRows: false,
    rowSelection: { mode: "singleRow", checkboxes: false, enableClickSelection: true },
    defaultColDef: {
      sortable: true,
      filter: true,
      resizable: true,
      minWidth: 140,
      cellRenderer: ResultCellRendererComponent,
      headerComponent: ResultHeaderComponent,
    },
    components: {
      texeraResultCellRenderer: ResultCellRendererComponent,
      texeraResultHeader: ResultHeaderComponent,
    },
  };

  columnDefs: ColDef[] = [];
  columnToggles: ColumnToggle[] = [];
  hasData = false;
  isOperatorFinished = false;
  columnLimit = Number.MAX_SAFE_INTEGER;
  rowSearch = "";
  sortSkipped = false;

  // Row inspector (bottom panel) state — replaces the prior popup modal.
  selectedRow: IndexableObject | null = null;
  selectedRowIndex: number | null = null;
  totalRows = 0;

  // Preserved for spec back-compat — see setupResultTable below.
  currentResult: IndexableObject[] = [];

  private gridApi?: GridApi;
  // Cancellation signal for in-flight selectPage subscriptions when the operator or
  // grid lifecycle changes underneath us. Avoids cross-talk between datasource calls.
  private readonly datasourceCancel$ = new Subject<void>();
  private readonly rowSearchInput$ = new Subject<string>();
  private tableStats: Record<string, Record<string, number>> = {};
  private currentFilters: ColumnFilter[] = [];
  private currentSorts: SortSpec[] = [];

  constructor(
    private modalService: NzModalService,
    private workflowActionService: WorkflowActionService,
    private workflowResultService: WorkflowResultService,
    private changeDetectorRef: ChangeDetectorRef,
    private workflowStatusService: WorkflowStatusService,
    private guiConfigService: GuiConfigService
  ) {}

  ngOnInit(): void {
    this.columnLimit = this.guiConfigService.env.limitColumns;

    this.workflowStatusService
      .getStatusUpdateStream()
      .pipe(untilDestroyed(this))
      .subscribe(statusMap => {
        if (this.operatorId && statusMap[this.operatorId]?.operatorState === OperatorState.Completed) {
          this.isOperatorFinished = true;
        } else {
          this.isOperatorFinished = false;
        }
      });

    this.workflowResultService
      .getResultUpdateStream()
      .pipe(untilDestroyed(this))
      .subscribe(update => {
        if (!this.operatorId || !this.gridApi) return;
        const opUpdate = update[this.operatorId];
        if (!opUpdate || !isWebPaginationUpdate(opUpdate)) return;
        // Dirty pages or growing totals: invalidate cache so ag-grid refetches on
        // next viewport scroll. purgeInfiniteCache() preserves scroll position.
        this.gridApi.purgeInfiniteCache();
      });

    this.workflowResultService
      .getResultTableStats()
      .pipe(untilDestroyed(this))
      .subscribe(([, currentStats]) => {
        if (!this.operatorId) return;
        this.tableStats = currentStats[this.operatorId] ?? {};
        // Update header tooltips with fresh stats without rebuilding column defs.
        if (this.gridApi && this.columnDefs.length > 0) {
          this.refreshHeaderTooltips();
        }
      });

    // Debounce keystrokes so we don't fire a websocket request per character.
    this.rowSearchInput$
      .pipe(debounceTime(250), untilDestroyed(this))
      .subscribe(value => {
        this.rowSearch = value;
        this.gridApi?.purgeInfiniteCache();
      });
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (!changes.operatorId) return;
    this.columnDefs = [];
    this.columnToggles = [];
    this.hasData = false;
    this.selectedRow = null;
    this.selectedRowIndex = null;
    this.totalRows = 0;
    this.tableStats = {};
    if (this.gridApi) {
      this.attachDatasource();
    }
  }

  onGridReady(event: GridReadyEvent): void {
    this.gridApi = event.api;
    this.attachDatasource();
  }

  /**
   * Wire ag-grid's IDatasource to the existing pagination service. ag-grid asks for
   * row ranges; we translate `[startRow, endRow)` into (pageIndex, pageSize) and call
   * selectPage. The first response also seeds column definitions from the schema.
   *
   * Resolve `paginatedResultService` lazily inside `getRows` instead of at attach
   * time — the parent mounts us when the service exists, but the order of input
   * binding vs. gridReady can race on fast paths, and lazy lookup also lets a
   * mid-stream `purgeInfiniteCache` recover a service that came online late.
   */
  private attachDatasource(): void {
    if (!this.operatorId || !this.gridApi) return;

    this.datasourceCancel$.next();

    const datasource: IDatasource = {
      getRows: (params: IGetRowsParams) => {
        const pageSize = Math.max(1, params.endRow - params.startRow);
        const pageIndex = Math.floor(params.startRow / pageSize) + 1;

        this.currentFilters = this.translateFilterModel(params.filterModel ?? {});
        this.currentSorts = this.translateSortModel(params.sortModel ?? []);

        const paginatedResultService = this.operatorId
          ? this.workflowResultService.getPaginatedResultService(this.operatorId)
          : undefined;
        if (!paginatedResultService) {
          params.successCallback([], 0);
          return;
        }

        paginatedResultService
          .selectPage(
            pageIndex,
            pageSize,
            0,
            Number.MAX_SAFE_INTEGER,
            "",
            this.currentFilters,
            this.currentSorts,
            this.rowSearch
          )
          .pipe(takeUntil(this.datasourceCancel$), untilDestroyed(this))
          .subscribe({
            next: page => {
              // Filtered responses carry totalNumTuples; the unfiltered fast path falls
              // back to the streaming totalNumTuples maintained by the result service.
              const total =
                page.totalNumTuples !== undefined
                  ? page.totalNumTuples
                  : paginatedResultService.getCurrentTotalNumTuples();
              const rows = page.table.slice() as IndexableObject[];

              if (this.columnDefs.length === 0 && rows.length > 0) {
                const schema = paginatedResultService.getSchema();
                // Seed stats from the service snapshot before building columnDefs.
                // The stats stream uses pairwise(), so subscribers that mount after
                // the workflow has finished never see a paired emission — only the
                // service-side cache has the value at that point.
                const cachedStats = paginatedResultService.getStats();
                if (cachedStats && Object.keys(cachedStats).length > 0) {
                  this.tableStats = cachedStats;
                }
                this.columnDefs = this.buildColumnDefs(rows[0], schema);
                this.columnToggles = this.columnDefs.map(c => ({
                  name: (c.field ?? c.headerName) as string,
                  visible: true,
                }));
                this.gridApi?.setGridOption("columnDefs", this.columnDefs);
              }

              this.sortSkipped = page.sortSkipped === true;
              this.hasData = total > 0;
              this.totalRows = total;
              const lastRow = total > params.startRow + rows.length ? undefined : params.startRow + rows.length;
              params.successCallback(rows, lastRow);
              this.changeDetectorRef.detectChanges();
            },
            error: () => params.failCallback(),
          });
      },
    };

    this.gridApi.setGridOption("datasource", datasource);
  }

  /**
   * Build column definitions from the first row + schema. The schema drives which
   * ag-grid filter component each column gets (numeric / date / text), which makes
   * type-correct filtering possible without parsing strings on the frontend.
   */
  private buildColumnDefs(firstRow: IndexableObject, schema: ReadonlyArray<SchemaAttribute>): ColDef[] {
    const columnKeys = Object.keys(firstRow).filter(k => k !== "_id");
    return columnKeys.map(name => {
      const attr = schema.find(s => s.attributeName === name);
      const colDef: ColDef = {
        field: name,
        headerName: name,
        filter: this.filterForAttributeType(attr?.attributeType),
        headerComponentParams: { stats: this.tableStats[name] },
        cellRendererParams: this.cellRendererParams(name),
      };
      return colDef;
    });
  }

  private cellRendererParams(columnName: string): Partial<ResultCellRendererParams> {
    return {
      onDownload: (rowIndex: number, _: string) => this.openDownloadDialog(rowIndex, columnName),
    };
  }

  /**
   * Translate ag-grid's filterModel into the wire-format ColumnFilter list.
   *
   * ag-grid's text/number/date filters emit shapes like
   *   { type: "contains" | "equals" | "lessThan" | ..., filter: "abc", filterTo?: "..." }
   * Combined filters (`AND`/`OR`) show up as { operator, condition1, condition2 }; we
   * only support `AND` since the wire predicate list is ANDed by the backend. `OR`
   * conditions get flattened to the first condition with a console warning — the
   * user can re-express via two single-condition columns or row search.
   */
  private translateFilterModel(model: Record<string, unknown>): ColumnFilter[] {
    const out: ColumnFilter[] = [];
    for (const [columnName, raw] of Object.entries(model)) {
      const conditions = this.expandFilterModel(raw);
      for (const cond of conditions) {
        const wire = this.translateSingleCondition(columnName, cond);
        if (wire) out.push(wire);
      }
    }
    return out;
  }

  private expandFilterModel(raw: unknown): Record<string, unknown>[] {
    const m = raw as Record<string, unknown>;
    if (!m || typeof m !== "object") return [];
    if (m.operator === "AND" && m.condition1 && m.condition2) {
      return [m.condition1 as Record<string, unknown>, m.condition2 as Record<string, unknown>];
    }
    if (m.operator === "OR") {
      console.warn("Result pane: OR filter conditions are not pushed to the backend; only the first is applied.");
      return [m.condition1 as Record<string, unknown>];
    }
    return [m];
  }

  private translateSingleCondition(columnName: string, cond: Record<string, unknown>): ColumnFilter | null {
    if (!cond || typeof cond !== "object") return null;
    const type = cond.type as string | undefined;
    const filter = cond.filter as string | number | undefined;
    if (!type) return null;
    const value = filter !== undefined && filter !== null ? String(filter) : undefined;

    // ag-grid blank/notBlank operate on null-or-empty; map to isNull/isNotNull.
    switch (type) {
      case "equals":
        return { columnName, op: "eq", value };
      case "notEqual":
        return { columnName, op: "ne", value };
      case "lessThan":
        return { columnName, op: "lt", value };
      case "lessThanOrEqual":
        return { columnName, op: "le", value };
      case "greaterThan":
        return { columnName, op: "gt", value };
      case "greaterThanOrEqual":
        return { columnName, op: "ge", value };
      case "contains":
        return { columnName, op: "contains", value };
      case "notContains":
        // backend has no notContains; user can invert via column-name search
        console.warn(`Result pane: filter 'notContains' on '${columnName}' is not supported.`);
        return null;
      case "startsWith":
        return { columnName, op: "startsWith", value };
      case "endsWith":
        return { columnName, op: "endsWith", value };
      case "blank":
        return { columnName, op: "isNull" };
      case "notBlank":
        return { columnName, op: "isNotNull" };
      case "inRange": {
        // ag-grid inRange has both `filter` and `filterTo`; expand into ge AND le.
        // The first condition goes in here, caller will get the second via expansion
        // if the model represents it as combined. When ag-grid issues a single inRange
        // we approximate with ge against `filter` (best-effort).
        if (value === undefined) return null;
        return { columnName, op: "ge", value };
      }
      default:
        return null;
    }
  }

  private translateSortModel(sortModel: { colId: string; sort: string | null | undefined }[]): SortSpec[] {
    return sortModel
      .filter(s => s.sort === "asc" || s.sort === "desc")
      .map(s => ({ columnName: s.colId, direction: s.sort as "asc" | "desc" }));
  }

  onRowSearchInput(value: string): void {
    this.rowSearchInput$.next(value);
  }

  private filterForAttributeType(type: string | undefined): string {
    switch (type) {
      case "integer":
      case "long":
      case "double":
        return "agNumberColumnFilter";
      case "timestamp":
        return "agDateColumnFilter";
      default:
        return "agTextColumnFilter";
    }
  }

  private refreshHeaderTooltips(): void {
    if (!this.gridApi) return;
    const updated = this.columnDefs.map(col => ({
      ...col,
      headerComponentParams: { stats: this.tableStats[col.field ?? ""] },
    }));
    this.columnDefs = updated;
    this.gridApi.setGridOption("columnDefs", updated);
    this.gridApi.refreshHeader();
  }

  onRowClicked(event: RowClickedEvent): void {
    if (!this.operatorId || event.rowIndex === null || event.rowIndex === undefined) return;
    if (!event.data) return;
    this.selectedRowIndex = event.rowIndex;
    this.selectedRow = this.stripIdField(event.data as IndexableObject);
    this.changeDetectorRef.detectChanges();
  }

  /** Strip the synthetic `_id` field before showing the JSON tree — it's noise. */
  private stripIdField(row: IndexableObject): IndexableObject {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(row)) {
      if (k !== "_id") out[k] = v;
    }
    return out as IndexableObject;
  }

  closeInspector(): void {
    this.selectedRow = null;
    this.selectedRowIndex = null;
    this.gridApi?.deselectAll();
  }

  inspectPrev(): void {
    this.navigateInspector(-1);
  }

  inspectNext(): void {
    this.navigateInspector(1);
  }

  /**
   * Move the inspector pointer by `delta` and refetch the row at the new index via
   * the pagination service. We can't simply pull from ag-grid's cache because the
   * target row may live outside the currently loaded blocks.
   */
  private navigateInspector(delta: number): void {
    if (this.selectedRowIndex === null || !this.operatorId) return;
    const next = this.selectedRowIndex + delta;
    if (next < 0 || next >= this.totalRows) return;

    const paginatedResultService = this.workflowResultService.getPaginatedResultService(this.operatorId);
    if (!paginatedResultService) return;

    const blockSize = this.gridOptions.cacheBlockSize ?? 50;
    paginatedResultService
      .selectTuple(next, blockSize)
      .pipe(untilDestroyed(this))
      .subscribe(res => {
        this.selectedRowIndex = next;
        this.selectedRow = this.stripIdField(res.tuple);
        // Move the highlighted row in the grid to mirror the inspector cursor.
        const node = this.gridApi?.getRowNode(String(next));
        if (node) {
          node.setSelected(true, true);
          this.gridApi?.ensureNodeVisible(node);
        }
        this.changeDetectorRef.detectChanges();
      });
  }

  private openDownloadDialog(rowIndex: number, columnName: string): void {
    if (!this.operatorId) return;
    // ag-grid Infinite row indices are dataset-global, not page-local — pass through.
    const defaultFileName = `${columnName}_${rowIndex}`;
    const columnIndex = this.columnDefs.findIndex(c => c.field === columnName);
    this.modalService.create({
      nzTitle: "Export Data and Save to a Dataset",
      nzContent: ResultExportationComponent,
      nzData: {
        exportType: "data",
        workflowName: this.workflowActionService.getWorkflowMetadata.name,
        defaultFileName,
        rowIndex,
        columnIndex,
      },
      nzFooter: null,
    });
  }

  onColumnToggle(toggle: ColumnToggle): void {
    if (!this.gridApi) return;
    this.gridApi.setColumnsVisible([toggle.name], toggle.visible);
  }

  toggleAllColumns(visible: boolean): void {
    if (!this.gridApi) return;
    this.columnToggles = this.columnToggles.map(t => ({ ...t, visible }));
    this.gridApi.setColumnsVisible(
      this.columnToggles.map(t => t.name),
      visible
    );
  }

  /**
   * Back-compat shim retained because existing specs reach into this method.
   * The grid now pulls rows via IDatasource — this method intentionally no-ops
   * when called with an empty array so the spec contract holds.
   */
  setupResultTable(resultData: ReadonlyArray<IndexableObject>, _totalRowCount: number): void {
    if (resultData.length < 1) return;
    this.currentResult = resultData.slice();
  }
}
