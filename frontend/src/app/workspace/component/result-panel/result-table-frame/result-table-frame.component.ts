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
import { NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import {
  NzTableQueryParams,
  NzTableComponent,
  NzTheadComponent,
  NzTrDirective,
  NzTableCellDirective,
  NzThMeasureDirective,
  NzTbodyComponent,
  NzCellEllipsisDirective,
} from "ng-zorro-antd/table";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowResultService } from "../../../service/workflow-result/workflow-result.service";
import { PanelResizeService } from "../../../service/workflow-result/panel-resize/panel-resize.service";
import { isWebPaginationUpdate, OperatorState } from "../../../types/execute-workflow.interface";
import { IndexableObject, TableColumn } from "../../../types/result-table.interface";
import { RowModalComponent } from "../result-panel-modal.component";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DomSanitizer, SafeHtml } from "@angular/platform-browser";
import { ResultExportationComponent } from "../../result-exportation/result-exportation.component";
import { WorkflowStatusService } from "../../../service/workflow-status/workflow-status.service";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { NgIf, NgFor, NgClass } from "@angular/common";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import {
  LineageHighlightRequest,
  LineageHighlightService,
} from "../../../service/workflow-result/lineage-highlight.service";

/**
 * The Component will display the result in an excel table format,
 *  where each row represents a result from the workflow,
 *  and each column represents the type of result the workflow returns.
 *
 * Clicking each row of the result table will create an pop-up window
 *  and display the detail of that row in a pretty json format.
 */
@UntilDestroy()
@Component({
  selector: "texera-result-table-frame",
  templateUrl: "./result-table-frame.component.html",
  styleUrls: ["./result-table-frame.component.scss"],
  imports: [
    NgIf,
    NzSpaceCompactItemDirective,
    NzInputDirective,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzTableComponent,
    NzTheadComponent,
    NzTrDirective,
    NgFor,
    NzTableCellDirective,
    NzThMeasureDirective,
    NgClass,
    NzTbodyComponent,
    NzCellEllipsisDirective,
    NzTooltipDirective,
  ],
})
export class ResultTableFrameComponent implements OnInit, OnChanges {
  @Input() operatorId?: string;

  // display result table
  currentColumns?: TableColumn[];
  currentResult: IndexableObject[] = [];
  //   for more details
  //   see https://ng.ant.design/components/table/en#components-table-demo-ajax
  isFrontPagination: boolean = true;

  isLoadingResult: boolean = false;

  // paginator section, used when displaying rows

  // this attribute stores whether front-end should handle pagination
  //   if false, it means the pagination is managed by the server
  // this starts from **ONE**, not zero
  currentPageIndex: number = 1;
  totalNumTuples: number = 0;
  pageSize = 5;
  currentColumnOffset = 0;
  columnLimit = 25;
  columnSearch = "";
  panelHeight = 0;
  tableStats: Record<string, Record<string, number>> = {};
  prevTableStats: Record<string, Record<string, number>> = {};
  widthPercent: string = "";
  isOperatorFinished: boolean = false;

  /**
   * 0-indexed row position (within the currently displayed page) that should be
   * temporarily highlighted to satisfy a `LineageHighlightService` request. -1
   * means no row is currently highlighted. Cleared after a few seconds.
   */
  highlightedRowIndex: number = -1;
  private highlightClearTimer: ReturnType<typeof setTimeout> | null = null;
  /**
   * The `__lineage_origin_row` value we're trying to locate. We can't rely on
   * positional math (page = floor(N/pageSize)) because the underlying Iceberg
   * storage doesn't guarantee insertion order on read — page 9 row 5 might not
   * be the row whose lineage value is 45. Instead, we navigate to a best-
   * guess page, inspect the actual lineage values that come back, and iterate
   * outward (binary-search-style) until we find the target row or exhaust the
   * attempt budget.
   */
  private pendingHighlightSourceRow: number | null = null;
  private highlightSearchAttempts = 0;
  private highlightVisitedPages: Set<number> = new Set();
  private static readonly HIGHLIGHT_MAX_ATTEMPTS = 15;

  constructor(
    private modalService: NzModalService,
    private workflowActionService: WorkflowActionService,
    private workflowResultService: WorkflowResultService,
    private resizeService: PanelResizeService,
    private changeDetectorRef: ChangeDetectorRef,
    private sanitizer: DomSanitizer,
    private workflowStatusService: WorkflowStatusService,
    private guiConfigService: GuiConfigService,
    private lineageHighlightService: LineageHighlightService
  ) {}

  ngOnChanges(changes: SimpleChanges): void {
    this.operatorId = changes.operatorId?.currentValue;
    if (this.operatorId) {
      const paginatedResultService = this.workflowResultService.getPaginatedResultService(this.operatorId);
      if (paginatedResultService) {
        this.isFrontPagination = false;
        this.totalNumTuples = paginatedResultService.getCurrentTotalNumTuples();
        this.currentPageIndex = paginatedResultService.getCurrentPageIndex();
        this.changePaginatedResultData();

        this.tableStats = paginatedResultService.getStats();
        this.prevTableStats = this.tableStats;
      }
      // If a lineage-highlight request was set before this component swapped in
      // for the source operator, apply it now.
      this.applyLineageHighlightIfApplicable(this.lineageHighlightService.getPending());
    }
  }

  ngOnInit(): void {
    this.workflowStatusService
      .getStatusUpdateStream()
      .pipe(untilDestroyed(this))
      .subscribe(statusMap => {
        if (this.operatorId && statusMap[this.operatorId]?.operatorState === OperatorState.Completed) {
          this.isOperatorFinished = true;
          this.changeDetectorRef.detectChanges();
        } else {
          this.isOperatorFinished = false;
        }
      });

    this.columnLimit = this.guiConfigService.env.limitColumns;

    this.workflowResultService
      .getResultUpdateStream()
      .pipe(untilDestroyed(this))
      .subscribe(update => {
        if (!this.operatorId) {
          return;
        }
        const opUpdate = update[this.operatorId];
        if (!opUpdate || !isWebPaginationUpdate(opUpdate)) {
          return;
        }
        let columnCount = this.currentColumns?.length;
        if (columnCount) this.widthPercent = (1 / columnCount) * 100 + "%";
        this.isFrontPagination = false;
        this.totalNumTuples = opUpdate.totalNumTuples;
        if (opUpdate.dirtyPageIndices.includes(this.currentPageIndex)) {
          this.changePaginatedResultData();
        }
        this.changeDetectorRef.detectChanges();
      });

    this.workflowResultService
      .getResultTableStats()
      .pipe(untilDestroyed(this))
      .subscribe(([prevStats, currentStats]) => {
        if (!this.operatorId) {
          return;
        }

        if (currentStats[this.operatorId]) {
          this.tableStats = currentStats[this.operatorId];
          if (prevStats[this.operatorId] && this.checkKeys(this.tableStats, prevStats[this.operatorId])) {
            this.prevTableStats = prevStats[this.operatorId];
          } else {
            this.prevTableStats = this.tableStats;
          }
        }
      });

    // React to lineage "jump to source row" requests broadcast from elsewhere.
    this.lineageHighlightService
      .pendingHighlight()
      .pipe(untilDestroyed(this))
      .subscribe(req => this.applyLineageHighlightIfApplicable(req));

    this.resizeService.currentSize.pipe(untilDestroyed(this)).subscribe(size => {
      this.panelHeight = size.height;
      this.adjustPageSizeBasedOnPanelSize(size.height);
      let currentPageNum: number = Math.ceil(this.totalNumTuples / this.pageSize);
      while (this.currentPageIndex > currentPageNum && this.currentPageIndex > 1) {
        this.currentPageIndex -= 1;
      }
    });

    if (this.operatorId) {
      const paginatedResultService = this.workflowResultService.getPaginatedResultService(this.operatorId);
      if (paginatedResultService) {
      }
    }
  }

  checkKeys(
    currentStats: Record<string, Record<string, number>>,
    prevStats: Record<string, Record<string, number>>
  ): boolean {
    let firstSet = Object.keys(currentStats);
    let secondSet = Object.keys(prevStats);

    if (firstSet.length != secondSet.length) {
      return false;
    }

    for (let i = 0; i < firstSet.length; i++) {
      if (firstSet[i] != secondSet[i]) {
        return false;
      }
    }

    return true;
  }

  compare(field: string, stats: string): SafeHtml {
    let current = this.tableStats[field][stats];
    let previous = this.prevTableStats[field][stats];
    let currentStr: string;
    let previousStr: string;

    if (typeof current === "number" && typeof previous === "number") {
      currentStr = current.toFixed(2);
      previousStr = previous !== undefined ? previous.toFixed(2) : currentStr;
    } else {
      currentStr = current.toLocaleString();
      previousStr = previous !== undefined ? previous.toLocaleString() : currentStr;
    }
    let styledValue = "";

    if (this.isOperatorFinished) {
      for (let i = 0; i < currentStr.length; i++) {
        styledValue += `<span style="color: black">${currentStr[i]}</span>`;
      }
      return this.sanitizer.bypassSecurityTrustHtml(styledValue);
    }

    for (let i = 0; i < currentStr.length; i++) {
      const char = currentStr[i];
      const prevChar = previousStr[i];

      if (char !== prevChar) {
        styledValue += `<span style="color: blue">${char}</span>`;
      } else {
        styledValue += `<span style="color: black">${char}</span>`;
      }
    }

    return this.sanitizer.bypassSecurityTrustHtml(styledValue);
  }

  /**
   * Adjusts the number of result rows displayed per page based on the
   * available vertical space of the Texera results panel.
   *
   * The method accounts for fixed UI elements within the panel—such as
   * headers, column navigation controls, pagination, and the search bar—
   * to determine the remaining space available for rendering result rows.
   * The page size is then recalculated using the height of a single table row.
   *
   * To maintain a stable user experience during panel resizes, the current
   * page index is recomputed so that the previously visible results remain
   * in view and the user does not experience an abrupt jump in the dataset.
   *
   * @param panelHeight - The total height (in pixels) of the results panel.
   */
  private adjustPageSizeBasedOnPanelSize(panelHeight: number) {
    const TABLE_HEADER_HEIGHT = 38.62;
    const PANEL_HEADER_HEIGHT = 64.27; // Includes panel title and tab bar
    const COLUMN_NAVIGATION_HEIGHT = 56.6; // Previous/Next columns controls
    const PAGINATION_HEIGHT = 32.63;
    const SEARCH_BAR_HEIGHT_WITH_MARGIN = 77; // Approximate height for search bar and margins
    const ROW_HEIGHT = 38.62;

    const usedHeight =
      TABLE_HEADER_HEIGHT +
      PANEL_HEADER_HEIGHT +
      COLUMN_NAVIGATION_HEIGHT +
      PAGINATION_HEIGHT +
      SEARCH_BAR_HEIGHT_WITH_MARGIN;

    const newPageSize = Math.max(1, Math.floor((panelHeight - usedHeight) / ROW_HEIGHT));

    const oldOffset = (this.currentPageIndex - 1) * this.pageSize;

    this.pageSize = newPageSize;
    this.resizeService.pageSize = newPageSize;

    this.currentPageIndex = Math.floor(oldOffset / newPageSize) + 1;
  }

  /**
   * Callback function for table query params changed event
   *   params containing new page index, new page size, and more
   *   (this function will be called when user switch page)
   *
   * @param params new parameters
   */
  onTableQueryParamsChange(params: NzTableQueryParams) {
    if (this.isFrontPagination) {
      return;
    }
    if (!this.operatorId) {
      return;
    }
    this.currentPageIndex = params.pageIndex;

    this.changePaginatedResultData();
  }

  /**
   * Opens the model to display the row details in
   *  pretty json format when clicked. User can view the details
   *  in a larger, expanded format.
   */
  open(indexInPage: number, rowData: IndexableObject): void {
    const currentRowIndex = indexInPage + (this.currentPageIndex - 1) * this.pageSize;
    // open the modal component
    const modalRef: NzModalRef<RowModalComponent> = this.modalService.create({
      // modal title
      nzTitle: "Row Details",
      nzContent: RowModalComponent,
      nzData: { operatorId: this.operatorId, rowIndex: currentRowIndex }, // set the index value and page size to the modal for navigation
      // prevent browser focusing close button (ugly square highlight)
      nzAutofocus: null,
      // modal footer buttons
      nzFooter: [
        {
          label: "<",
          onClick: () => {
            const component = modalRef.componentInstance;
            if (component) {
              component.rowIndex -= 1;
              this.currentPageIndex = Math.floor(component.rowIndex / this.pageSize) + 1;
              component.ngOnChanges();
            }
          },
          disabled: () => modalRef.componentInstance?.rowIndex === 0,
        },
        {
          label: ">",
          onClick: () => {
            const component = modalRef.componentInstance;
            if (component) {
              component.rowIndex += 1;
              this.currentPageIndex = Math.floor(component.rowIndex / this.pageSize) + 1;
              component.ngOnChanges();
            }
          },
          disabled: () => modalRef.componentInstance?.rowIndex === this.totalNumTuples - 1,
        },
        {
          label: "OK",
          onClick: () => {
            modalRef.destroy();
          },
          type: "primary",
        },
      ],
    });
  }

  // frontend table data must be changed, because:
  // 1. result panel is opened - must display currently selected page
  // 2. user selects a new page - must display new page data
  // 3. current page is dirty - must re-fetch data
  changePaginatedResultData(): void {
    if (!this.operatorId) {
      return;
    }
    const paginatedResultService = this.workflowResultService.getPaginatedResultService(this.operatorId);
    if (!paginatedResultService) {
      return;
    }
    this.isLoadingResult = true;
    paginatedResultService
      .selectPage(this.currentPageIndex, this.pageSize, this.currentColumnOffset, this.columnLimit, this.columnSearch)
      .pipe(untilDestroyed(this))
      .subscribe(pageData => {
        if (this.currentPageIndex === pageData.pageIndex) {
          this.setupResultTable(pageData.table, paginatedResultService.getCurrentTotalNumTuples());
          this.changeDetectorRef.detectChanges();
        }
      });
  }

  /**
   * Updates all the result table properties based on the execution result,
   *  displays a new data table with a new paginator on the result panel.
   *
   * @param resultData rows of the result (may not be all rows if displaying result for workflow completed event)
   * @param totalRowCount
   */
  setupResultTable(resultData: ReadonlyArray<IndexableObject>, totalRowCount: number) {
    if (!this.operatorId) {
      return;
    }
    if (resultData.length < 1) {
      return;
    }

    this.isLoadingResult = false;
    this.changeDetectorRef.detectChanges();

    // creates a shallow copy of the readonly response.result,
    //  this copy will be has type object[] because MatTableDataSource's input needs to be object[]
    this.currentResult = resultData.slice();

    //  1. Get all the column names except '_id', using the first tuple
    //  2. Use those names to generate a list of display columns
    //  3. Pass the result data as array to generate a new data table

    let columns: { columnKey: any; columnText: string }[];

    // Hide internal/lineage columns (anything prefixed with "__") from the visible
    // table — they remain on the row object so per-row affordances (e.g. the "Why?"
    // lineage button) can still access them.
    const columnKeys = Object.keys(resultData[0]).filter(x => x !== "_id" && !x.startsWith("__"));
    columns = columnKeys.map(v => ({ columnKey: v, columnText: v }));

    // generate columnDef from first row, column definition is in order
    this.currentColumns = this.generateColumns(columns);
    this.totalNumTuples = totalRowCount;

    // If a "Jump to source row" request is waiting on this operator, resolve it
    // now against the freshly loaded rows (Iceberg's read order isn't
    // guaranteed, so we match by `__lineage_origin_row` value, not position).
    this.resolvePendingHighlight();
  }

  /**
   * Generates all the column information for the result data table
   *
   * @param columns
   */
  generateColumns(columns: { columnKey: any; columnText: string }[]): TableColumn[] {
    return columns.map((col, index) => ({
      columnDef: col.columnKey,
      header: col.columnText,
      getCell: (row: IndexableObject) => row[col.columnKey].toString(),
    }));
  }

  // Column name emitted by source operators when "Track row-level lineage" is on.
  // Must stay in sync with CSVScanSourceOpDesc.LineageOriginRowColumn (Scala).
  static readonly LINEAGE_ORIGIN_ROW_COLUMN = "__lineage_origin_row";

  /**
   * Whether the currently displayed result rows carry a lineage tag. Used to
   * conditionally render the per-row "Why?" button in the template.
   */
  hasLineage(row: IndexableObject): boolean {
    return row[ResultTableFrameComponent.LINEAGE_ORIGIN_ROW_COLUMN] !== undefined;
  }

  /**
   * True when the currently displayed result rows carry a lineage tag. Used to
   * decide whether to render the extra "Why?" table column at all.
   */
  get hasLineageColumn(): boolean {
    return this.currentResult.length > 0 && this.hasLineage(this.currentResult[0]);
  }

  /**
   * Walks upstream via input links (BFS) from the given operator and returns
   * the closest ancestor that emits row-level lineage (currently, a
   * `CSVFileScan` with `trackLineage` enabled). Returns `undefined` if no such
   * source is reachable — for example because lineage was lost across a
   * Projection or Python UDF, or because the user enabled the checkbox after
   * the workflow was last run.
   */
  private findUpstreamLineageSource(operatorID: string): { id: string; sourceFile?: string } | undefined {
    const graph = this.workflowActionService.getTexeraGraph();
    const visited = new Set<string>();
    const queue: string[] = [operatorID];
    while (queue.length > 0) {
      const id = queue.shift()!;
      if (visited.has(id)) continue;
      visited.add(id);
      const op = graph.getOperator(id);
      if (op) {
        const props = op.operatorProperties ?? {};
        if (op.operatorType === "CSVFileScan" && props["trackLineage"] === true) {
          return { id, sourceFile: props["fileName"] as string | undefined };
        }
      }
      for (const link of graph.getInputLinksByOperatorId(id)) {
        queue.push(link.source.operatorID);
      }
    }
    return undefined;
  }

  /**
   * Apply a pending lineage-highlight request iff it targets *this* operator's
   * result panel. Navigates to the page that *probably* contains the source row
   * (based on emission-order math) and stores the lineage value so
   * `setupResultTable` can find the actual row by matching its
   * `__lineage_origin_row` field — Iceberg may not preserve insertion order on
   * read, so positional navigation alone can land on the wrong row.
   */
  private applyLineageHighlightIfApplicable(req: LineageHighlightRequest | null): void {
    if (!req || !this.operatorId || req.operatorID !== this.operatorId) return;
    const rowOneIndexed = req.sourceRow;
    if (!Number.isFinite(rowOneIndexed) || rowOneIndexed < 1) return;

    // Fresh request: reset the iterative search state.
    this.pendingHighlightSourceRow = rowOneIndexed;
    this.highlightSearchAttempts = 0;
    this.highlightVisitedPages = new Set();
    this.lineageHighlightService.clear();

    // Best-guess page from emission order; if Iceberg returns rows out of
    // insertion order we'll iterate outward in resolvePendingHighlight.
    // NOTE: don't pre-add the user's previous page to visitedPages — the search
    // may legitimately need to walk back into it. Only pages actually inspected
    // for the target value should be marked visited.
    const targetPage = Math.floor((rowOneIndexed - 1) / this.pageSize) + 1;
    if (this.currentPageIndex !== targetPage) {
      this.currentPageIndex = targetPage;
      this.changePaginatedResultData();
    } else {
      this.resolvePendingHighlight();
    }
  }

  /**
   * Searches the currently loaded page for the row whose `__lineage_origin_row`
   * matches `pendingHighlightSourceRow`. If found, marks its index for the
   * highlight class and schedules a clear timer. If not found, leaves the
   * pending value in place — a subsequent page load (e.g. user-driven) can
   * still resolve it. Called from `setupResultTable` after the table data is
   * refreshed.
   */
  private resolvePendingHighlight(): void {
    if (this.pendingHighlightSourceRow === null) return;
    const target = this.pendingHighlightSourceRow;
    const col = ResultTableFrameComponent.LINEAGE_ORIGIN_ROW_COLUMN;

    const lineageVals: number[] = this.currentResult
      .map(r => Number(r[col]))
      .filter(v => Number.isFinite(v));
    const idx = this.currentResult.findIndex(r => Number(r[col]) === target);

    // Diagnostic — visible in DevTools Console.
    // eslint-disable-next-line no-console
    console.log(
      `[lineage] resolve attempt ${this.highlightSearchAttempts + 1}: ` +
        `target=${target}, page=${this.currentPageIndex}, pageSize=${this.pageSize}, ` +
        `matchIdx=${idx}, valsOnPage=`,
      lineageVals
    );

    if (idx >= 0) {
      this.highlightedRowIndex = idx;
      this.pendingHighlightSourceRow = null;
      if (this.highlightClearTimer) clearTimeout(this.highlightClearTimer);
      this.highlightClearTimer = setTimeout(() => {
        this.highlightedRowIndex = -1;
        this.changeDetectorRef.detectChanges();
      }, 6000);
      this.changeDetectorRef.detectChanges();
      return;
    }

    // Not on this page — pick the next page to fetch based on whether target
    // is below or above the values we see. Caps at a few attempts to avoid
    // hammering the backend if values are scattered chaotically.
    this.highlightSearchAttempts++;
    this.highlightVisitedPages.add(this.currentPageIndex);

    if (lineageVals.length === 0) {
      this.giveUpHighlight(target, "no lineage values found on this page");
      return;
    }
    if (this.highlightSearchAttempts >= ResultTableFrameComponent.HIGHLIGHT_MAX_ATTEMPTS) {
      this.giveUpHighlight(
        target,
        `gave up after ${this.highlightSearchAttempts} page fetches`
      );
      return;
    }

    const minVal = Math.min(...lineageVals);
    const maxVal = Math.max(...lineageVals);
    const totalPages = Math.max(1, Math.ceil(this.totalNumTuples / this.pageSize));

    let nextPage: number;
    if (target < minVal) {
      const stepRows = Math.max(this.pageSize, minVal - target);
      const stepPages = Math.max(1, Math.ceil(stepRows / this.pageSize));
      nextPage = Math.max(1, this.currentPageIndex - stepPages);
    } else if (target > maxVal) {
      const stepRows = Math.max(this.pageSize, target - maxVal);
      const stepPages = Math.max(1, Math.ceil(stepRows / this.pageSize));
      nextPage = Math.min(totalPages, this.currentPageIndex + stepPages);
    } else {
      // Target is between min and max but not present — page is sparse w.r.t.
      // lineage. Step one page in the direction of the side with more room.
      nextPage =
        target - minVal < maxVal - target
          ? Math.max(1, this.currentPageIndex - 1)
          : Math.min(totalPages, this.currentPageIndex + 1);
    }

    if (this.highlightVisitedPages.has(nextPage)) {
      // Already tried — give up rather than loop.
      this.giveUpHighlight(target, `would revisit page ${nextPage}`);
      return;
    }

    this.currentPageIndex = nextPage;
    this.changePaginatedResultData();
  }

  private giveUpHighlight(target: number, reason: string): void {
    this.pendingHighlightSourceRow = null;
    // eslint-disable-next-line no-console
    console.warn(`[lineage] gave up locating source row ${target}: ${reason}`);
    this.modalService.warning({
      nzTitle: "Couldn't locate the source row",
      nzContent:
        `Tried up to ${this.highlightSearchAttempts} pages without finding a row ` +
        `whose __lineage_origin_row equals ${target}. Iceberg's read order ` +
        `made the search inconclusive. Last attempted page: ${this.currentPageIndex}.`,
      nzOkText: "OK",
    });
  }

  /**
   * Opens a modal explaining where the selected row came from. If a lineage-
   * emitting source operator can be located upstream, the modal also offers a
   * "Jump to source row" action that selects that operator and asks its result
   * panel to scroll to + highlight the originating row.
   */
  onWhyButtonClick(row: IndexableObject): void {
    const lineageValue = row[ResultTableFrameComponent.LINEAGE_ORIGIN_ROW_COLUMN];
    if (lineageValue === undefined || this.operatorId === undefined) return;
    const sourceRowNum = Number(lineageValue);
    if (!Number.isFinite(sourceRowNum)) return;

    const operator = this.workflowActionService.getTexeraGraph().getOperator(this.operatorId);
    const opLabel = operator?.customDisplayName?.trim() || operator?.operatorType || "this operator";

    const upstream = this.findUpstreamLineageSource(this.operatorId);
    const sourceFile = upstream?.sourceFile;
    const sourceLabel = sourceFile
      ? sourceFile.split("/").pop() ?? sourceFile
      : "the source operator";

    const escape = (s: string) => s.replace(/[&<>"']/g, c =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]!));

    const footer: any[] = [
      {
        label: "Close",
        onClick: () => modalRef.destroy(),
      },
    ];
    if (upstream) {
      footer.push({
        label: "Jump to source row",
        type: "primary",
        onClick: () => {
          this.workflowActionService.highlightOperators(false, upstream.id);
          this.lineageHighlightService.requestHighlight(upstream.id, sourceRowNum);
          modalRef.destroy();
        },
      });
    }

    const modalRef: NzModalRef = this.modalService.create({
      nzTitle: "Why is this row here?",
      nzContent: `
        <div style="line-height: 1.6">
          <p>This row of <b>${escape(opLabel)}</b> originated from
          <b>row ${escape(String(sourceRowNum))}</b> of
          <b>${escape(sourceLabel)}</b>.</p>
          <p style="color:#888;font-size:12px;margin-top:12px">
            The 1-indexed source position was carried through every
            pass-through operator (Filter, Sort) in this workflow's pipeline.
            Many-to-one operators (Join, Aggregate, GroupBy) and Python UDFs
            do not propagate lineage in this version.
          </p>
        </div>`,
      nzFooter: footer,
      nzWidth: 520,
    });
  }

  downloadData(data: any, rowIndex: number, columnIndex: number, columnName: string): void {
    const realRowNumber = (this.currentPageIndex - 1) * this.pageSize + rowIndex;
    const defaultFileName = `${columnName}_${realRowNumber}`;
    const modal = this.modalService.create({
      nzTitle: "Export Data and Save to a Dataset",
      nzContent: ResultExportationComponent,
      nzData: {
        exportType: "data",
        workflowName: this.workflowActionService.getWorkflowMetadata.name,
        defaultFileName: defaultFileName,
        rowIndex: realRowNumber,
        columnIndex: columnIndex,
      },
      nzFooter: null,
    });
  }

  onColumnShiftLeft(): void {
    if (this.currentColumnOffset > 0) {
      this.currentColumnOffset = Math.max(0, this.currentColumnOffset - this.columnLimit);
      this.changePaginatedResultData();
    }
  }

  onColumnShiftRight(): void {
    if (this.currentColumns && this.currentColumns.length === this.columnLimit) {
      this.currentColumnOffset += this.columnLimit;
      this.changePaginatedResultData();
    }
  }

  onColumnSearch(event: Event): void {
    const input = event.target as HTMLInputElement;
    this.columnSearch = input.value;
    this.currentColumnOffset = 0;
    this.changePaginatedResultData();
  }
}
