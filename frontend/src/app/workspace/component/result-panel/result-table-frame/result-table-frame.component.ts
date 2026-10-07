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

import {
  ChangeDetectorRef,
  Component,
  ElementRef,
  Input,
  OnChanges,
  OnInit,
  SimpleChanges,
  ViewChild,
} from "@angular/core";
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
import { isWebPaginationUpdate, OperatorState } from "../../../types/execute-workflow.interface";
import { IndexableObject, TableColumn } from "../../../types/result-table.interface";
import { RowModalComponent } from "../result-panel-modal.component";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DomSanitizer, SafeHtml } from "@angular/platform-browser";
import { isAudioUrl, isVideoUrl, isImageUrl } from "../../../../common/util/media-type.util";
import { ResultExportationComponent } from "../../result-exportation/result-exportation.component";
import { WorkflowStatusService } from "../../../service/workflow-status/workflow-status.service";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { NgIf, NgFor, NgClass, NgSwitch, NgSwitchCase, NgSwitchDefault } from "@angular/common";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { ResultCellIconComponent } from "./result-cell-icon.component";

export type MediaCellType = "video" | "audio" | "image" | "text";

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
    NgSwitch,
    NgSwitchCase,
    NgSwitchDefault,
    NzSpaceCompactItemDirective,
    NzInputDirective,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    ResultCellIconComponent,
    NzTableComponent,
    NzTheadComponent,
    NzTrDirective,
    NgFor,
    NzTableCellDirective,
    NzThMeasureDirective,
    NgClass,
    NzTbodyComponent,
    NzCellEllipsisDirective,
  ],
})
export class ResultTableFrameComponent implements OnInit, OnChanges {
  @Input() operatorId?: string;
  @ViewChild("tableContainer") private tableContainer?: ElementRef<HTMLElement>;

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
  pageSize = 50;
  readonly pageSizeOptions = [10, 25, 50, 100];
  hasReceivedResult = false;
  private resultRequestVersion = 0;
  currentColumnOffset = 0;
  columnLimit = 25;
  columnSearch = "";
  tableStats: Record<string, Record<string, number>> = {};
  prevTableStats: Record<string, Record<string, number>> = {};
  // Fixed-header tables have separate header/body elements. Give both the same
  // column widths so statistics cannot widen just the header's intrinsic layout.
  readonly columnWidth = 180;
  isOperatorFinished: boolean = false;

  // Media type of each cell, precomputed once per row when result data arrives so the
  // template doesn't re-run getCell + regex-based classification on every change
  // detection cycle. Keyed by row object (not row index) because *ngFor iterates
  // basicTable.data, which under front-end pagination is a page-local slice of
  // currentResult whose indices don't line up with currentResult's indices.
  cellMediaTypes: Map<IndexableObject, MediaCellType[]> = new Map();

  constructor(
    private modalService: NzModalService,
    private workflowActionService: WorkflowActionService,
    private workflowResultService: WorkflowResultService,
    private changeDetectorRef: ChangeDetectorRef,
    private sanitizer: DomSanitizer,
    private workflowStatusService: WorkflowStatusService,
    // Read by the template only (the export button's flag); the template can see a protected member.
    protected guiConfigService: GuiConfigService
  ) {}

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.operatorId) {
      ++this.resultRequestVersion;
      this.currentResult = [];
      this.currentColumns = undefined;
      this.cellMediaTypes.clear();
      this.hasReceivedResult = false;
      this.isLoadingResult = false;
      this.isFrontPagination = true;
      this.isOperatorFinished = false;
      this.currentPageIndex = 1;
      this.pageSize = 50;
      this.totalNumTuples = 0;
      this.currentColumnOffset = 0;
      this.columnSearch = "";
      this.tableStats = {};
      this.prevTableStats = {};
    }
    this.operatorId = changes.operatorId?.currentValue;
    if (this.operatorId) {
      const paginatedResultService = this.workflowResultService.getPaginatedResultService(this.operatorId);
      if (paginatedResultService) {
        this.isFrontPagination = false;
        this.totalNumTuples = paginatedResultService.getCurrentTotalNumTuples();
        const pageSize = paginatedResultService.getCurrentPageSize();
        if (pageSize !== undefined && this.pageSizeOptions.includes(pageSize)) {
          this.pageSize = pageSize;
        }
        const pageIndex = paginatedResultService.getCurrentPageIndex();
        const lastPage = Math.max(1, Math.ceil(this.totalNumTuples / this.pageSize));
        this.currentPageIndex = Number.isSafeInteger(pageIndex) ? Math.max(1, Math.min(pageIndex, lastPage)) : 1;
        this.changePaginatedResultData();

        this.tableStats = paginatedResultService.getStats();
        this.prevTableStats = this.tableStats;
      }
    }
  }

  ngOnInit(): void {
    this.workflowStatusService
      .getStateUpdateStream()
      .pipe(untilDestroyed(this))
      .subscribe(stateMap => {
        if (this.operatorId && stateMap[this.operatorId] === OperatorState.Completed) {
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
        this.isFrontPagination = false;
        this.totalNumTuples = opUpdate.totalNumTuples;
        const lastPage = Math.max(1, Math.ceil(this.totalNumTuples / this.pageSize));
        const pageChanged = this.currentPageIndex > lastPage;
        if (pageChanged) {
          this.currentPageIndex = lastPage;
          this.scrollToFirstRow();
        }
        if (this.totalNumTuples === 0) {
          ++this.resultRequestVersion;
          this.setupResultTable([], 0);
        } else if (pageChanged || opUpdate.dirtyPageIndices.includes(this.currentPageIndex)) {
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
   * Callback function for table query params changed event
   *   params containing new page index, new page size, and more
   *   (this function will be called when user switch page)
   *
   * @param params new parameters
   */
  onTableQueryParamsChange(params: NzTableQueryParams) {
    if (
      !this.operatorId ||
      !this.pageSizeOptions.includes(params.pageSize) ||
      !Number.isSafeInteger(params.pageIndex) ||
      params.pageIndex < 1
    ) {
      return;
    }
    const sizeChanged = this.pageSize !== params.pageSize;
    const pageIndex = sizeChanged ? 1 : params.pageIndex;
    const queryChanged = sizeChanged || this.currentPageIndex !== pageIndex;
    this.pageSize = params.pageSize;
    this.currentPageIndex = pageIndex;

    if (queryChanged) {
      this.scrollToFirstRow();
    }
    if (!this.isFrontPagination && queryChanged) {
      this.changePaginatedResultData();
    }
  }

  private scrollToFirstRow(): void {
    const body = this.tableContainer?.nativeElement.querySelector<HTMLElement>(".ant-table-body");
    if (body) {
      body.scrollTop = 0;
    }
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
      nzData: { operatorId: this.operatorId, rowIndex: currentRowIndex, pageSize: this.pageSize },
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
              this.showPageForRow(component.rowIndex);
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
              this.showPageForRow(component.rowIndex);
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

  private showPageForRow(rowIndex: number): void {
    const pageIndex = Math.floor(rowIndex / this.pageSize) + 1;
    if (pageIndex !== this.currentPageIndex) {
      this.currentPageIndex = pageIndex;
      this.scrollToFirstRow();
      if (!this.isFrontPagination) {
        this.changePaginatedResultData();
      }
    }
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
    const requestVersion = ++this.resultRequestVersion;
    const operatorId = this.operatorId;
    paginatedResultService
      .selectPage(this.currentPageIndex, this.pageSize, this.currentColumnOffset, this.columnLimit, this.columnSearch)
      .pipe(untilDestroyed(this))
      .subscribe(pageData => {
        if (
          requestVersion === this.resultRequestVersion &&
          operatorId === this.operatorId &&
          this.currentPageIndex === pageData.pageIndex
        ) {
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
    this.isLoadingResult = false;
    this.hasReceivedResult = true;
    this.totalNumTuples = totalRowCount;
    if (resultData.length < 1) {
      this.currentResult = [];
      this.cellMediaTypes.clear();
      return;
    }

    // creates a shallow copy of the readonly response.result,
    //  this copy will be has type object[] because MatTableDataSource's input needs to be object[]
    this.currentResult = resultData.slice();

    //  1. Get all the column names except '_id', using the first tuple
    //  2. Use those names to generate a list of display columns
    //  3. Pass the result data as array to generate a new data table

    let columns: { columnKey: any; columnText: string }[];

    const columnKeys = Object.keys(resultData[0]).filter(x => x !== "_id");
    columns = columnKeys.map(v => ({ columnKey: v, columnText: v }));

    // generate columnDef from first row, column definition is in order
    this.currentColumns = this.generateColumns(columns);
    this.totalNumTuples = totalRowCount;

    this.cellMediaTypes = new Map(
      this.currentResult.map(row => [row, this.currentColumns!.map(column => this.classifyCell(column.getCell(row)))])
    );
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

  downloadData(data: any, rowIndex: number, columnIndex: number, columnName: string): void {
    // A cell belongs to the operator whose results this frame is showing. Without one there is
    // nothing to scope an export to, and the dialog would open only to export nothing.
    if (!this.operatorId) {
      return;
    }
    const realRowNumber = (this.currentPageIndex - 1) * this.pageSize + rowIndex;
    const defaultFileName = `${columnName}_${realRowNumber}`;
    const modal = this.modalService.create({
      nzTitle: "Export Data and Save to a Dataset",
      nzContent: ResultExportationComponent,
      nzData: {
        exportType: "data",
        workflowName: this.workflowActionService.getWorkflowMetadata()?.name,
        defaultFileName: defaultFileName,
        rowIndex: realRowNumber,
        columnIndex: columnIndex,
        // Named rather than left to the canvas selection, which answers a different question:
        // what the user has selected. This frame also mounts on the Form View, where nothing is
        // selected until the user clicks a step, so the export found an empty scope and the
        // button did nothing.
        operatorIds: [this.operatorId],
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

  isVideoCell(value: unknown): boolean {
    return typeof value === "string" && isVideoUrl(value);
  }

  isAudioCell(value: unknown): boolean {
    return typeof value === "string" && isAudioUrl(value);
  }

  isImageCell(value: unknown): boolean {
    return typeof value === "string" && isImageUrl(value);
  }

  private classifyCell(value: unknown): MediaCellType {
    if (this.isVideoCell(value)) return "video";
    if (this.isAudioCell(value)) return "audio";
    if (this.isImageCell(value)) return "image";
    return "text";
  }

  // O(1) lookup into the precomputed cellMediaTypes map, used by the template
  // instead of calling isVideoCell/isAudioCell/isImageCell on every change detection cycle.
  getCellMediaType(row: IndexableObject, columnIndex: number): MediaCellType {
    return this.cellMediaTypes.get(row)?.[columnIndex] ?? "text";
  }

  onColumnSearch(event: Event): void {
    const input = event.target as HTMLInputElement;
    this.columnSearch = input.value;
    this.currentColumnOffset = 0;
    this.changePaginatedResultData();
  }
}
