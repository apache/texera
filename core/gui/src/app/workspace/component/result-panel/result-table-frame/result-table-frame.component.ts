import { Component, Input, OnChanges, OnInit, SimpleChanges } from "@angular/core";
import { NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import { NzTableQueryParams } from "ng-zorro-antd/table";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowResultService } from "../../../service/workflow-result/workflow-result.service";
import { PanelResizeService } from "../../../service/workflow-result/panel-resize/panel-resize.service";
import { isWebPaginationUpdate } from "../../../types/execute-workflow.interface";
import { IndexableObject, TableColumn } from "../../../types/result-table.interface";
import { RowModalComponent } from "../result-panel-modal.component";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DomSanitizer, SafeHtml } from "@angular/platform-browser";
import { style } from "@angular/animations";
import { isBase64, isBinary, trimAndFormatData } from "src/app/common/util/json";
import { ResultExportationComponent } from "../../result-exportation/result-exportation.component";

export const TABLE_COLUMN_TEXT_LIMIT = 100;
export const PRETTY_JSON_TEXT_LIMIT = 50000;

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
  panelHeight = 0;
  tableStats: Record<string, Record<string, number>> = {};
  prevTableStats: Record<string, Record<string, number>> = {};
  widthPercent: string = "";
  sinkStorageMode: string = "";

  constructor(
    private executeWorkflowService: ExecuteWorkflowService,
    private modalService: NzModalService,
    private workflowActionService: WorkflowActionService,
    private workflowResultService: WorkflowResultService,
    private resizeService: PanelResizeService,
    private sanitizer: DomSanitizer
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
    }
  }

  ngOnInit(): void {
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

    this.workflowResultService
      .getSinkStorageMode()
      .pipe(untilDestroyed(this))
      .subscribe(sinkStorageMode => {
        this.sinkStorageMode = sinkStorageMode;
        this.adjustPageSizeBasedOnPanelSize(this.panelHeight);
      });

    this.resizeService.currentSize.pipe(untilDestroyed(this)).subscribe(size => {
      this.panelHeight = size.height;
      this.adjustPageSizeBasedOnPanelSize(size.height);
      let currentPageNum: number = Math.ceil(this.totalNumTuples / this.pageSize);
      while (this.currentPageIndex > currentPageNum && this.currentPageIndex > 1) {
        this.currentPageIndex -= 1;
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
    let currentStr = "";
    let previousStr = "";

    if (typeof current === "number" && typeof previous === "number") {
      currentStr = current.toFixed(2);
      previousStr = previous !== undefined ? previous.toFixed(2) : currentStr;
    } else {
      currentStr = current.toLocaleString();
      previousStr = previous !== undefined ? previous.toLocaleString() : currentStr;
    }
    let styledValue = "";

    for (let i = 0; i < currentStr.length; i++) {
      const char = currentStr[i];
      const prevChar = previousStr[i];

      if (char !== prevChar) {
        styledValue += `<span style="color: red">${char}</span>`;
      } else {
        styledValue += `<span style="color: black">${char}</span>`;
      }
    }

    return this.sanitizer.bypassSecurityTrustHtml(styledValue);
  }

  private adjustPageSizeBasedOnPanelSize(panelHeight: number) {
    const rowHeight = 39; // use the rendered height of a row.
    let extra: number;

    if (this.sinkStorageMode == "mongodb") {
      extra = Math.floor((panelHeight - 88 - 170) / rowHeight);
    } else {
      extra = Math.floor((panelHeight - 170) / rowHeight);
    }

    if (extra < 0) {
      extra = 0;
    }
    this.pageSize = 1 + extra;
    this.resizeService.pageSize = this.pageSize;
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
      .selectPage(this.currentPageIndex, this.pageSize)
      .pipe(untilDestroyed(this))
      .subscribe(pageData => {
        if (this.currentPageIndex === pageData.pageIndex) {
          this.setupResultTable(pageData.table, paginatedResultService.getCurrentTotalNumTuples());
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
  }

  /**
   * Generates all the column information for the result data table
   *
   * @param columns
   */
  generateColumns(columns: { columnKey: any; columnText: string }[]): TableColumn[] {
    return columns.map(col => ({
      columnDef: col.columnKey,
      header: col.columnText,
      getCell: (row: IndexableObject) => {
        if (row[col.columnKey] === null) {
          return "NULL"; // Explicitly show NULL for null values
        } else if (row[col.columnKey] !== undefined) {
          return this.trimTableCell(row[col.columnKey]);
        } else {
          return ""; // Keep empty string for undefined values
        }
      },
    }));
  }

  trimTableCell(cellContent: any): string {
    return trimAndFormatData(cellContent, TABLE_COLUMN_TEXT_LIMIT);
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
}
