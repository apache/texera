import { Component, OnDestroy, OnInit } from '@angular/core';
import { isEqual } from 'lodash-es';
import { NzModalRef, NzModalService } from 'ng-zorro-antd/modal';
import { NzTableQueryParams } from 'ng-zorro-antd/table';
import { Subscription } from 'rxjs';
import { assertType } from '../../../../common/util/assert';
import { trimDisplayJsonData } from '../../../../common/util/json';
import { ExecuteWorkflowService } from '../../../service/execute-workflow/execute-workflow.service';
import { ResultPanelToggleService } from '../../../service/result-panel-toggle/result-panel-toggle.service';
import { WorkflowActionService } from '../../../service/workflow-graph/model/workflow-action.service';
import { DEFAULT_PAGE_SIZE, WorkflowResultService } from '../../../service/workflow-result/workflow-result.service';
import { isWebPaginationUpdate } from '../../../types/execute-workflow.interface';
import { IndexableObject, TableColumn } from '../../../types/result-table.interface';
import { RowModalComponent } from '../result-panel-modal.component';

@Component({
  selector: 'texera-result-table-frame',
  templateUrl: './result-table-frame.component.html',
  styleUrls: ['./result-table-frame.component.scss']
})
export class ResultTableFrameComponent implements OnInit, OnDestroy {
  // the highlighted operator ID for display result table / visualization / breakpoint
  resultPanelOperatorID: string | undefined;

  // display result table
  currentColumns: TableColumn[] | undefined;
  currentResult: object[] = [];
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
  pageSize = DEFAULT_PAGE_SIZE;
  resultUpdateSubscription: Subscription | undefined;

  private readonly TABLE_COLUMN_TEXT_LIMIT: number = 1000;
  private readonly PRETTY_JSON_TEXT_LIMIT: number = 50000;

  constructor(
    private executeWorkflowService: ExecuteWorkflowService,
    private modalService: NzModalService,
    private resultPanelToggleService: ResultPanelToggleService,
    private workflowActionService: WorkflowActionService,
    private workflowResultService: WorkflowResultService
  ) { }

  ngOnInit(): void {
    // update highlighted operator
    const highlightedOperators = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
    this.resultPanelOperatorID = highlightedOperators.length === 1 ? highlightedOperators[0] : undefined;

    // display result table by default
    if (this.resultPanelOperatorID) {
      const paginatedResultService = this.workflowResultService.getPaginatedResultService(this.resultPanelOperatorID);
      this.resultUpdateSubscription = this.workflowResultService.getResultUpdateStream().subscribe(update => {
        if (!this.resultPanelOperatorID) {
          return;
        }
        const opUpdate = update[this.resultPanelOperatorID];
        if (!opUpdate || !isWebPaginationUpdate(opUpdate)) {
          // clear result panel if currently display results
          if (this.totalNumTuples > 0) {
            this.totalNumTuples = 0;
            this.currentPageIndex = 1;
            this.currentColumns = undefined;
            this.currentResult = [];
          }
          return;
        }
        this.totalNumTuples = opUpdate.totalNumTuples;
        this.isFrontPagination = false;
        if (opUpdate.dirtyPageIndices.includes(this.currentPageIndex)) {
          this.changePaginatedResultData();
        }
      });
      if (paginatedResultService) {
        this.isFrontPagination = false;
        this.totalNumTuples = paginatedResultService.getCurrentTotalNumTuples();
        this.currentPageIndex = paginatedResultService.getCurrentPageIndex();
        this.changePaginatedResultData();
      }
    }
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
    if (!this.resultPanelOperatorID) {
      return;
    }
    this.currentPageIndex = params.pageIndex;

    this.changePaginatedResultData();
  }

  /**
   * Opens the ng-bootstrap model to display the row details in
   *  pretty json format when clicked. User can view the details
   *  in a larger, expanded format.
   *
   * @param rowData the object containing the data of the current row in columnDef and cellData pairs
   */
  open(rowData: object): void {

    let selectedRowIndex = this.currentResult.findIndex(eachRow => isEqual(eachRow, rowData));

    // generate a new row data that shortens the column text to limit rendering time for pretty json
    const rowDataCopy = trimDisplayJsonData(rowData as IndexableObject, this.PRETTY_JSON_TEXT_LIMIT);

    // open the modal component
    const modalRef: NzModalRef = this.modalService.create({
      // modal title
      nzTitle: 'Row Details',
      nzContent: RowModalComponent,
      // set component @Input attributes
      nzComponentParams: {
        // set the currentDisplayRowData of the modal to be the data of clicked row
        currentDisplayRowData: rowDataCopy,
        // set the index value and page size to the modal for navigation
        currentDisplayRowIndex: selectedRowIndex
      },
      // prevent browser focusing close button (ugly square highlight)
      nzAutofocus: null,
      // modal footer buttons
      nzFooter: [
        {
          label: '<',
          onClick: () => {
            selectedRowIndex -= 1;
            assertType<RowModalComponent>(modalRef.componentInstance);
            modalRef.componentInstance.currentDisplayRowData = this.currentResult[selectedRowIndex];
          },
          disabled: () => selectedRowIndex === 0
        },
        {
          label: '>',
          onClick: () => {
            selectedRowIndex += 1;
            assertType<RowModalComponent>(modalRef.componentInstance);
            modalRef.componentInstance.currentDisplayRowData = this.currentResult[selectedRowIndex];
          },
          disabled: () => selectedRowIndex === this.currentResult.length - 1
        },
        { label: 'OK', onClick: () => { modalRef.destroy(); }, type: 'primary' }
      ]
    });
  }


  // frontend table data must be changed, because:
  // 1. result panel is opened - must display currently selected page
  // 2. user selects a new page - must display new page data
  // 3. current page is dirty - must re-fetch data
  changePaginatedResultData(): void {
    if (!this.resultPanelOperatorID) {
      return;
    }
    const paginatedResultService = this.workflowResultService.getPaginatedResultService(this.resultPanelOperatorID);
    if (!paginatedResultService) {
      return;
    }
    this.isLoadingResult = true;
    paginatedResultService.selectPage(this.currentPageIndex, DEFAULT_PAGE_SIZE).subscribe(pageData => {
      if (this.currentPageIndex === pageData.pageIndex) {
        this.setupResultTable(pageData.table, paginatedResultService.getCurrentTotalNumTuples());
      }
    });
  }

  ngOnDestroy(): void {
    if (this.resultUpdateSubscription !== undefined) {
      this.resultUpdateSubscription.unsubscribe();
      this.resultUpdateSubscription = undefined;
    }
  }

  /**
   * Updates all the result table properties based on the execution result,
   *  displays a new data table with a new paginator on the result panel.
   *
   * @param resultData rows of the result (may not be all rows if displaying result for workflow completed event)
   */
  private setupResultTable(resultData: ReadonlyArray<object>, totalRowCount: number) {
    if (!this.resultPanelOperatorID) {
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

    let columns: { columnKey: any, columnText: string }[];

    const columnKeys = Object.keys(resultData[0]).filter(x => x !== '_id');
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
  private generateColumns(columns: { columnKey: any, columnText: string }[]): TableColumn[] {
    return columns.map(col => ({
      columnDef: col.columnKey,
      header: col.columnText,
      getCell: (row: IndexableObject) => {
        if (row[col.columnKey] !== null && row[col.columnKey] !== undefined) {
          return this.trimTableCell(row[col.columnKey].toString());
        } else {
          // allowing null value from backend
          return '';
        }
      }
    }));
  }

  private trimTableCell(cellContent: string): string {
    if (cellContent.length > this.TABLE_COLUMN_TEXT_LIMIT) {
      return cellContent.substring(0, this.TABLE_COLUMN_TEXT_LIMIT);
    }
    return cellContent;
  }
}
