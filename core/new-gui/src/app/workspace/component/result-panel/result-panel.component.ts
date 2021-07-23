import { Component, Input } from '@angular/core';
import deepMap from 'deep-map';
import { isEqual } from 'lodash';
import { NzModalRef, NzModalService } from 'ng-zorro-antd/modal';
import { NzTableQueryParams } from 'ng-zorro-antd/table';
import { Observable } from 'rxjs/Observable';
import { assertType } from 'src/app/common/util/assert';
import { ExecuteWorkflowService } from '../../service/execute-workflow/execute-workflow.service';
import { ResultPanelToggleService } from '../../service/result-panel-toggle/result-panel-toggle.service';
import { WorkflowActionService } from '../../service/workflow-graph/model/workflow-action.service';
import { ExecutionState, isWebPaginationUpdate, ExecutionStateInfo } from '../../types/execute-workflow.interface';
import { IndexableObject, TableColumn } from '../../types/result-table.interface';
import { BreakpointTriggerInfo } from '../../types/workflow-common.interface';
import { WorkflowResultService, DEFAULT_PAGE_SIZE } from '../../service/workflow-result/workflow-result.service';
import { Subscription } from 'rxjs';

/**
 * ResultPanelComponent is the bottom level area that displays the
 *  execution result of a workflow after the execution finishes.
 *
 * The Component will display the result in an excel table format,
 *  where each row represents a result from the workflow,
 *  and each column represents the type of result the workflow returns.
 *
 * Clicking each row of the result table will create an pop-up window
 *  and display the detail of that row in a pretty json format.
 *
 * @author Henry Chen
 * @author Zuozhi Wang
 */
@Component({
  selector: 'texera-result-panel',
  templateUrl: './result-panel.component.html',
  styleUrls: ['./result-panel.component.scss']
})
export class ResultPanelComponent {
  public static readonly DEFAULT_PAGE_SIZE: number = 10;

  private static readonly PRETTY_JSON_TEXT_LIMIT: number = 50000;
  private static readonly TABLE_COLUMN_TEXT_LIMIT: number = 1000;

  public pageSize = DEFAULT_PAGE_SIZE;

  public showResultPanel: boolean = false;

  // display error message:
  public errorMessages: Readonly<Record<string, string>> | undefined;

  // display result table
  public currentColumns: TableColumn[] | undefined;
  public currentResult: object[] = [];

  // display visualization
  public chartType: string | undefined;

  // display breakpoint
  public breakpointTriggerInfo: BreakpointTriggerInfo | undefined;
  public breakpointAction: boolean = false;

  // the highlighted operator ID for display result table / visualization / breakpoint
  public resultPanelOperatorID: string | undefined;

  // paginator section, used when displaying rows

  // this attribute stores whether front-end should handle pagination
  //   if false, it means the pagination is managed by the server
  //   see https://ng.ant.design/components/table/en#components-table-demo-ajax
  //   for more details
  public isFrontPagination: boolean = true;
  public isLoadingResult: boolean = false;
  // this starts from **ONE**, not zero
  public currentPageIndex: number = 1;
  public totalNumTuples: number = 0;

  public resultUpdateSubscription: Subscription | undefined;

  constructor(
    private executeWorkflowService: ExecuteWorkflowService,
    private modalService: NzModalService,
    private resultPanelToggleService: ResultPanelToggleService,
    private workflowActionService: WorkflowActionService,
    private workflowResultService: WorkflowResultService,
  ) {

    Observable.merge(
      this.executeWorkflowService.getExecutionStateStream().filter(event => this.needRerenderOnStateChange(event)),
      this.workflowActionService.getJointGraphWrapper().getJointOperatorHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointOperatorUnhighlightStream(),
      this.resultPanelToggleService.getToggleChangeStream()
    ).subscribe(trigger => {
      this.rerenderResultPanel();
    });

    this.executeWorkflowService.getExecutionStateStream().subscribe(event => {
      if (event.current.state === ExecutionState.BreakpointTriggered) {
        const breakpointOperator = this.executeWorkflowService.getBreakpointTriggerInfo()?.operatorID;
        if (breakpointOperator) {
          this.workflowActionService.getJointGraphWrapper().highlightOperators(breakpointOperator);
        }
        this.resultPanelToggleService.openResultPanel();
      }
      if (event.current.state === ExecutionState.Failed) {
        this.resultPanelToggleService.openResultPanel();
      }
      if (event.current.state === ExecutionState.Completed || event.current.state === ExecutionState.Running) {
        const sinkOperators = this.workflowActionService.getTexeraGraph().getAllOperators()
          .filter(op => op.operatorType.toLowerCase().includes('sink'));
        if (sinkOperators.length > 0 && ! this.resultPanelOperatorID) {
          const currentlyHighlighted = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
          this.workflowActionService.getJointGraphWrapper().unhighlightOperators(...currentlyHighlighted);
          this.workflowActionService.getJointGraphWrapper().highlightOperators(sinkOperators[0].operatorID);
        }
        this.resultPanelToggleService.openResultPanel();
      }
    });

  }

  public needRerenderOnStateChange(event: { previous: ExecutionStateInfo, current: ExecutionStateInfo }): boolean {
    // transitioning from any state to failed state
    if (event.current.state === ExecutionState.Failed) {
      return true;
    }
    // transitioning from any state to breakpoint triggered state
    if (event.current.state === ExecutionState.BreakpointTriggered) {
      return true;
    }
    // transition from uninitialized / completed to anything else indicates a new execution of the workflow
    if (event.previous.state === ExecutionState.Uninitialized || event.previous.state === ExecutionState.Completed) {
      return true;
    }
    return false;
  }

  public rerenderResultPanel(): void {
    // update highlighted operator
    const highlightedOperators = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
    this.resultPanelOperatorID = highlightedOperators.length === 1 ? highlightedOperators[0] : undefined;

    // current result panel is closed, do nothing
    this.showResultPanel = this.resultPanelToggleService.isResultPanelOpen();
    if (!this.showResultPanel) {
      return;
    }

    // clear everything, prepare for state change
    this.clearResultPanel();

    const executionState = this.executeWorkflowService.getExecutionState();

    if (executionState.state === ExecutionState.Failed) {
      this.errorMessages = this.executeWorkflowService.getErrorMessages();
    } else if (executionState.state === ExecutionState.BreakpointTriggered) {
      const breakpointTriggerInfo = this.executeWorkflowService.getBreakpointTriggerInfo();
      if (this.resultPanelOperatorID && this.resultPanelOperatorID === breakpointTriggerInfo?.operatorID) {
        this.breakpointTriggerInfo = breakpointTriggerInfo;
        this.breakpointAction = true;
        const result = breakpointTriggerInfo.report.map(r => r.faultedTuple.tuple).filter(t => t !== undefined);
        this.setupResultTable(result, result.length);
        const errorsMessages: Record<string, string> = {};
        breakpointTriggerInfo.report.forEach(r => {
          const pathsplitted = r.actorPath.split('/');
          const workerName = pathsplitted[pathsplitted.length - 1];
          const workerText = 'Worker ' + workerName + ':                ';
          if (r.messages.toString().toLowerCase().includes('exception')) {
            errorsMessages[workerText] = r.messages.toString();
          }
        });
        this.errorMessages = errorsMessages;
      }
    } else {
      // display result table by default
      if (this.resultPanelOperatorID) {
        const paginatedResultService = this.workflowResultService.getPaginatedResultService(this.resultPanelOperatorID);
        const resultService = this.workflowResultService.getResultService(this.resultPanelOperatorID);

        this.resultUpdateSubscription = this.workflowResultService.getResultUpdateStream().subscribe(update => {
          if (! this.resultPanelOperatorID) {
            return;
          }
          const opUpdate = update[this.resultPanelOperatorID];

          if (! opUpdate || ! isWebPaginationUpdate(opUpdate)) {
            // clear result panel if currently display results
            if (this.totalNumTuples > 0) {
              this.clearResultPanel();
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

        } else if (resultService && resultService.getChartType()) {
          this.chartType = resultService.getChartType();
        }
      }
    }
  }

  public clearResultPanel(): void {
    this.errorMessages = undefined;

    this.currentColumns = undefined;
    this.currentResult = [];

    this.chartType = undefined;
    this.breakpointTriggerInfo = undefined;
    this.breakpointAction = false;

    this.isFrontPagination = true;

    this.currentPageIndex = 1;

    this.totalNumTuples = 0;
    this.isLoadingResult = false;

    if (this.resultUpdateSubscription !== undefined) {
      this.resultUpdateSubscription.unsubscribe();
      this.resultUpdateSubscription = undefined;
    }
  }

  /**
   * Opens the ng-bootstrap model to display the row details in
   *  pretty json format when clicked. User can view the details
   *  in a larger, expanded format.
   *
   * @param rowData the object containing the data of the current row in columnDef and cellData pairs
   */
  public open(rowData: object): void {

    let selectedRowIndex = this.currentResult.findIndex(eachRow => isEqual(eachRow, rowData));

    // generate a new row data that shortens the column text to limit rendering time for pretty json
    const rowDataCopy = ResultPanelComponent.trimDisplayJsonData(rowData as IndexableObject);

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

  public onClickSkipTuples(): void {
    this.executeWorkflowService.skipTuples();
    this.breakpointAction = false;
  }
  /**
   * Callback function for table query params changed event
   *   params containing new page index, new page size, and more
   *   (this function will be called when user switch page)
   *
   * @param params new parameters
   */
  public onTableQueryParamsChange(params: NzTableQueryParams) {
    if (this.isFrontPagination) {
      return;
    }
    if (!this.resultPanelOperatorID) {
      return;
    }
    this.currentPageIndex = params.pageIndex;

    this.changePaginatedResultData();
  }

  // frontend table data must be changed, because:
  // 1. result panel is opened - must display currently selected page
  // 2. user selects a new page - must display new page data
  // 3. current page is dirty - must re-fetch data
  public changePaginatedResultData(): void {
    if (! this.resultPanelOperatorID) {
      return;
    }
    const paginatedResultService = this.workflowResultService.getPaginatedResultService(this.resultPanelOperatorID);
    if (! paginatedResultService) {
      return;
    }
    this.isLoadingResult = true;
    paginatedResultService.selectPage(this.currentPageIndex, DEFAULT_PAGE_SIZE).subscribe(pageData => {
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
   */
  private setupResultTable(resultData: ReadonlyArray<object>, totalRowCount: number) {
    if (! this.resultPanelOperatorID) {
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
    this.currentColumns = ResultPanelComponent.generateColumns(columns);
    this.totalNumTuples = totalRowCount;

  }

  /**
   * Generates all the column information for the result data table
   *
   * @param columns
   */
  private static generateColumns(columns: { columnKey: any, columnText: string }[]): TableColumn[] {
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

  private static trimTableCell(cellContent: string): string {
    if (cellContent.length > this.TABLE_COLUMN_TEXT_LIMIT) {
      return cellContent.substring(0, this.TABLE_COLUMN_TEXT_LIMIT);
    }
    return cellContent;
  }

  /**
   * This method will recursively iterate through the content of the row data and shorten
   *  the column string if it exceeds a limit that will excessively slow down the rendering time
   *  of the UI.
   *
   * This method will return a new copy of the row data that will be displayed on the UI.
   *
   * @param rowData original row data returns from execution
   */
  private static trimDisplayJsonData(rowData: IndexableObject): object {
    const rowDataTrimmed = deepMap<object>(rowData, value => {
      if (typeof value === 'string' && value.length > this.PRETTY_JSON_TEXT_LIMIT) {
        return value.substring(0, this.PRETTY_JSON_TEXT_LIMIT) + '...';
      } else {
        return value;
      }
    });
    return rowDataTrimmed;
  }

}

/**
 *
 * NgbModalComponent is the pop-up window that will be
 *  displayed when the user clicks on a specific row
 *  to show the displays of that row.
 *
 * User can exit the pop-up window by
 *  1. Clicking the dismiss button on the top-right hand corner
 *      of the Modal
 *  2. Clicking the `Close` button at the bottom-right
 *  3. Clicking any shaded area that is not the pop-up window
 *  4. Pressing `Esc` button on the keyboard
 */
@Component({
  selector: 'texera-row-modal-content',
  templateUrl: './result-panel-modal.component.html',
  styleUrls: ['./result-panel.component.scss']
})
export class RowModalComponent {
  // when modal is opened, currentDisplayRow will be passed as
  //  componentInstance to this NgbModalComponent to display
  //  as data table.
  @Input() currentDisplayRowData: object = {};

  // Index of currentDisplayRowData in currentResult
  @Input() currentDisplayRowIndex: number = 0;

  constructor(public modal: NzModalRef<any, number>) { }

}

