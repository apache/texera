import { Component, Input } from '@angular/core';
import deepMap from 'deep-map';
import { isEqual } from 'lodash';
import { NzModalRef, NzModalService } from 'ng-zorro-antd/modal';
import { NzTableQueryParams } from 'ng-zorro-antd/table';
import { Observable } from 'rxjs/Observable';
import { assertType } from 'src/app/common/util/assert';
import { sessionGetObject, sessionRemoveObject, sessionSetObject } from 'src/app/common/util/storage';
import { ExecuteWorkflowService } from '../../service/execute-workflow/execute-workflow.service';
import { ResultPanelToggleService } from '../../service/result-panel-toggle/result-panel-toggle.service';
import { WorkflowActionService } from '../../service/workflow-graph/model/workflow-action.service';
import { WorkflowWebsocketService } from '../../service/workflow-websocket/workflow-websocket.service';
import { ExecutionState } from '../../types/execute-workflow.interface';
import { IndexableObject, PAGINATION_INFO_STORAGE_KEY, ResultPaginationInfo, TableColumn } from '../../types/result-table.interface';
import { BreakpointTriggerInfo } from '../../types/workflow-common.interface';

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
  private static readonly PRETTY_JSON_TEXT_LIMIT: number = 50000;
  private static readonly TABLE_COLUMN_TEXT_LIMIT: number = 1000;

  public showResultPanel: boolean = false;

  // display error message:
  public errorMessages: Readonly<Record<string, string>> | undefined;

  // display result table
  public currentColumns: TableColumn[] | undefined;
  public currentDisplayColumns: string[] | undefined;
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
  public currentPageSize: number = 10;
  // this starts from **ONE**, not zero
  public currentPageIndex: number = 1;
  public total: number = 0;
  private operatorID: string = '';

  constructor(
    private executeWorkflowService: ExecuteWorkflowService,
    private modalService: NzModalService,
    private resultPanelToggleService: ResultPanelToggleService,
    private workflowActionService: WorkflowActionService,
    private workflowWebsocketService: WorkflowWebsocketService
  ) {
    const activeStates: ExecutionState[] = [ExecutionState.Completed, ExecutionState.Failed, ExecutionState.BreakpointTriggered];
    Observable.merge(
      this.executeWorkflowService.getExecutionStateStream(),
      this.workflowActionService.getJointGraphWrapper().getJointOperatorHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointOperatorUnhighlightStream(),
      this.resultPanelToggleService.getToggleChangeStream()
    ).subscribe(trigger => this.displayResultPanel());

    this.executeWorkflowService.getExecutionStateStream().subscribe(event => {
      console.log(event.current.state);
      console.log(event.current);
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
      if (event.current.state === ExecutionState.Completed) {
        const sinkOperators = this.workflowActionService.getTexeraGraph().getAllOperators()
          .filter(op => op.operatorType.toLowerCase().includes('sink'));
        if (sinkOperators.length > 0) {
          this.workflowActionService.getJointGraphWrapper().highlightOperators(sinkOperators[0].operatorID);
        }
        this.resultPanelToggleService.openResultPanel();
      }
    });

    this.workflowWebsocketService.websocketEvent().subscribe(websocketEvent => {
      if (websocketEvent.type !== 'PaginatedResultEvent') {
        return;
      }

      const highlightedOperators = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();

      for (const result of websocketEvent.paginatedResults) {
        if (result.operatorID === highlightedOperators[0]) {
          this.total = result.totalRowCount;
          this.currentResult = result.table.slice();
          this.isLoadingResult = false;
          return;
        }
      }
    });

    // clear session storage for refresh
    sessionRemoveObject(PAGINATION_INFO_STORAGE_KEY);
  }

  public displayResultPanel(): void {
    // current result panel is closed, do nothing
    this.showResultPanel = this.resultPanelToggleService.isResultPanelOpen();
    if (!this.showResultPanel) {
      return;
    }

    // clear everything, prepare for state change
    this.clearResultPanel();

    const executionState = this.executeWorkflowService.getExecutionState();
    const highlightedOperators = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
    this.resultPanelOperatorID = highlightedOperators.length === 1 ? highlightedOperators[0] : undefined;

    if (executionState.state === ExecutionState.Failed) {
      this.errorMessages = this.executeWorkflowService.getErrorMessages();
    } else if (executionState.state === ExecutionState.BreakpointTriggered) {
      const breakpointTriggerInfo = this.executeWorkflowService.getBreakpointTriggerInfo();
      if (this.resultPanelOperatorID && this.resultPanelOperatorID === breakpointTriggerInfo?.operatorID) {
        this.breakpointTriggerInfo = breakpointTriggerInfo;
        this.breakpointAction = true;
        const result = breakpointTriggerInfo.report.map(r => r.faultedTuple.tuple).filter(t => t !== undefined);
        this.setupResultTable(result, result.length, highlightedOperators[0]);
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
    } else if (executionState.state === ExecutionState.Completed) {
      if (this.resultPanelOperatorID) {
        const result = executionState.resultMap.get(this.resultPanelOperatorID);
        if (result) {
          this.chartType = result.chartType;
          this.isFrontPagination = false;
          this.setupResultTable(result.table, result.totalRowCount, highlightedOperators[0]);
        }
      }
    } else if (executionState.state === ExecutionState.Paused) {
      if (this.resultPanelOperatorID) {
        const result = executionState.currentTuples[this.resultPanelOperatorID]?.tuples;
        if (result) {
          const resultTable: string[][] = [];
          result.forEach(workerTuple => {
            const updatedTuple: string[] = [];
            updatedTuple.push(workerTuple.workerID);
            updatedTuple.push(...workerTuple.tuple);
            resultTable.push(updatedTuple);
          });
          this.setupResultTable(resultTable, resultTable.length, highlightedOperators[0]);
        }
      }
    }
  }

  public clearResultPanel(): void {
    // store result into session storage so that they could be restored
    //   when user click the "view result" operator again
    const resultPaginationInfo = sessionGetObject<ResultPaginationInfo>(PAGINATION_INFO_STORAGE_KEY);
    if (resultPaginationInfo && !resultPaginationInfo.newWorkflowExecuted && this.currentResult.length > 0) {
      sessionSetObject(PAGINATION_INFO_STORAGE_KEY, {
        ...resultPaginationInfo,
        // We have to turn it into array first because JSON.stringfy(someMap) will produce an empty object
        viewResultOperatorInfoMap: Array.from((new Map(resultPaginationInfo.viewResultOperatorInfoMap).set(this.operatorID, {
          currentResult: this.currentResult,
          currentPageIndex: this.currentPageIndex,
          currentPageSize: this.currentPageSize,
          total: this.total,
          columnKeys: this.currentDisplayColumns ?? [],
          operatorID: this.operatorID
        })).entries())
      });
    }

    this.errorMessages = undefined;

    this.currentColumns = undefined;
    this.currentDisplayColumns = undefined;
    this.currentResult = [];

    this.resultPanelOperatorID = undefined;
    this.chartType = undefined;
    this.breakpointTriggerInfo = undefined;
    this.breakpointAction = false;

    this.isFrontPagination = true;
    this.currentPageIndex = 1;
    this.currentPageSize = 10;
    this.total = 0;
    this.isLoadingResult = false;
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
        {label: 'OK', onClick: () => {modalRef.destroy(); }, type: 'primary'}
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
    const {pageSize: newPageSize, pageIndex: newPageIndex} = params;
    this.currentPageSize = newPageSize;
    this.currentPageIndex = newPageIndex;

    if (this.isFrontPagination) {
      return;
    }

    this.isLoadingResult = true;
    this.workflowWebsocketService.send('ResultPaginationRequest', {pageSize: newPageSize, pageIndex: newPageIndex});
  }

  /**
   * Updates all the result table properties based on the execution result,
   *  displays a new data table with a new paginator on the result panel.
   *
   * @param resultData rows of the result (may not be all rows if displaying result for workflow completed event)
   * @param totalRowCount the total number of rows for the result
   * @param operatorID id of the operator providing data
   */
  private setupResultTable(resultData: ReadonlyArray<object>, totalRowCount: number, operatorID: string) {
    this.operatorID = operatorID;

    if (resultData.length < 1) {
      return;
    }

    // if there is no new result
    //   then restore the previous paginated result data from session storage
    let resultPaginationInfo = sessionGetObject<ResultPaginationInfo>(PAGINATION_INFO_STORAGE_KEY);
    let viewResultOperatorInfoMap;
    if (resultPaginationInfo && !resultPaginationInfo.newWorkflowExecuted &&
      (viewResultOperatorInfoMap = new Map(resultPaginationInfo.viewResultOperatorInfoMap)).has(this.operatorID)) {
      const viewResultOperatorInfo = viewResultOperatorInfoMap.get(this.operatorID)!;
      this.isFrontPagination = false;
      this.currentResult = viewResultOperatorInfo.currentResult;
      this.currentPageIndex = viewResultOperatorInfo.currentPageIndex;
      this.currentPageSize = viewResultOperatorInfo.currentPageSize;
      this.total = viewResultOperatorInfo.total;
      this.currentDisplayColumns = viewResultOperatorInfo.columnKeys;
      // generate columnDef from first row, column definition is in order
      this.currentColumns = ResultPanelComponent.generateColumns(
        this.currentDisplayColumns.map(v => ({columnKey: v, columnText: v}))
      );
      return;
    }
    // creates a shallow copy of the readonly response.result,
    //  this copy will be has type object[] because MatTableDataSource's input needs to be object[]

    // save a copy of current result
    this.currentResult = resultData.slice();

    // When there is a result data from the backend,
    //  1. Get all the column names except '_id', using the first instance of
    //      result data.
    //  2. Use those names to generate a list of display columns, which would
    //      be used for displaying on angular material table.
    //  3. Pass the result data as array to generate a new angular material
    //      data table.
    //  4. Set the newly created data table to our own paginator.

    let columns: { columnKey: any, columnText: string }[];

    const columnKeys = Object.keys(resultData[0]).filter(x => x !== '_id');
    this.currentDisplayColumns = columnKeys;
    columns = columnKeys.map(v => ({columnKey: v, columnText: v}));

    // generate columnDef from first row, column definition is in order
    this.currentColumns = ResultPanelComponent.generateColumns(columns);
    this.total = totalRowCount ?? resultData.length;

    // get the current page size, if the result length is less than `this.currentPageSize`,
    //  then the maximum number of items each page will be the length of the result, otherwise `this.currentPageSize`.
    this.currentPageSize = Math.min(this.total, this.currentPageSize);

    // save paginated result into session storage
    resultPaginationInfo = {
      newWorkflowExecuted: false,

      // just update the map if there's one; otherwise create a map with current view result operator as an entry
      viewResultOperatorInfoMap: resultPaginationInfo ? (new Map(resultPaginationInfo.viewResultOperatorInfoMap)).set(operatorID, {
        currentResult: this.currentResult,
        currentPageIndex: this.currentPageIndex,
        currentPageSize: this.currentPageSize,
        total: this.total,
        columnKeys: columnKeys,
        operatorID: this.operatorID
      }) : new Map([[operatorID, {
        currentResult: this.currentResult,
        currentPageIndex: this.currentPageIndex,
        currentPageSize: this.currentPageSize,
        total: this.total,
        columnKeys: columnKeys,
        operatorID: this.operatorID
      }]])
    };
    sessionSetObject(PAGINATION_INFO_STORAGE_KEY, {
      newWorkflowExecuted: resultPaginationInfo.newWorkflowExecuted,
      // We have to turn it into array first because JSON.stringfy(someMap) will produce an empty object
      viewResultOperatorInfoMap: Array.from(resultPaginationInfo.viewResultOperatorInfoMap.entries())
    });
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

