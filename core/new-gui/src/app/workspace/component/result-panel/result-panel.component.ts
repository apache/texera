import { Component, Input } from '@angular/core';
import { ExecuteWorkflowService } from './../../service/execute-workflow/execute-workflow.service';
import { Observable } from 'rxjs/Observable';

import { NzModalRef, NzModalService } from 'ng-zorro-antd/modal';
import { ExecutionResult, SuccessExecutionResult, ExecutionState, ExecutionStateInfo } from './../../types/execute-workflow.interface';
import { TableColumn, IndexableObject } from './../../types/result-table.interface';
import { ResultPanelToggleService } from './../../service/result-panel-toggle/result-panel-toggle.service';
import deepMap from 'deep-map';
import { isEqual, repeat, range } from 'lodash';
import { ResultObject } from '../../types/execute-workflow.interface';
import { WorkflowActionService } from '../../service/workflow-graph/model/workflow-action.service';
import { BreakpointTriggerInfo } from '../../types/workflow-common.interface';
import { OperatorMetadata } from '../../types/operator-schema.interface';
import { OperatorMetadataService } from '../../service/operator-metadata/operator-metadata.service';
import { DynamicSchemaService } from '../../service/dynamic-schema/dynamic-schema.service';
import { environment } from 'src/environments/environment';
import { assertType } from 'src/app/common/util/assert';

/**
 * ResultPanelCompoent is the bottom level area that displays the
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

  constructor(
    private executeWorkflowService: ExecuteWorkflowService,
    private modalService: NzModalService,
    private resultPanelToggleService: ResultPanelToggleService,
    private workflowActionService: WorkflowActionService
  ) {
    const activeStates: ExecutionState[] = [ExecutionState.Completed, ExecutionState.Failed, ExecutionState.BreakpointTriggered];
    Observable.merge(
      this.executeWorkflowService.getExecutionStateStream(),
      this.workflowActionService.getJointGraphWrapper().getJointCellHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointCellUnhighlightStream(),
      this.resultPanelToggleService.getToggleChangeStream()
    ).subscribe(trigger => this.displayResultPanel());

    this.executeWorkflowService.getExecutionStateStream().subscribe(event => {
      console.log(event.current.state);
      console.log(event.current);
      if (event.current.state === ExecutionState.BreakpointTriggered) {
        const breakpointOperator = this.executeWorkflowService.getBreakpointTriggerInfo()?.operatorID;
        if (breakpointOperator) {
          this.workflowActionService.getJointGraphWrapper().highlightOperator(breakpointOperator);
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
          this.workflowActionService.getJointGraphWrapper().highlightOperator(sinkOperators[0].operatorID);
        }
        this.resultPanelToggleService.openResultPanel();
      }
    });
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

    if (executionState.state === ExecutionState.Failed) {
      this.errorMessages = this.executeWorkflowService.getErrorMessages();
    } else if (executionState.state === ExecutionState.BreakpointTriggered) {
      const breakpointTriggerInfo = this.executeWorkflowService.getBreakpointTriggerInfo();
      if (highlightedOperators.length === 1 && highlightedOperators[0] === breakpointTriggerInfo?.operatorID) {
        this.breakpointTriggerInfo = breakpointTriggerInfo;
        this.breakpointAction = true;
        this.setupResultTable(breakpointTriggerInfo.report.map(r => r.faultedTuple.tuple).filter(t => t !== undefined));
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
      if (highlightedOperators.length === 1) {
        const result = executionState.resultMap.get(highlightedOperators[0]);
        if (result) {
          this.chartType = result.chartType;
          this.setupResultTable(result.table);
        }
      }
    } else if (executionState.state === ExecutionState.Paused) {
      if (highlightedOperators.length === 1) {
        const result = executionState.currentTuples[(highlightedOperators[0])]?.tuples;
        if (result) {
          const resultTable: string[][] = [];
          result.forEach(workerTuple => {
            const updatedTuple: string[] = [];
            updatedTuple.push(workerTuple.workerID);
            updatedTuple.push(...workerTuple.tuple);
            resultTable.push(updatedTuple);
          });
          this.setupResultTable(resultTable);
        }
      }
    }
  }

  public clearResultPanel(): void {
    this.errorMessages = undefined;

    this.currentColumns = undefined;
    this.currentDisplayColumns = undefined;
    this.currentResult = [];

    this.chartType = undefined;
    this.breakpointTriggerInfo = undefined;
    this.breakpointAction = false;

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
        currentDisplayRowIndex: selectedRowIndex,
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
        {label: 'OK', onClick: () => {modalRef.destroy(); }, type: 'primary'},
      ],
    });
  }

  public onClickSkipTuples(): void {
    this.executeWorkflowService.skipTuples();
    this.breakpointAction = false;
  }

  /**
   * Updates all the result table properties based on the execution result,
   *  displays a new data table with a new paginator on the result panel.
   *
   * @param response
   */
  private setupResultTable(resultData: ReadonlyArray<object>) {

    if (resultData.length < 1) {
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
    //      be used for displaying on angular mateiral table.
    //  3. Pass the result data as array to generate a new angular material
    //      data table.
    //  4. Set the newly created data table to our own paginator.

    let columns: {columnKey: any, columnText: string}[];

    const columnKeys = Object.keys(resultData[0]).filter(x => x !== '_id');
    this.currentDisplayColumns = columnKeys;
    columns = columnKeys.map(v => ({columnKey: v, columnText: v}));

    // generate columnDef from first row, column definition is in order
    this.currentColumns = ResultPanelComponent.generateColumns(columns);
  }

  /**
   * Generates all the column information for the result data table
   *
   * @param columnNames
   */
  private static generateColumns(columns: {columnKey: any, columnText: string}[]): TableColumn[] {
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

