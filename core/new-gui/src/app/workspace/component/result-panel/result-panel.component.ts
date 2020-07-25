import { Component, ViewChild, Input } from '@angular/core';
import { MatPaginator, PageEvent } from '@angular/material/paginator';
import { MatTableDataSource } from '@angular/material/table';
import { ExecuteWorkflowService } from './../../service/execute-workflow/execute-workflow.service';
import { Observable } from 'rxjs/Observable';

import { NgbModal, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ExecutionResult, SuccessExecutionResult } from './../../types/execute-workflow.interface';
import { TableColumn, IndexableObject } from './../../types/result-table.interface';
import { ResultPanelToggleService } from './../../service/result-panel-toggle/result-panel-toggle.service';
import deepMap from 'deep-map';
import { isEqual, repeat } from 'lodash';
import { ResultObject } from '../../types/execute-workflow.interface';
import { WorkflowActionService } from '../../service/workflow-graph/model/workflow-action.service';
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
<<<<<<< Updated upstream

=======
interface Result {
  table: ReadonlyArray<object>;
  chartType: string | undefined;
}
>>>>>>> Stashed changes
@Component({
  selector: 'texera-result-panel',
  templateUrl: './result-panel.component.html',
  styleUrls: ['./result-panel.component.scss']
})
export class ResultPanelComponent {

  private static readonly PRETTY_JSON_TEXT_LIMIT: number = 50000;
  private static readonly TABLE_COLUMN_TEXT_LIMIT: number = 1000;
<<<<<<< Updated upstream

  //  show table when chartType is undefined, instead show visualization button
=======
  public result: ReadonlyArray<ResultObject> | undefined;
  public resultMap: Map<string, Result> = new Map<string, Result>();
  public selectedOperatorID: string = '';
>>>>>>> Stashed changes
  public chartType: string | undefined;
  // record which operator is selected
  public showMessage: boolean = false;
  public message: string = '';
  public currentColumns: TableColumn[] | undefined;
  public currentDisplayColumns: string[] | undefined;
  public currentDataSource: MatTableDataSource<object> | undefined;
  public showResultPanel: boolean | undefined;
  public currentResult: object[] = [];

  @ViewChild(MatPaginator) paginator: MatPaginator | null = null;

  private currentMaxPageSize: number = 0;
  private currentPageSize: number = 0;
  private currentPageIndex: number = 0;

  constructor(private executeWorkflowService: ExecuteWorkflowService, private modalService: NgbModal,
    private resultPanelToggleService: ResultPanelToggleService,
    private workflowActionService: WorkflowActionService) {
<<<<<<< Updated upstream
=======

>>>>>>> Stashed changes
    this.workflowActionService.getJointGraphWrapper().getJointCellHighlightStream()
      .subscribe(() => this.handleHighLightOperator());
    this.workflowActionService.getJointGraphWrapper().getJointCellUnhighlightStream()
      .subscribe(() => this.handleHighLightOperator());

    // once an execution has ended, update the result panel to dispaly
    //  execution result or error
    this.executeWorkflowService.getExecuteEndedStream().subscribe(
      executionResult => this.handleResultData(executionResult),
    );

    this.resultPanelToggleService.getToggleChangeStream().subscribe(
      value => this.showResultPanel = value,
    );
  }
<<<<<<< Updated upstream
=======
  handleHighLightOperator(): void {
    const highlightedOperators = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();

    if (highlightedOperators.length === 1) {
      this.selectedOperatorID = highlightedOperators[0];
      if (this.resultMap.has(this.selectedOperatorID)) {

        const result: Result | undefined = this.resultMap.get(this.selectedOperatorID);
        this.displayResultTable(result!.table);
        if (typeof result?.chartType === 'undefined') {
          this.showTable = true;

        } else {
          this.chartType = result?.chartType;
          this.showTable = false;
        }
      }
    }
    console.log(this.showTable);
  }
>>>>>>> Stashed changes
  /**
   * Opens the ng-bootstrap model to display the row details in
   *  pretty json format when clicked. User can view the details
   *  in a larger, expanded format.
   *
   * @param rowData the object containing the data of the current row in columnDef and cellData pairs
   */
  public open(rowData: object): void {

    // the row index will include the previous pages, therefore we need to minus the current page index
    //  multiply by the page size previously.
    const selectedRowIndex = this.currentResult.findIndex(eachRow => isEqual(eachRow, rowData));

    const rowPageIndex = selectedRowIndex - this.currentPageIndex * this.currentMaxPageSize;

    // generate a new row data that shortens the column text to limit rendering time for pretty json
    const rowDataCopy = ResultPanelComponent.trimDisplayJsonData(rowData as IndexableObject);

    // open the modal component
    const modalRef = this.modalService.open(NgbModalComponent, { size: 'lg' });

    // subscribe the modal close event for modal navigations (go to previous or next row detail)
    Observable.from(modalRef.result)
      .subscribe((modalResult: number) => {
        if (modalResult === 1) {
          // navigate to previous detail modal
          this.open(this.currentResult[selectedRowIndex - 1]);
        } else if (modalResult === 2) {
          // navigate to next detail modal
          this.open(this.currentResult[selectedRowIndex + 1]);
        }
      });

    // cast the instance type from `any` to NgbModalComponent
    const modalComponentInstance = modalRef.componentInstance as NgbModalComponent;

    // set the currentDisplayRowData of the modal to be the data of clicked row
    modalComponentInstance.currentDisplayRowData = rowDataCopy;

    // set the index value and page size to the modal for navigation
    modalComponentInstance.currentDisplayRowIndex = rowPageIndex;
    modalComponentInstance.currentPageSize = this.currentPageSize;
  }

<<<<<<< Updated upstream
  /**
   * Handler for high lighted operator.
   * When use click on an operator, check whether it is a sink operator and display result.
   */
  public handleHighLightOperator(): void {
    const highlightedOperators = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();

    if (highlightedOperators.length === 1) {
      this.showResultPanel = true;
      const resultMap = this.executeWorkflowService.getResultMap();
      const result: ResultObject | undefined = resultMap.get(highlightedOperators[0]);
      if (result) {
        this.displayResultTable(result.table);
        if (result.chartType) {
          this.chartType = result?.chartType;
        } else {
          this.chartType = undefined;
        }
      }
    } else {
      this.showResultPanel = false;
    }
=======
  public getResult(): Object[] {
    return this.currentResult;
  }

  public getColumn(): TableColumn[] | undefined {
    return this.currentColumns;
>>>>>>> Stashed changes
  }

  /**
   * This function will listen to the page change event in the paginator
   *  to update current page index and current page size for
   *  modal navigations
   *
   * @param event paginator event
   */
  public onPaginateChange(event: PageEvent): void {
    this.currentPageIndex = event.pageIndex;
    const currentPageOffset = event.pageIndex * event.pageSize;
    const remainingItemCounts = event.length - currentPageOffset;
    if (remainingItemCounts < 10) {
      this.currentPageSize = remainingItemCounts;
    } else {
      this.currentPageSize = event.length;
    }
  }

  /**
   * Handler for the execution result.
   *
   * Response code == 0:
   *  - Execution had run correctly
   *  - Don't show any error message
   *  - Update data table's property to display new result
   * Response code == 1:
   *  - Execution had encountered an error
   *  - Update and show the error message on the panel
   *
   * @param response
   */
  private handleResultData(response: ExecutionResult): void {

    // show resultPanel
    this.resultPanelToggleService.openResultPanel();

    // backend returns error, display error message
    if (response.code === 1) {
      this.displayErrorMessage(response.message);
      return;
    }

    // execution success, but result is empty, also display message
    if (response.result.length === 0) {
      this.displayErrorMessage(`execution doesn't have any results`);
      return;
    }
<<<<<<< Updated upstream

    const highlightedOperators = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();

    // If the selected operator is a sink operator, the result panel should display result when response is coming.
    if (highlightedOperators.length === 1) {

        const resultMap = this.executeWorkflowService.getResultMap();
        const result: ResultObject | undefined = resultMap.get(highlightedOperators[0]);
          if (result) {
            this.displayResultTable(result.table);
            if (result.chartType) {
              this.chartType = result.chartType;
            } else {
              this.chartType = undefined;
            }
          }

=======
    this.result = response.result;

    for (const item of this.result) {
      const result: Result = {
        table: item.table,
        chartType: item.chartType
      };
      this.resultMap.set(item.operator, result);
>>>>>>> Stashed changes
    }

  }

  /**
   * Displays the error message instead of the result table,
   *  sets all the local properties correctly.
   * @param errorMessage
   */
  private displayErrorMessage(errorMessage: string): void {
    // clear data source and columns
    this.currentDataSource = undefined;
    this.currentColumns = undefined;
    this.currentDisplayColumns = undefined;

    // display message
    this.showMessage = true;
    this.message = errorMessage;
  }

  /**
   * Updates all the result table properties based on the execution result,
   *  displays a new data table with a new paginator on the result panel.
   *
   * @param response
   */
  private displayResultTable(resultData: ReadonlyArray<object>) {
     if (resultData.length < 1) {
       throw new Error(`display result table inconsistency: result data should not be empty`);
     }
     // don't display message, display result table instead
     this.showMessage = false;

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


    // generate columnDef from first row, column definition is in order
    this.currentDisplayColumns = Object.keys(resultData[0]).filter(x => x !== '_id');
    this.currentColumns = ResultPanelComponent.generateColumns(this.currentDisplayColumns);

    // create a new DataSource object based on the new result data
    this.currentDataSource = new MatTableDataSource<object>(this.currentResult);

    // move paginator back to page one whenever new results come in. This prevents the error when
    //  previously paginator is at page 10 while the new result only have 2 pages.
    if (this.paginator !== null) {
      this.paginator.firstPage();
    }

    // set the paginator to be the new DataSource's paginator
    this.currentDataSource.paginator = this.paginator;

    // get the current page size, if the result length is less than 10, then the maximum number of items
    //   each page will be the length of the result, otherwise 10.
    this.currentMaxPageSize = this.currentPageSize = resultData.length < 10 ? resultData.length : 10;
  }

  /**
   * Generates all the column information for the result data table
   *
   * @param columnNames
   */
  private static generateColumns(columnNames: string[]): TableColumn[] {
    return columnNames.map(col => ({
      columnDef: col,
      header: col,
      getCell: (row: IndexableObject) => {
        if (row[col] !== null && row[col] !== undefined) {
          return this.trimTableCell(row[col].toString());
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
  selector: 'texera-ngbd-modal-content',
  templateUrl: './result-panel-modal.component.html',
  styleUrls: ['./result-panel.component.scss']
})
export class NgbModalComponent {
  // when modal is opened, currentDisplayRow will be passed as
  //  componentInstance to this NgbModalComponent to display
  //  as data table.
  @Input() currentDisplayRowData: object = {};

  // when modal is opened, currentDisplayRowIndex will be passed as
  //  component instance to this NgbModalComponent to either
  //  enable to disable row navigation buttons that allow users
  //  to navigate between different rows of data.
  @Input() currentDisplayRowIndex: number = 0;

  // the maximum page index that the navigation can go in the current page
  @Input() currentPageSize: number = 0;

  // activeModal is responsible for interacting with the
  //  ng-bootstrap modal, such as dismissing or exitting
  //  the pop-up modal.
  // it is used in the HTML template

  constructor(public activeModal: NgbActiveModal) { }

}

