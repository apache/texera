import { Component, ViewChild, Input } from '@angular/core';
import { MatPaginator, MatTableDataSource } from '@angular/material';

import { ExecuteWorkflowService } from './../../service/execute-workflow/execute-workflow.service';

import { NgbModal, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ExecutionResult, SuccessExecutionResult } from './../../types/execute-workflow.interface';
import { TableColumn, IndexableObject } from './../../types/result-table.interface';
import { ResultPanelToggleService } from './../../service/result-panel-toggle/result-panel-toggle.service';
import { clone, cloneDeep } from 'lodash-es';

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

  private static readonly PRETTY_JSON_TEXT_LIMIT: number = 2000;
  private static readonly TABLE_COLUMN_TEXT_LIMIT: number = 30;

  public showMessage: boolean = false;
  public message: string = '';
  public currentColumns: TableColumn[] | undefined;
  public currentDisplayColumns: string[] | undefined;
  public currentDataSource: MatTableDataSource<object> | undefined;
  public showResultPanel: boolean | undefined;

  @ViewChild(MatPaginator) paginator: MatPaginator | null = null;

  constructor(private executeWorkflowService: ExecuteWorkflowService, private modalService: NgbModal,
    private resultPanelToggleService: ResultPanelToggleService) {


    // once an execution has ended, update the result panel to dispaly
    //  execution result or error
    this.executeWorkflowService.getExecuteEndedStream().subscribe(
      executionResult => this.handleResultData(executionResult),
    );

    this.resultPanelToggleService.getToggleChangeStream().subscribe(
      value => this.showResultPanel = value,
    );
  }

  /**
   * Opens the ng-bootstrap model to display the row details in
   *  pretty json format when clicked. User can view the details
   *  in a larger, expanded format.
   *
   * @param rowData the object containing the data of the current row in columnDef and cellData pairs
   */
  public open(rowData: object): void {
    // generate a new row data that shortens the column text to limit rendering time for pretty json
    const rowDataCopy = ResultPanelComponent.mutateColumnData(rowData as IndexableObject);

    // open the modal component
    const modalRef = this.modalService.open(NgbModalComponent);

    // cast the instance type from `any` to NgbModalComponent
    const modalComponentInstance = modalRef.componentInstance as NgbModalComponent;

    // set the currentDisplayRowData of the modal to be the data of clicked row
    modalComponentInstance.currentDisplayRowData = rowDataCopy;
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

    // execution success, display result table
    this.displayResultTable(response);
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
  private displayResultTable(response: SuccessExecutionResult): void {
    if (response.result.length < 1) {
      throw new Error(`display result table inconsistency: result data should not be empty`);
    }

    // don't display message, display result table instead
    this.showMessage = false;

    // creates a shallow copy of the readonly response.result,
    //  this copy will be has type object[] because MatTableDataSource's input needs to be object[]
    const resultData = response.result.slice();

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
    this.currentDataSource = new MatTableDataSource<object>(resultData);

    // set the paginator to be the new DataSource's paginator
    this.currentDataSource.paginator = this.paginator;
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
      getCell: (row: IndexableObject) => `${row[col]}`.substring(0, ResultPanelComponent.TABLE_COLUMN_TEXT_LIMIT)
    }));
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
  private static mutateColumnData(rowData: IndexableObject): object {
    let rowDataCopy = cloneDeep(rowData);
    Object.keys(rowDataCopy).forEach(column => {
      const currentColumnData = rowDataCopy[column];
      if (typeof currentColumnData === 'string') {
        const columnString: string = currentColumnData;
        console.log(columnString);
        const trimmedColumnData: string = columnString.length > ResultPanelComponent.PRETTY_JSON_TEXT_LIMIT
          ? columnString.substring(0, ResultPanelComponent.PRETTY_JSON_TEXT_LIMIT) + '...' : columnString;
        rowDataCopy = { ...rowDataCopy, [column]: trimmedColumnData };
      } else if (Array.isArray(currentColumnData)) {
        const columnArray: Array<object> = currentColumnData;
        columnArray.forEach(nestedColumn =>
          rowDataCopy = { ...rowDataCopy, [column]: this.mutateColumnData(nestedColumn as IndexableObject) });
      } else if (typeof currentColumnData === 'object') {
        rowDataCopy = { ...rowDataCopy, [column]: this.mutateColumnData(currentColumnData as IndexableObject)};
      }
    });

    return rowDataCopy;
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

  // activeModal is responsible for interacting with the
  //  ng-bootstrap modal, such as dismissing or exitting
  //  the pop-up modal.
  // it is used in the HTML template

  constructor(public activeModal: NgbActiveModal) { }

}

