import { Component, ViewChild, Input } from '@angular/core';
import { MatPaginator, MatTableDataSource } from '@angular/material';

import { ExecuteWorkflowService } from './../../service/execute-workflow/execute-workflow.service';

import { NgbModal, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ExecutionResult, SuccessExecutionResult } from './../../types/execute-workflow.interface';
import { TableColumn, IndexableObject } from './../../types/result-table.interface';

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

  public showMessage: boolean = false;
  public message: string = '';

  public currentColumns: TableColumn[] | undefined;
  public currentDisplayColumns: string[] | undefined;
  public currentDataSource: MatTableDataSource<object> | undefined;

  @ViewChild(MatPaginator) paginator: MatPaginator | null = null;

  constructor(private executeWorkflowService: ExecuteWorkflowService, private modalService: NgbModal) {
    // once an execution has ended, update the result panel to dispaly
    //  execution result or error
    this.executeWorkflowService.getExecuteEndedStream().subscribe(
      executionResult => this.handleResultData(executionResult),
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
    const modalRef = this.modalService.open(NgbModalComponent);
    // cast the instance type from `any` to NgbModalComponent
    const modalComponentInstance = modalRef.componentInstance as NgbModalComponent;
    // set the currentDisplayRowData of the modal to be the data of clicked row
    modalComponentInstance.currentDisplayRowData = rowData;
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
    this.currentDataSource = new MatTableDataSource<object> (resultData);

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
      getCell: (row: IndexableObject) => `${row[col]}`
    }));
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

  constructor(public activeModal: NgbActiveModal) {}

}

