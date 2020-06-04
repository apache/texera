import { Component, ComponentRef, ViewChild, Input, ViewContainerRef, Optional, Inject } from '@angular/core';

import { ExecuteWorkflowService } from './../../service/execute-workflow/execute-workflow.service';
import { Observable } from 'rxjs/Observable';

import { ZorroModalComponent } from '../../../abstract-component/zorro-modal.component';
import { ComponentInserterService } from '../../../service/component-inserter.service';

import { ExecutionResult, SuccessExecutionResult } from './../../types/execute-workflow.interface';
import { TableColumn, IndexableObject } from './../../types/result-table.interface';
import { ResultPanelToggleService } from './../../service/result-panel-toggle/result-panel-toggle.service';
import deepMap from 'deep-map';
import { isEqual } from 'lodash';

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

  public showMessage: boolean = false;
  public message: string = '';
  public currentDisplayColumns: string[] | undefined;
  public currentModalRef: ComponentRef<NgbModalComponent>|undefined = undefined;
  public showResultPanel: boolean | undefined;
  public currentPage = 1;
  public currentPageSize = 10;

  private currentResult: object[] = [];

  constructor(private executeWorkflowService: ExecuteWorkflowService,
    private resultPanelToggleService: ResultPanelToggleService,
    private componentInserter: ComponentInserterService,
    private viewContainerRef: ViewContainerRef) {

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
  public open(index: number): void {
    index += (this.currentPage-1) * this.currentPageSize;
    console.log(index);
    // generate a new row data that shortens the column text to limit rendering time for pretty json
    const rowDataCopy = ResultPanelComponent.trimDisplayJsonData(this.currentResult[index] as IndexableObject);

    const prev = () => {
      if (this.currentModalRef) {
        index += -1;
        const newRowData = this.currentResult[index];
        this.currentModalRef.instance.currentDisplayRowData = ResultPanelComponent.trimDisplayJsonData(newRowData as IndexableObject);
        this.currentModalRef.instance.currentDisplayRowIndex = index;
      }
    };
    prev.bind(this);

    const next = () => {
      if (this.currentModalRef) {
        index += +1;
        const newRowData = this.currentResult[index];
        this.currentModalRef.instance.currentDisplayRowData = ResultPanelComponent.trimDisplayJsonData(newRowData as IndexableObject);
        this.currentModalRef.instance.currentDisplayRowIndex = index;
      }
    };
    next.bind(this);

    // open the modal component
    const modalRef = this.componentInserter.injectComponent<NgbModalComponent>(
      NgbModalComponent,
      this.viewContainerRef,
      [
        {provide: 'prev', useValue: prev},
        {provide: 'next', useValue: next}
      ]
      );

    this.currentModalRef = modalRef;

    // set the currentDisplayRowData of the modal to be the data of clicked row
    modalRef.instance.currentDisplayRowData = rowDataCopy;

    // set the index value and page size to the modal for navigation
    modalRef.instance.currentDisplayRowIndex = index;
    modalRef.instance.maxRowIndex = this.currentResult.length-1;
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

    // save a copy of current result
    this.currentResult = resultData;

    // When there is a result data from the backend,
    //  1. Get all the column names except '_id', using the first instance of
    //      result data.
    //  2. Use those names to generate a list of display columns, which would
    //      be used for displaying on angular mateiral table.


    // generate columnDef from first row, column definition is in order
    this.currentDisplayColumns = Object.keys(resultData[0]).filter(x => x !== '_id');
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
      getCell: (row: IndexableObject) => this.trimTableCell(row[col].toString())
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
export class NgbModalComponent extends ZorroModalComponent {
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
  @Input() maxRowIndex: number = 0;

  // activeModal is responsible for interacting with the
  //  ng-bootstrap modal, such as dismissing or exitting
  //  the pop-up modal.
  // it is used in the HTML template

  // will be overriden by result-panel

  constructor(
    @Optional() @Inject('ComponentRefPromise') ref: Promise<ComponentRef<NgbModalComponent>>,
    @Inject('prev') public prev: () => void,
    @Inject('next') public next: () => void
  ) { super(ref); }

}

