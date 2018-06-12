import { Component, OnInit, ViewChild } from '@angular/core';
import { DataSource } from '@angular/cdk/table';
import { MatPaginator, MatTableDataSource } from '@angular/material';


import { ExecuteWorkflowService } from './../../service/execute-workflow/execute-workflow.service';

import { Observable } from 'rxjs/Observable';
import { NgbModal , ModalDismissReasons } from '@ng-bootstrap/ng-bootstrap';
import { ExecutionResult, SuccessExecutionResult } from './../../types/workflow-execute.interface';

/**
 * ResultPanelCompoent is the bottom level area that
 *  displays the execution result of a workflow after
 *  the execution finishes.
 *
 * The Component will display the result in an excel
 *  table format, where each row represents a result
 *  from the workflow and each column represents the
 *  type of result the workflow returns.
 *
 * Clicking each row of the result table will create an
 *  pop-up window and display the detail of that row
 *  in a pretty json format.
 *
 * @author Henry Chen
 * @author Zuozhi Wang
 *
 */
@Component({
  selector: 'texera-result-panel',
  templateUrl: './result-panel.component.html',
  styleUrls: ['./result-panel.component.scss']
})
export class ResultPanelComponent implements OnInit {

  public showMessage: boolean = false;
  public message: string = '';

  public currentColumns: TableColumn[] | undefined;
  public currentDisplayColumns: string[] | undefined;
  public currentDataSource: MatTableDataSource<object> | undefined;
  public currentDisplayRow: string = '';

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
   * @param content
   */
  public open(content: any): void {
    this.modalService.open(content).result.then((result) => {
      console.log(result);
    }, (reason) => {
      console.log(reason);
    });
  }


  /**
   * Fetches the information from the selected row in the result panel,
   *  updates the content of the row to dispaly, and opens the
   *  result panel to display the details of the selected rows.
   *
   * @param row
   * @param content
   */
  public getRowDetails(row: any, content: any): void {
    // console.log('getRowDetails: ');
    // console.log(row);
    this.currentDisplayRow = JSON.stringify(row, undefined, 2);
    this.open(content);
  }

  ngOnInit() {

  }

  /**
   *
   * Update all the result table properties based on the newly acquired
   *  execution result and display a new data table with a new paginator
   *  on the result panel.
   *
   */
  private changeResultTableProperty(response: SuccessExecutionResult) {
    const resultData: ReadonlyArray<object> | undefined = response.result;

    if (resultData !== undefined) {
      // generate columnDef from first row, column definition is in order
      this.currentDisplayColumns = Object.keys(resultData[0]).filter(x => x !== '_id');
      this.currentColumns = this.generateColumns(this.currentDisplayColumns);


      // create a new DataSource object based on the new result data
      this.currentDataSource = new MatTableDataSource<object> (resultData as Array<object>);

      // set the paginator to be the new DataSource's paginator
      this.currentDataSource.paginator = this.paginator;

      // console.log(this.currentDisplayColumns);
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
    // console.log('view result compoenent, ');
    // console.log(response);
    if (response.code === 0) {
      // when the execution had run correctly
      this.showMessage = false;
      this.changeResultTableProperty(response);
    } else {
      // when the execution encountered an error
      this.showMessage = true;
      this.message = JSON.stringify(response.message);
    }
  }

  /**
   * Generates all the column information for the result data table
   *
   * @param columnNames
   */
  private generateColumns(columnNames: string[]): TableColumn[] {
    // console.log('generateColumns: ');

    const columns: TableColumn[] = [];
    // generate a TableColumn object for each column
    columnNames.forEach(col => columns.push(new TableColumn(col, col, (row) => `${row[col]}`)));
    // console.log(columns);
    return columns;
  }

}

/**
 * Class for holding the properties for each column
 */
export class TableColumn {
  constructor(
    public columnDef: string,
    public header: string,
    public cell: (row: any) => any
  ) { }
}
