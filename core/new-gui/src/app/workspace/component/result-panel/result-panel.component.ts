import { Component, OnInit, ViewChild } from '@angular/core';
import { DataSource } from '@angular/cdk/table';
import { MatPaginator, MatTableDataSource } from '@angular/material';


import { ExecuteWorkflowService } from "./../../service/execute-workflow/execute-workflow.service";

import { Observable } from 'rxjs/Observable';
import { NgbModal , ModalDismissReasons } from '@ng-bootstrap/ng-bootstrap';
import { ExecutionResult } from "./../../types/workflow-execute.interface";

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
    // once the execution ended, update the result panel
    this.executeWorkflowService.getExecuteEndedStream().subscribe(
      executionResult => this.handleResultData(executionResult),
    );
  }

  /**
   * open the ng-bootstrap model to display row delay when clicked
   * @param content model content passed from the html
   */
  public open(content: any): void {
    this.modalService.open(content).result.then((result) => {
      console.log(result);
    }, (reason) => {
      console.log(reason);
    });
  }

  /**
   * Changes the result table to use the new workflow execution result.
   * Set all the necessary for angular table and display the data table
   * and paginator.
   */
  private changeResultTableProperty(response: ExecutionResult) {
    const resultData: object[] | undefined = response.result;
    if (resultData !== undefined){
      // generate columnDef from first row, column definition is in order
      this.currentDisplayColumns = Object.keys(resultData[0]).filter(x => x !== '_id');
      this.currentColumns = this.generateColumns(this.currentDisplayColumns);

      // create a new DataSource object based on the new result data
      this.currentDataSource = new MatTableDataSource<object>(resultData);

      // set the paginator to be the new DataSource's paginator
      this.currentDataSource.paginator = this.paginator;

      console.log(this.currentDisplayColumns);
    }
  }

  /**
   * Handler for the execution result.
   * Response code == 0:
   * - Execution had run correctly
   * - Don't show any error message
   * - Update data table's property to display new result
   * Response code != 0:
   * - Execution had encountered an error
   * - Update and show the error message on the panel
   *
   * @param response execution response from the backend
   */
  private handleResultData(response: ExecutionResult): void {
    console.log('view result compoenent, ');

    console.log(response);
    if (response.code === 0) {
      console.log('show success data');
      console.log(response.result);
      this.showMessage = false;
      this.changeResultTableProperty(response);
    } else {
      console.log('show fail message');
      this.showMessage = true;
      this.message = JSON.stringify(response.message);
    }
  }

  /**
   * generate all the column information for displaying
   * @param columnNames list of column names that will be displayed on the data table
   */
  private generateColumns(columnNames: string[]): TableColumn[] {
    console.log('generateColumns: ');

    const columns: TableColumn[] = [];
    // generate a TableColumn object for each column
    columnNames.forEach(col => columns.push(new TableColumn(col, col, (row) => `${row[col]}`)));
    console.log(columns);
    return columns;
  }

  /**
   * get the information from the selected row
   * and display it on the modal once it is opened
   *
   * @param row row data
   * @param content modal content
   */
  public getRowDetails(row: any, content: any): void {
    console.log('getRowDetails: ');
    console.log(row);
    this.currentDisplayRow = JSON.stringify(row, undefined, 2);
    this.open(content);
  }

  ngOnInit() {

  }

}

/**
 * Class for holding the information regarding to each column
 */
export class TableColumn {
  constructor(
    public columnDef: string,
    public header: string,
    public cell: (row: any) => any
  ) { }
}
