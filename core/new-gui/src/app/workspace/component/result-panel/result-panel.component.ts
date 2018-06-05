import { Component, OnInit, ChangeDetectorRef } from '@angular/core';
import { DataSource } from '@angular/cdk/table';

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
  public currentDataSource: ResultDataSource | undefined;
  public currentDisplayRows: string = '';

  /** Column definitions in order */
  // displayedColumns = this.columns.map(x => x.columnDef);
  constructor(private executeWorkflowService: ExecuteWorkflowService, private changeDetectorRef: ChangeDetectorRef,
    private modalService: NgbModal) {
    this.executeWorkflowService.getExecuteEndedStream().subscribe(
      executionResult => this.handleResultData(executionResult),
    );
  }

  open(content: any): void {
    this.modalService.open(content).result.then((result) => {
      console.log(result);
    }, (reason) => {
      console.log(reason);
    });
  }

  private changeResultTableProperty(response: ExecutionResult) {
    const resultData: object[] | undefined = response.result;
    if (resultData !== undefined){
      this.currentDisplayColumns = Object.keys(resultData[0]).filter(x => x !== '_id');
      this.currentColumns = this.generateColumns(this.currentDisplayColumns);
      this.currentDataSource = new ResultDataSource(resultData);
      console.log(this.currentDisplayColumns);
    }
  }

  private handleResultData(response: ExecutionResult): void {
    console.log('view result compoenent, ');

    console.log(response);
    if (response.code === 0) {
      console.log('show success data');
      this.showMessage = false;
      // generate columnDef from first row
      this.changeResultTableProperty(response);
    } else {
      console.log('show fail message');
      this.showMessage = true;
      this.message = JSON.stringify(response.message);
    }
  }

  private generateColumns(columnNames: string[]): TableColumn[] {
    const columns: TableColumn[] = [];
    columnNames.forEach(col => columns.push(new TableColumn(col, col, (row) => `${row[col]}`)));
    console.log(columns);
    return columns;
  }

  public getRowDetails(row: any, content: any): void {
    console.log('ROw clicked');
    console.log(row);
    this.currentDisplayRows = JSON.stringify(row, undefined, 2);
    this.open(content);
  }

  ngOnInit() {
  }

}

export class TableColumn {
  constructor(
    public columnDef: string,
    public header: string,
    public cell: (row: any) => any
  ) { }
}


export class ResultDataSource extends DataSource<object> {

  constructor(private resultData: object[]) {
    super();
  }

  connect(): Observable<object[]> {
    return Observable.of(this.resultData);
  }

  disconnect() {
  }

}
