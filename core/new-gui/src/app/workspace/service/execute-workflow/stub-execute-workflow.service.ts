import { Injectable } from '@angular/core';

import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';
import './../../../common/rxjs-operators';
import { AppSettings } from './../../../common/app-setting';

import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import { WorkflowGraph, WorkflowGraphReadonly } from './../workflow-graph/model/workflow-graph';
import { LogicalLink, LogicalPlan, LogicalOperator } from './../../types/workflow-execute.interface';

import { MOCK_RESULT_DATA } from './mock-result-data';
import { MOCK_WORKFLOW_PLAN } from './mock-workflow-plan';

export const EXECUTE_WORKFLOW_ENDPOINT = 'queryplan/execute';


@Injectable()
export class StubExecuteWorkflowService {


  private executeStartedStream = new Subject<string>();
  private executeEndedStream = new Subject<object>();

  constructor(private workflowActionService: WorkflowActionService) { }

  public executeWorkflow(): void {
    // console.log('execute workflow plan');
    // console.log(this.workflowActionService.getTexeraGraph());
    this.executeRealPlan();
  }

  public getExecuteStartedStream(): Observable<string> {
    return this.executeStartedStream.asObservable();
  }

  public getExecuteEndedStream(): Observable<object> {
    return this.executeEndedStream.asObservable();
  }

  private showMockResultData(): void {
    this.executeStartedStream.next('started');
    this.executeEndedStream.next({code: 0, result: MOCK_RESULT_DATA});
  }

  private executeMockPlan(): void {
    this.executeWorkflowPlan(MOCK_WORKFLOW_PLAN);
  }

  private executeRealPlan(): void {
    this.executeWorkflowPlan(this.workflowActionService.getTexeraGraph());
  }

  private executeWorkflowPlan(workflowPlan: WorkflowGraphReadonly): void {
    const body = this.getLogicalPlanRequest(workflowPlan);
    // console.log('making http post request to backend');
    // console.log('body is:');
    // console.log(body);
    // console.log(`${AppSettings.getApiEndpoint()}/${EXECUTE_WORKFLOW_ENDPOINT}`);
    Observable.of(MOCK_RESULT_DATA).subscribe(
        response => this.handleExecuteResult(response),
        errorResponse => this.handleExecuteError(errorResponse)
    );
  }

  private handleExecuteResult(response: any): void {
    // console.log('handling success result ');
    // console.log('result value is:');
    // console.log(response);
    this.executeEndedStream.next(response);
  }

  private handleExecuteError(errorResponse: any): void {
    // console.log('handling error result ');
    // console.log('error value is:');
    // console.log(errorResponse);
    this.executeEndedStream.next(errorResponse.error);
  }

  /**
   * Transform a workflowGraph object to the HTTP request body according to the backend API.
   *
   * @param logicalPlan
   */
  private getLogicalPlanRequest(workflowGraph: WorkflowGraphReadonly): LogicalPlan {

    // const logicalPlanJson = { operators: [] as any, links: [] as any};

    const logicalOperators = [] as LogicalOperator[];
    const logicalLinks = [] as LogicalLink[];

    // each operator only needs the operatorID, operatorType, and the properties
    // inputPorts and outputPorts are not needed (for now)
    workflowGraph.getOperators().forEach(
      op => logicalOperators.push(
        {
          ...op.operatorProperties,
          operatorID: op.operatorID,
          operatorType: op.operatorType
        }
      )
    );

    // filter out the non-connected links (because the workflowGraph model allows links that only connected to one operator)
    //  and generates json object with key 'origin' and 'destination'
    workflowGraph.getLinks()
    .filter(link => (link.source && link.target))
    .forEach(
      link => logicalLinks.push(
        {
          origin: link.source.operatorID,
          destination: link.target.operatorID
        }
      )
    );

    return { operators : logicalOperators , links: logicalLinks };

  }
}
