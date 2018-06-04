import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';

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
export class ExecuteWorkflowService {


  private executeStartedStream = new Subject<string>();
  private executeEndedStream = new Subject<object>();

  constructor(private workflowActionService: WorkflowActionService, private http: HttpClient) { }

  /**
   * Execute the workflow based on the existing graph
   */
  public executeWorkflow(): void {
    console.log('execute workflow plan');
    console.log(this.workflowActionService.getTexeraGraph());
    this.executeRealPlan();
  }

  /**
   * Get the observable for execution started event
   * Contains a string that says:
   *  - execution process has begun
   */
  public getExecuteStartedStream(): Observable<string> {
    return this.executeStartedStream.asObservable();
  }

  /**
   * Get the observable for execution ended event
   * Contains an object with:
   * -  resultID: the result ID of this execution
   * -  Code: the result code
   * -  result: the actual result data to be displayed
   */
  public getExecuteEndedStream(): Observable<object> {
    return this.executeEndedStream.asObservable();
  }

  /**
   * Stimulate an execution and passed the mock result
   *  data back to the execution end event stream
   */
  private showMockResultData(): void {
    this.executeStartedStream.next('started');
    this.executeEndedStream.next({code: 0, result: MOCK_RESULT_DATA});
  }

  /**
   * Execute a mock workflow plan for testing
   */
  private executeMockPlan(): void {
    this.executeWorkflowPlan(MOCK_WORKFLOW_PLAN);
  }

  /**
   * Execute the real workflow plan using the current
   *  workflow graph saved.
   */
  private executeRealPlan(): void {
    this.executeWorkflowPlan(this.workflowActionService.getTexeraGraph());
  }

  /**
   * Creates a Logical Plan based on the workflow graph objected
   *  passed and make a http post request to the API endpoint with
   *  the logical plan object. The backend will either respond
   *  an execution result or an error encountered in the backend.
   *  Both results will be handled accordingly.
   *
   * @param workflowPlan
   */
  private executeWorkflowPlan(workflowPlan: WorkflowGraphReadonly): void {
    const body = this.getLogicalPlanRequest(workflowPlan);
    console.log('making http post request to backend');
    console.log('body is:');
    console.log(body);
    console.log(`${AppSettings.getApiEndpoint()}/${EXECUTE_WORKFLOW_ENDPOINT}`);
    this.http.post(`${AppSettings.getApiEndpoint()}/${EXECUTE_WORKFLOW_ENDPOINT}`, JSON.stringify(body),
      {headers: {'Content-Type': 'application/json'}}).subscribe(
        response => this.handleExecuteResult(response),
        errorResponse => this.handleExecuteError(errorResponse)
    );
  }

  /**
   * Handler function for valid execution result from the
   * backend.
   * Send the execution result to the execution end
   * event stream.
   *
   * @param response
   */
  private handleExecuteResult(response: any): void {
    console.log('handling success result ');
    console.log('result value is:');
    console.log(response);
    this.executeEndedStream.next(response);
  }

  /**
   * Handler function for invalid execution.
   *
   * Send the error messages generated from the backend
   * to the execution end event stream.
   *
   * @param errorResponse
   */
  private handleExecuteError(errorResponse: any): void {
    console.log('handling error result ');
    console.log('error value is:');
    console.log(errorResponse);
    this.executeEndedStream.next(errorResponse.error);
  }

  /**
   * Transform a workflowGraph object to the HTTP request body according to the backend API.
   *
   *  All the operators in the workflowGraph will be transformed to LogicalOperator objects,
   *  where each operator might have some unique properties and must have an operatorID and
   *  an operator type.
   *
   *  All the links in the workflowGraph will be tranformed to LogicalLink objects,
   *  where each link will store its source id as its origin and target id as its
   *  destination.
   *
   * @param workflowGraph
   */
  public getLogicalPlanRequest(workflowGraph: WorkflowGraphReadonly): LogicalPlan {

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
