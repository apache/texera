import { Injectable } from '@angular/core';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';

import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';
import './../../../common/rxjs-operators';
import { AppSettings } from './../../../common/app-setting';

import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import { WorkflowGraph, WorkflowGraphReadonly } from './../workflow-graph/model/workflow-graph';
import { LogicalLink, LogicalPlan, LogicalOperator,
  ExecutionResult, ErrorExecutionResult, SuccessExecutionResult } from './../../types/workflow-execute.interface';

import { MOCK_WORKFLOW_PLAN } from './mock-workflow-plan';
import { MOCK_EXECUTION_RESULT } from './mock-result-data';

export const EXECUTE_WORKFLOW_ENDPOINT = 'queryplan/execute';


@Injectable()
export class ExecuteWorkflowService {


  private executeStartedStream = new Subject<string>();
  private executeEndedStream = new Subject<ExecutionResult>();

  constructor(private workflowActionService: WorkflowActionService, private http: HttpClient) { }

  /**
   * Creates a Logical Plan based on the workflow graph objected
   *  passed and make a http post request to the API endpoint with
   *  the logical plan object. The backend will either respond
   *  an execution result or an error encountered in the backend.
   *  Both results will be handled accordingly.
   *
   * @param workflowPlan
   */
  public executeWorkflow(): void {
    const workflowPlan = this.workflowActionService.getTexeraGraph();

    const body = ExecuteWorkflowService.getLogicalPlanRequest(workflowPlan);
    const requestURL = `${AppSettings.getApiEndpoint()}/${EXECUTE_WORKFLOW_ENDPOINT}`;

    console.log(`making http post request to backend ${requestURL}`);
    console.log(body);

    this.executeStartedStream.next('execution started');
    this.http.post<SuccessExecutionResult>(
      requestURL,
      JSON.stringify(body),
      { headers: { 'Content-Type': 'application/json' } })
      .subscribe(
        response => this.handleExecuteResult(response),
        errorResponse => this.handleExecuteError(errorResponse)
      );
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
   * When correct, Contains an object with:
   * -  resultID: the result ID of this execution
   * -  Code: the result code of 0
   * -  result: the actual result data to be displayed
   *
   * When incorrect, Contains an object with:
   * -  Code: the result code is not 0
   * -  message: error message received from backend
   */
  public getExecuteEndedStream(): Observable<ExecutionResult> {
    return this.executeEndedStream.asObservable();
  }

  /**
   * Handler function for valid execution result from the
   * backend.
   * Send the execution result to the execution end
   * event stream.
   *
   * @param response
   */
  private handleExecuteResult(response: SuccessExecutionResult): void {
    console.log('handling success result ');
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
  private handleExecuteError(errorResponse: HttpErrorResponse): void {
    console.log('handling error result ');
    console.log(errorResponse);

    // error shown to the user in different error scenarios
    let displayedErrorMessage: ErrorExecutionResult;

    // client side error, such as no internet connect
    if (errorResponse.error instanceof ErrorEvent) {
      displayedErrorMessage = {
        code: 1,
        message: 'Could not reach Texera server'
      };
    } else {
      // the workflow graph is invalid
      if (errorResponse.status === 400) {
        displayedErrorMessage = <ErrorExecutionResult>(errorResponse.error);
      } else {
        displayedErrorMessage = {
          code: 1,
          message: `Texera server error: ${errorResponse.message}`
        };
      }
    }
    this.executeEndedStream.next(displayedErrorMessage);
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
  public static getLogicalPlanRequest(workflowGraph: WorkflowGraphReadonly): LogicalPlan {

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

    return { operators: logicalOperators, links: logicalLinks };
  }
}
